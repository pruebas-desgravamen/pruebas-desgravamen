package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/s3"
)

type FuncionArgumento struct {
	Atributo  string
	Funcion   []string
	Argumento [][]string
}

type AtributoFuncionArgumento struct {
	Atributo          string           `json:"atributo"`
	FuncionArgumentos FuncionArgumento `json:"funcion"`
}
type RegistroAtributoValor struct {
	Transaccion string
	Registro    int
	Atributo    string `json:"atributo"`
	Valor       string `json:"valor"`
}

type RegistroAtributoValorFuncionArgumento struct {
	Transaccion string     `json:"transaccion"`
	Registro    int        `json:"registro"`
	Atributo    string     `json:"atributo"`
	Valor       string     `json:"valor"`
	Funcion     []string   `json:"funcion"`
	Argumentos  [][]string `json:"argumentos"`
}

type QueryConfiguradorResponse struct {
	Atributo  string   `json:"atributo"`
	Funcion   []string `json:"funcion"`
	Argumento []string `json:"argumento"`
}

type IdConfiguradorNPoliza struct {
	IdConfigurador int
}

type Cliente struct {
	PK     string `json:"pk"`
	Sk     string `json:"sk"`
	TIdDoc string `json:"tIdDoc"`
	NIdDoc string `json:"nIdDoc"`
	Name   string `json:"name"`
}

type Credito struct {
	PK       string `json:"pk"`
	Sk       string `json:"sk"`
	Currency string `json:"currency"`
}

type Certificado struct {
	PK    string `json:"pk"`
	Sk    string `json:"sk"`
	Prime string `json:"Prime"`
}

type Iobject struct {
	Key       string `json:"key"`
	Etag      string `json:"etag"`
	Sequencer string `json:"sequencer"`
}

type Evento struct {
	Object Iobject `json:"object"`
}

type Output struct {
	Errors []string
	Array  [][]RegistroAtributoValorFuncionArgumento
}

func handler(ctx context.Context, ev Evento) (Output, error) {

	var BUCKET_NAME = os.Getenv("BUCKET_NAME")
	var OBJECT_NAME = ev.Object.Key
	var TABLE_NAME_CONFIGURADOR = os.Getenv("TABLA_NAME_CONFIGURADOR")
	var output = Output{}

	//Iniciar sesion en aws
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(os.Getenv("us-east-1"))},
	)
	if err != nil {
		fmt.Println(err.Error())
		return Output{}, err
	}

	svcDynamo := dynamodb.New(sess) // Dynamodb
	svcS3 := s3.New(sess)           // s3

	// Retrieve file from S3 using EventBridge
	file, _ := svcS3.GetObject(
		&s3.GetObjectInput{
			Bucket: aws.String(BUCKET_NAME),
			Key:    aws.String(OBJECT_NAME),
		})
	if err != nil {
		fmt.Println(err.Error())
		return Output{}, err
		// return "could not retrieve document", err
	}

	// Open XSLX file
	result, _ := OpenFile(*file)

	// Get data from File Name
	transaccion := OBJECT_NAME[0:2]
	fmt.Println(transaccion)
	if transaccion == "VE" {
		transaccion = "VENTA"
	}

	nPolicy := OBJECT_NAME[2:17]
	nPolicyInt, _ := strconv.Atoi(nPolicy)
	nPolicy = strconv.Itoa(nPolicyInt)
	fmt.Println(nPolicy)

	// Query to know id estructura

	queryInput := dynamodb.QueryInput{
		TableName:              aws.String(TABLE_NAME_CONFIGURADOR),
		IndexName:              aws.String("pk-nPoliza"),
		KeyConditionExpression: aws.String("pk= :pk and nPoliza= :nPoliza"),
		ExpressionAttributeNames: map[string]*string{
			"#producto": aws.String("producto"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk":      {S: aws.String("POLIZA")},
			":nPoliza": {S: aws.String(nPolicy)},
		},
		ProjectionExpression: aws.String(" #producto"),
	}
	queryIdConfigurador, _ := svcDynamo.Query(&queryInput)

	idConfiguradorNPoliza := []IdConfiguradorNPoliza{}
	err = dynamodbattribute.UnmarshalListOfMaps(queryIdConfigurador.Items, &idConfiguradorNPoliza)
	if err != nil {
		fmt.Println("Unmarshall Error")
		// return "error on unmarshall", err
		return Output{}, err
	}

	fmt.Println(idConfiguradorNPoliza)

	////////////////////////////////////////////////////////////////////////////////////////////////////
	// Query to know the functions that will be applied to their respective columns (ATRIBUTO - FUNCION)
	inputQueryConfigurador := dynamodb.QueryInput{
		TableName:              aws.String(TABLE_NAME_CONFIGURADOR),
		KeyConditionExpression: aws.String("pk=:pk"),
		ExpressionAttributeNames: map[string]*string{
			"#atributo":  aws.String("atributo"),
			"#funcion":   aws.String("funcion"),
			"#argumento": aws.String("argumento"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {S: aws.String("1")},
		},
		ProjectionExpression: aws.String("#funcion, #atributo, #argumento"),
	}

	queryConfigurador, _ := svcDynamo.Query(&inputQueryConfigurador)
	if err != nil {
		fmt.Println(err.Error())
		return Output{}, err
		// return "could not get from configurador", err
	}

	queryResponse := []QueryConfiguradorResponse{}
	err = dynamodbattribute.UnmarshalListOfMaps(queryConfigurador.Items, &queryResponse)
	if err != nil {
		fmt.Println("Unmarshall Error")
		// return "error on unmarshall", err
		return Output{}, err
	}
	fmt.Println("queryResponse")
	fmt.Println(queryResponse)

	///////////////////////////////////////////////////////////////////////////////////

	// Save the name of the columns
	columnas := result.GetRows("Sheet1")[0]

	mapFirstRows := make(map[int]string)

	for i, firstRow := range columnas {
		mapFirstRows[i] = firstRow
	}
	fmt.Println(mapFirstRows)

	// validacion de cantidad de columnas
	if len(columnas) != len(queryResponse) {
		output.Errors = append(output.Errors, "Error en la cantidad de columnas de la trama con la del configurador")
		return output, nil
	}

	for columnaContador := 0; columnaContador < len(queryResponse); columnaContador++ {
		if columnas[columnaContador] == queryResponse[columnaContador].Atributo {
			fmt.Println(columnas[columnaContador])
			fmt.Println("okay: true")
		} else {
			output.Errors = append(output.Errors, columnas[columnaContador]+"no existe")
		}
	}

	/////////////////////////////////////////////////////

	////////////////////////////// SE PUEDE JUNTAR //////////////////////////////////////////////////////////
	// Guardar en un array cada atributo con su array de funciones
	atributoFuncionArray := []AtributoFuncionArgumento{}

	for atributoFuncionContador := range queryResponse {
		var argumentoMatrix [][]string
		var argumentoArray []string

		for argumento := range queryResponse[atributoFuncionContador].Argumento {
			argumentoArray = strings.Split(queryResponse[atributoFuncionContador].Argumento[argumento], ",")
			argumentoMatrix = append(argumentoMatrix, argumentoArray)
		}

		atributoFuncionElement := AtributoFuncionArgumento{
			Atributo: queryResponse[atributoFuncionContador].Atributo,
			FuncionArgumentos: FuncionArgumento{
				Atributo:  queryResponse[atributoFuncionContador].Atributo,
				Funcion:   queryResponse[atributoFuncionContador].Funcion,
				Argumento: argumentoMatrix,
			},
		}

		atributoFuncionArray = append(atributoFuncionArray, atributoFuncionElement)
	}

	fmt.Println("atributoFuncionArray")
	fmt.Println(atributoFuncionArray)

	// Hashmap de atributo con su respectivas funciones
	mapAtributoFuncionArgumento := make(map[string]FuncionArgumento)

	for atributoFuncionArrayContador := range atributoFuncionArray {
		mapAtributoFuncionArgumento[atributoFuncionArray[atributoFuncionArrayContador].Atributo] = atributoFuncionArray[atributoFuncionArrayContador].FuncionArgumentos
	}

	fmt.Println("mapAtributoFuncionArgumento")
	fmt.Println(mapAtributoFuncionArgumento)

	////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// Convierte el archivo (matriz) a un array que junta al atributo con su valor
	atributoValorList := []RegistroAtributoValor{}

	for rowIndex, rowValues := range result.GetRows("Sheet1")[1] {

		element := RegistroAtributoValor{
			Transaccion: transaccion,
			Atributo:    mapFirstRows[rowIndex],
			Registro:    0,
			Valor:       rowValues,
		}
		atributoValorList = append(atributoValorList, element)

	}

	fmt.Println("atributoValorList")
	fmt.Println(atributoValorList)

	// //////////////////////////////////////////////////////////////////////////////////////////////

	valorFuncionList := []RegistroAtributoValorFuncionArgumento{}

	for valorFuncionListContador := 0; valorFuncionListContador < len(atributoValorList); valorFuncionListContador++ {
		if val, ok := mapAtributoFuncionArgumento[atributoValorList[valorFuncionListContador].Atributo]; ok {
			element := RegistroAtributoValorFuncionArgumento{
				Transaccion: atributoValorList[valorFuncionListContador].Transaccion,
				Registro:    atributoValorList[valorFuncionListContador].Registro,
				Valor:       atributoValorList[valorFuncionListContador].Valor,
				Atributo:    val.Atributo,
				Funcion:     val.Funcion,
				Argumentos:  val.Argumento,
			}
			valorFuncionList = append(valorFuncionList, element)
		}
	}

	fmt.Println("valorFuncionList")
	fmt.Println(valorFuncionList)

	for i := 0; i < len(valorFuncionList); i += len(columnas) {
		output.Array = append(output.Array, valorFuncionList[i:i+len(columnas)])
	}

	fmt.Println("output")
	fmt.Println(output)

	return output, nil

	// return "change", nil
}

func main() {
	lambda.Start(handler)
}

func MarshalMap(in interface{}) (map[string]*dynamodb.AttributeValue, error) {
	av, err := getEncoder().Encode(in)
	if err != nil || av == nil || av.M == nil {
		return map[string]*dynamodb.AttributeValue{}, err
	}

	return av.M, nil
}

func getEncoder() *dynamodbattribute.Encoder {
	encoder := dynamodbattribute.NewEncoder()
	encoder.NullEmptyString = false
	return encoder
}

func OpenFile(filename s3.GetObjectOutput) (*excelize.File, error) {

	buff := new(bytes.Buffer)
	buff.ReadFrom(filename.Body)

	f, _ := excelize.OpenReader(buff)

	return f, nil
}

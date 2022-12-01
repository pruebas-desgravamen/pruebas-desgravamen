package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

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
	Sk string `json:"sk"`
}

type Iobject struct {
	Key       string `json:"key"`
	Etag      string `json:"etag"`
	Sequencer string `json:"sequencer"`
}

type Evento struct {
	Object    Iobject `json:"object"`
	Structure string  `json:"structure"`
	Filename  string  `json:"filename"`
}

type Registry struct {
	Array [][]RegistroAtributoValorFuncionArgumento
}

type Output struct {
	Errors         []string
	ErrorsDataType []FuncError
}

func handler(ctx context.Context, ev Evento) (Output, error) {

	var BUCKET_NAME = os.Getenv("BUCKET_NAME")
	var OBJECT_NAME = ev.Filename
	var TABLE_NAME_CONFIGURADOR = os.Getenv("TABLA_NAME_CONFIGURADOR")
	var output = Output{}
	var registry = Registry{}

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
			":pk": {S: aws.String(ev.Structure)},
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
	fmt.Println("first possible error")
	fmt.Println(output)

	for columnaContador := 0; columnaContador < len(queryResponse); columnaContador++ {
		if columnas[columnaContador] == queryResponse[columnaContador].Atributo {
		} else {
			output.Errors = append(output.Errors, columnas[columnaContador]+" no existe")
		}
	}
	if len(output.Errors) > 0 {
		return output, nil
	}

	fmt.Println("second possible erro")
	fmt.Println(output)

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
		registry.Array = append(registry.Array, valorFuncionList[i:i+len(columnas)])
	}

	fmt.Println("output")
	fmt.Println(output)

	validationErrors := Validations(registry.Array[0])

	if len(validationErrors) > 0 {
		fmt.Println("ERRORESSSSS")
		fmt.Println(validationErrors)
		output.ErrorsDataType = append(output.ErrorsDataType, validationErrors...)
		return output, nil
	}

	return output, nil

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

func ValidarCaracter(valor string, campo string) (bool, error) {
	return true, nil
}

func ValidarNumero(valor string, campo string) (bool, error) {
	_, err := strconv.ParseFloat(valor, 64)
	if err != nil {
		return false, fmt.Errorf("el valor %s del campo %s no es un numero", valor, campo)
	}
	return true, nil
}

func ValidarFormatoFecha(fecha string, formato string, campo string) (bool, error) {
	_, err := time.Parse(formato, fecha)
	if err != nil {
		return false, fmt.Errorf("fecha %s del campo %s no cumple con el formato %s", fecha, campo, formato)
	}
	return true, nil
}

type ValidationReturn interface{}
type FuncError struct {
	Transaccion string
	Registro    int    `json:"registro"`
	Atributo    string `json:"atributo"`
	Funcion     string
	Error       string
}

type FuncValidation struct {
	Registro int    `json:"registro"`
	Atributo string `json:"atributo"`
	Funcion  string
	Valid    bool `json:"valid"`
}

func Validations(eventArray []RegistroAtributoValorFuncionArgumento) []FuncError {

	var errores []FuncError

	for _, e := range eventArray {

		for i := 0; i < len(e.Funcion); i++ {

			var err error

			switch e.Funcion[i] {
			case "ValidarCaracter":
				_, err = ValidarCaracter(e.Valor, e.Atributo)
			case "ValidarNumero":
				_, err = ValidarNumero(e.Valor, e.Atributo)
			case "ValidarFormatoFecha":
				_, err = ValidarFormatoFecha(e.Valor, e.Argumentos[i][0], e.Atributo)
			}

			if err != nil {
				errores = append(errores, FuncError{Transaccion: e.Transaccion, Registro: e.Registro, Atributo: e.Atributo, Funcion: e.Funcion[i], Error: err.Error()})
			}
		}

	}

	return errores

}

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"

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
	Argumento []string
}

type AtributoFuncionArgumento struct {
	Atributo          string           `json:"atributo"`
	FuncionArgumentos FuncionArgumento `json:"funcion"`
}
type RegistroAtributoValor struct {
	Registro int
	Atributo string `json:"atributo"`
	Valor    string `json:"valor"`
}

type RegistroAtributoValorFuncionArgumento struct {
	Registro   int      `json:"registro"`
	Atributo   string   `json:"atributo"`
	Valor      string   `json:"valor"`
	Funcion    []string `json:"funcion"`
	Argumentos []string `json:"argumentos"`
}

type QueryConfiguradorResponse struct {
	Atributo  string   `json:"atributo"`
	Funcion   []string `json:"funcion"`
	Argumento []string `json:"argumento"`
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

func handler(ctx context.Context, ev Evento) ([]RegistroAtributoValorFuncionArgumento, error) {

	// var TABLE_NAME = os.Getenv("TABLA_NAME")
	var BUCKET_NAME = os.Getenv("BUCKET_NAME")
	var OBJECT_NAME = ev.Object.Key
	var TABLE_NAME_CONFIGURADOR = os.Getenv("TABLA_NAME_CONFIGURADOR")

	//Iniciar sesion en aws
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String(os.Getenv("us-east-1"))},
	)
	// if err != nil {
	// 	fmt.Println(err.Error())
	// 	return _, err
	// }

	svcDynamo := dynamodb.New(sess) // Dynamodb
	svcS3 := s3.New(sess)           // s3

	// Retrieve file from S3 using EventBridge
	file, _ := svcS3.GetObject(
		&s3.GetObjectInput{
			Bucket: aws.String(BUCKET_NAME),
			Key:    aws.String(OBJECT_NAME),
		})
	// if err != nil {
	// 	fmt.Println(err.Error())
	// 	return "could not retrieve document", err
	// }

	// Open XSLX file
	result, _ := OpenFile(*file)

	// Save the name of the columns
	columnas := result.GetRows("Sheet1")[0]

	mapFirstRows := make(map[int]string)

	for i, firstRow := range columnas {
		mapFirstRows[i] = firstRow
	}
	fmt.Println(mapFirstRows)

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
	// if err != nil {
	// 	fmt.Println(err.Error())
	// 	return "could not get from configurador", err
	// }

	queryResponse := []QueryConfiguradorResponse{}
	_ = dynamodbattribute.UnmarshalListOfMaps(queryConfigurador.Items, &queryResponse)
	// if err != nil {
	// 	fmt.Println("Unmarshall Error")
	// 	return "error on unmarshall", err
	// }

	fmt.Println(queryResponse)

	// validacion de cantidad de columnas
	// if len(columnas) == len(queryResponse) {
	// 	return "Error en la cantidad de columnas de la trama con la del configurador", nil
	// }

	for columnaContador := 0; columnaContador < len(queryResponse); columnaContador++ {
		if columnas[columnaContador] == queryResponse[columnaContador].Atributo {
			fmt.Println(columnas[columnaContador])
			fmt.Println("ok")
		} else {
			fmt.Println("error")
		}
	}

	// for _, queryResponseElement := range queryResponse {
	// 	if contains(columnas, queryResponseElement.Atributo) {
	// 		fmt.Println("works")
	// 		fmt.Println(queryResponseElement.Atributo)
	// 	} else {
	// 		fmt.Println("error")
	// 	}

	// 	// if queryResponseElement.Atributo in columnas{

	// 	// }
	// 	// if val, ok := mapAtributoFuncion[atributoValorList[valorFuncionListContador].Atributo]; ok {
	// 	// 		element := ValorFuncion{
	// 	// 			Valor:   atributoValorList[valorFuncionListContador].Valor,
	// 	// 			Funcion: val,
	// 	// 		}
	// 	// 		valorFuncionList = append(valorFuncionList, element)
	// 	// 	}
	// }

	////////////////////////////// SE PUEDE JUNTAR //////////////////////////////////////////////////////////
	// Guardar en un array cada atributo con su array de funciones
	atributoFuncionArray := []AtributoFuncionArgumento{}

	for atributoFuncionContador := range queryResponse {
		// 	var stringAsArray []string
		atributoFuncionElement := AtributoFuncionArgumento{
			Atributo: queryResponse[atributoFuncionContador].Atributo,
			FuncionArgumentos: FuncionArgumento{
				Atributo:  queryResponse[atributoFuncionContador].Atributo,
				Funcion:   queryResponse[atributoFuncionContador].Funcion,
				Argumento: queryResponse[atributoFuncionContador].Argumento,
			},
			// Funcion:           queryResponse[atributoFuncionContador].Funcion,
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

	for _, rowValues := range result.GetRows("Sheet1")[1:] {
		for columnIndex, columnValue := range rowValues {
			element := RegistroAtributoValor{
				Atributo: mapFirstRows[columnIndex],
				Registro: columnIndex,
				Valor:    columnValue,
			}
			atributoValorList = append(atributoValorList, element)
		}
	}

	fmt.Println("atributoValorList")
	fmt.Println(atributoValorList)

	// //////////////////////////////////////////////////////////////////////////////////////////////

	valorFuncionList := []RegistroAtributoValorFuncionArgumento{}

	for valorFuncionListContador := 0; valorFuncionListContador < len(atributoValorList); valorFuncionListContador++ {
		// 	// 	// element := ValorFuncion{
		// 	// 	// 	Valor:   atributoValorList[valorFuncionListContador].Valor,
		// 	// 	// 	Funcion: mapAtributoFuncion[atributoValorList[valorFuncionListContador].Atributo],
		// 	// 	// }
		// 	// 	// valorFuncionList = append(valorFuncionList, element)
		if val, ok := mapAtributoFuncionArgumento[atributoValorList[valorFuncionListContador].Atributo]; ok {
			element := RegistroAtributoValorFuncionArgumento{
				Valor:      atributoValorList[valorFuncionListContador].Valor,
				Atributo:   val.Atributo,
				Funcion:    val.Funcion,
				Argumentos: val.Argumento,
			}
			valorFuncionList = append(valorFuncionList, element)
		}
	}

	fmt.Println("valorFuncionList")
	fmt.Println(valorFuncionList)

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// cell1 := result.GetCellValue("Sheet1", "A2")
	// TIdDoc := result.GetCellValue("Sheet1", "B2")
	// NIdDoc := result.GetCellValue("Sheet1", "C2")
	// Name := result.GetCellValue("Sheet1", "D2")
	// Currency := result.GetCellValue("Sheet1", "E2")
	// Prime := result.GetCellValue("Sheet1", "F2")

	// var matrix = [][]string{{cell1, TIdDoc, NIdDoc, Name, Currency, Prime}}

	// matrix[0] = append(matrix[0], cell1)
	// matrix[0] = append(matrix[0], TIdDoc)
	// matrix[0] = append(matrix[0], NIdDoc)
	// matrix[0] = append(matrix[0], Name)
	// matrix[0] = append(matrix[0], Currency)
	// matrix[0] = append(matrix[0], Prime)

	// println(matrix[0][1])

	// var i, j int

	// for i = 0; i < 1; i++ {
	// 	for j = 0; j < 6; j++ {
	// 		fmt.Print(matrix[i][j], "\t")
	// 	}
	// 	fmt.Println(" ")
	// }

	// cliente := Cliente{
	// 	PK:     "Cliente",
	// 	Sk:     NIdDoc,
	// 	TIdDoc: TIdDoc,
	// 	NIdDoc: NIdDoc,
	// 	Name:   Name,
	// }

	// credito := Credito{
	// 	PK:       "Credito",
	// 	Sk:       cell1,
	// 	Currency: Currency,
	// }

	// certificado := Certificado{
	// 	PK:    "Certificado",
	// 	Sk:    cell1,
	// 	Prime: Prime,
	// }

	// data1, _ := MarshalMap(cliente)
	// data2, _ := MarshalMap(credito)
	// data3, _ := MarshalMap(certificado)

	// if err != nil {
	// 	fmt.Println(err.Error())
	// 	return "marshal error", err
	// }
	// params1 := &dynamodb.PutItemInput{
	// 	Item:      data1,
	// 	TableName: aws.String(TABLE_NAME),
	// }
	// _, err2 := svcDynamo.PutItem(params1)

	// if err2 != nil {
	// 	fmt.Println(err2.Error())
	// 	return "dynamo error", err2
	// }

	// params2 := &dynamodb.PutItemInput{
	// 	Item:      data2,
	// 	TableName: aws.String(TABLE_NAME),
	// }
	// _, err3 := svcDynamo.PutItem(params2)

	// if err3 != nil {
	// 	fmt.Println(err2.Error())
	// 	return "dynamo error", err2
	// }

	// params3 := &dynamodb.PutItemInput{
	// 	Item:      data3,
	// 	TableName: aws.String(TABLE_NAME),
	// }
	// _, err4 := svcDynamo.PutItem(params3)

	// if err4 != nil {
	// 	fmt.Println(err2.Error())
	// 	return "dynamo error", err2
	// }

	// rows := result.GetRows("Sheet1")
	// fmt.Println(len(rows))

	// for _, row := range rows {
	// 	for _, colCell := range row {
	// 		fmt.Print(colCell, "\t")
	// 	}
	// }

	// for _, rowValues := range result.GetRows("Sheet1") {
	// 	for columnIndex, columnValue := range rowValues {
	// 		mapFirstRows[columnIndex] == mapAtributoFuncion[]
	// 	}
	// }
	return valorFuncionList, nil
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

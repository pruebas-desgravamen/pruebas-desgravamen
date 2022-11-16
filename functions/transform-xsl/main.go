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

func Handler(ctx context.Context, ev Evento) (string, error) {

	// var TABLE_NAME = os.Getenv("TABLA_NAME")
	var BUCKET_NAME = os.Getenv("BUCKET_NAME")
	var OBJECT_NAME = ev.Object.Key
	// var cliente Cliente
	// var credito Credito
	// var certificado Certificado

	//Iniciar sesion en aws
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(os.Getenv("us-east-1"))},
	)
	if err != nil {
		fmt.Println(err.Error())
		return "", err
	}

	// svcDynamo := dynamodb.New(sess) // Dynamodb
	svcS3 := s3.New(sess) // s3

	file, err := svcS3.GetObject(
		&s3.GetObjectInput{
			Bucket: aws.String(BUCKET_NAME),
			Key:    aws.String(OBJECT_NAME),
		})
	if err != nil {
		fmt.Println(err.Error())
		return "could not retrieve document", err
	}

	result, _ := OpenFile(*file)

	// cell1 := result.GetCellValue("Sheet1", "A2")
	// cell2 := result.GetCellValue("Sheet1", "B2")
	// cell3 := result.GetCellValue("Sheet1", "C2")
	// cell4 := result.GetCellValue("Sheet1", "D2")
	// cell5 := result.GetCellValue("Sheet1", "E2")
	// cell6 := result.GetCellValue("Sheet1", "F2")

	// cliente = Cliente{
	// 	PK:     "Cliente",
	// 	Sk:     cell3,
	// 	TIdDoc: cell2,
	// 	NIdDoc: cell3,
	// 	Name:   cell4,
	// }

	// credito = Credito{
	// 	PK:       "Credito",
	// 	Sk:       cell1,
	// 	Currency: cell5,
	// }

	// certificado = Certificado{
	// 	PK:    "Certificado",
	// 	Sk:    cell1,
	// 	Prime: cell6,
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

	rows := result.GetRows("Sheet1")
	fmt.Println(len(rows))

	for _, row := range rows {
		for _, colCell := range row {
			fmt.Print(colCell, "\t")
		}
	}
	return "success", nil
}

func main() {
	lambda.Start(Handler)
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

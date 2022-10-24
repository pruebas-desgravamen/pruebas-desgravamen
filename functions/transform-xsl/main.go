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

type Sdn struct {
	Id       string `json:"id"`
	Number   string `json:"number"`
	Footnote string `json:"footnote"`
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

	var TABLE_NAME = os.Getenv("TABLA_NAME")
	var BUCKET_NAME = os.Getenv("BUCKET_NAME")
	var OBJECT_NAME = ev.Object.Key
	var sdn Sdn

	//Iniciar sesion en aws
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(os.Getenv("us-east-1"))},
	)
	if err != nil {
		fmt.Println(err.Error())
		return "", err
	}

	svcDynamo := dynamodb.New(sess) // Dynamodb
	svcS3 := s3.New(sess)           // s3

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

	cell1 := result.GetCellValue("Sheet1", "A1")
	cell2 := result.GetCellValue("Sheet1", "B1")

	sdn = Sdn{
		Id:       "Trama",
		Number:   cell1,
		Footnote: cell2,
	}
	data, err := MarshalMap(sdn)
	if err != nil {
		fmt.Println(err.Error())
		return "marshal error", err
	}
	params := &dynamodb.PutItemInput{
		Item:      data,
		TableName: aws.String(TABLE_NAME),
	}
	_, err2 := svcDynamo.PutItem(params)
	if err2 != nil {
		fmt.Println(err2.Error())
		return "dynamo error", err2
	}

	// rows := result.GetRows("Sheet1")

	// for _, row := range rows {
	// 	for _, colCell := range row {
	// 		fmt.Print(colCell, "\t")
	// 	}
	// }
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

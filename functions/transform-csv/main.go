package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"

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
		return "", err
	}

	buff := new(bytes.Buffer)
	buff.ReadFrom(file.Body)
	result := buff.String()

	reqBody := strings.Split(result, "\n")

	print(" ------------ ")

	fmt.Print(reqBody)
	fmt.Print("\n")

	for i := range reqBody {
		// reemplazar -0- por vacio, eliminar \n
		reqBody[i] = strings.Replace(reqBody[i], "-0- ", "", -1)
		reqBody[i] = strings.TrimRight(reqBody[i], "\r\n")

		fmt.Print(reqBody[i])
		print("\n")

		// singleData is a row
		singleData := strings.Split(reqBody[i], ",")
		if len(singleData) == 2 {
			// convertir a int
			if err != nil {
				fmt.Println(err.Error())
				return "", err
			}

			sdn = Sdn{
				Id:       "Trama",
				Number:   strings.Trim(singleData[0], "\""),
				Footnote: strings.Trim(singleData[1], "\""),
			}
			data, err := MarshalMap(sdn)
			if err != nil {
				fmt.Println(err.Error())
				return "", err
			}
			params := &dynamodb.PutItemInput{
				Item:      data,
				TableName: aws.String(TABLE_NAME),
			}
			_, err2 := svcDynamo.PutItem(params)
			if err2 != nil {
				fmt.Println(err2.Error())
				return "", err2
			}
		}
	}
	result = "Sucess"
	return result, nil
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

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

type QueryConfiguradorResponse struct {
	Atributo  string   `json:"atributo"`
	Funcion   []string `json:"funcion"`
	Argumento []string `json:"argumento"`
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

	var TABLE_NAME_CONFIGURADOR = os.Getenv("TABLA_NAME_CONFIGURADOR")
	var BUCKET_NAME = os.Getenv("BUCKET_NAME")
	var OBJECT_NAME = ev.Object.Key

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

	// Query to know the functions, atributes and arguments that will be applied to their respective columns
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

	queryResponse := []QueryConfiguradorResponse{}
	_ = dynamodbattribute.UnmarshalListOfMaps(queryConfigurador.Items, &queryResponse)

	// Get file from S3

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

	fmt.Println(" ------------ ")

	fmt.Println(reqBody)

	for i := range reqBody {
		// reemplazar -0- por vacio, eliminar \n
		// reqBody[i] = strings.Replace(reqBody[i], "-0- ", "", -1)
		// reqBody[i] = strings.TrimRight(reqBody[i], "\r\n")

		// prints a registry
		fmt.Println(reqBody[i])

		// prints columns per registry
		singleData := strings.Split(reqBody[i], ",")

		for j := range singleData {
			fmt.Println(singleData[j])
			fmt.Println("---")
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

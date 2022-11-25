package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"log"
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

type Iobject struct {
	Key       string `json:"key"`
	Etag      string `json:"etag"`
	Sequencer string `json:"sequencer"`
}

type Evento struct {
	Object Iobject `json:"object"`
}

func handler(ctx context.Context, ev Evento) (string, error) {

	// var TABLE_NAME = os.Getenv("TABLA_NAME")
	var BUCKET_NAME = os.Getenv("BUCKET_NAME")
	var OBJECT_NAME = ev.Object.Key

	//Iniciar sesion en aws
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(os.Getenv("us-east-1"))},
	)
	if err != nil {
		fmt.Println(err.Error())
		return "could not initiate session", err
	}

	svcS3 := s3.New(sess)

	// Retrieve file from S3 using EventBridge
	file, err := svcS3.GetObject(
		&s3.GetObjectInput{
			Bucket: aws.String(BUCKET_NAME),
			Key:    aws.String(OBJECT_NAME),
		})
	if err != nil {
		fmt.Println(err.Error())
		return "could not retrieve document", err
	}

	buff := new(bytes.Buffer)
	buff.ReadFrom(file.Body)
	result := buff.String()

	reqBody := strings.Split(result, "\n")

	registriesPerChunk := 2

	numberRegistries := len(reqBody) // 80 000 - 4

	numberFiles := numberRegistries / registriesPerChunk // 800 - 2

	// for i := 0; i < numberFiles; i++ {
	// 	f:= excelize.NewFile()
	// 	f.SetCellBool()
	// }

	// for i := range reqBody {

	// 	// reemplazar -0- por vacio, eliminar \n
	// 	// reqBody[i] = strings.Replace(reqBody[i], "-0- ", "", -1)
	// 	// reqBody[i] = strings.TrimRight(reqBody[i], "\r\n")

	// 	// prints a registry
	// 	fmt.Println(reqBody[i])

	// 	// prints columns per registry
	// 	singleData := strings.Split(reqBody[i], ",")

	// 	for j := range singleData {
	// 		fmt.Println(singleData[j])
	// 		fmt.Println("---")
	// 	}
	// }

	for nFile := 0; nFile < numberFiles; nFile++ {

		fileName := "chunk" + strconv.Itoa(nFile) + ".csv"
		csvFile, err1 := os.Create(fileName)
		if err1 != nil {
			log.Fatalf("failed creating file: %s", err)
		}

		indexRegistry := nFile * registriesPerChunk

		csvwriter := csv.NewWriter(csvFile)

		for i := range reqBody[indexRegistry : indexRegistry+registriesPerChunk] {
			fmt.Println(reqBody[i])
		}

		defer csvwriter.Flush()
		// for indexRegistryChunk := 0; indexRegistryChunk < registriesPerChunk; indexRegistryChunk++ {
		// 	reqBody[0:registriesPerChunk]
		// }
		defer csvFile.Close()

		/// upload csv to s3

		_, err := svcS3.PutObject(
			&s3.PutObjectInput{
				Bucket: aws.String(BUCKET_NAME),
				Key:    aws.String(fileName),
				Body:   csvFile,
			})
		if err != nil {
			println("erro on uploading")
		}

	}

	return strconv.Itoa(numberFiles), nil
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

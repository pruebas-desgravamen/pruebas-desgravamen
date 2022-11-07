package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
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

type Response struct {
	Asegurados int     `json:"asegurados"`
	Monto      float64 `json:"monto"`
}

func Handler(ctx context.Context, ev Evento) (Response, error) {

	var BUCKET_NAME = os.Getenv("BUCKET_NAME")
	var OBJECT_NAME = ev.Object.Key

	//Iniciar sesion en aws
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String(os.Getenv("us-east-1"))},
	)
	// if err != nil {
	// 	fmt.Println(err.Error())
	// 	return "", err
	// }

	svcS3 := s3.New(sess) // s3

	file, _ := svcS3.GetObject(
		&s3.GetObjectInput{
			Bucket: aws.String(BUCKET_NAME),
			Key:    aws.String(OBJECT_NAME),
		})

	// if err != nil {
	// 	fmt.Println(err.Error())
	// 	return "could not retrieve document", err
	// }

	result, _ := OpenFile(*file)

	// cell1 := result.GetCellValue("Sheet1", "A1")
	// cell2 := result.GetCellValue("Sheet1", "B1")

	rows := result.GetRows("Sheet1")

	var monto []float64

	for i := 1; i <= len((rows))-1; i++ {
		montoInt, _ := strconv.ParseFloat(rows[i][19], 64)
		monto = append(monto, montoInt)
	}

	montoTotal := 0.00
	for _, numb := range monto {
		montoTotal += numb
	}

	response := Response{
		Asegurados: len((rows)) - 1,
		Monto:      montoTotal,
	}

	fmt.Println(response)
	return response, nil
}

func main() {
	lambda.Start(Handler)
}

func OpenFile(filename s3.GetObjectOutput) (*excelize.File, error) {

	buff := new(bytes.Buffer)
	buff.ReadFrom(filename.Body)

	f, _ := excelize.OpenReader(buff)

	return f, nil
}

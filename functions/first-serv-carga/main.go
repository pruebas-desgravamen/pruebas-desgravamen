package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type Input struct {
	Input CargaEvent `json:"input"`
}

type CargaEvent struct {
	Tipo               string `json:"tipo"`
	Poliza             string `json:"poliza"`
	Contratante        string `json:"contratante"`
	Producto           string `json:"producto"`
	CanalDeVenta       string `json:"canalDeVenta"`
	Transaccion        string `json:"transaccion"`
	PeriodoDeclaracion string `json:"periodoDeclaracion"`
	RUC                string `json:"ruc"`
}

type Carga struct {
	Pk                 string `json:"pk"`
	Sk                 string `json:"sk"`
	Tipo               string `json:"tipo"`
	Poliza             string `json:"poliza"`
	Contratante        string `json:"contratante"`
	Producto           string `json:"producto"`
	CanalDeVenta       string `json:"canalDeVenta"`
	Transaccion        string `json:"transaccion"`
	PeriodoDeclaracion string `json:"periodoDeclaracion"`
	RUC                string `json:"ruc"`
	Fecha              string `json:"fecha"`
	Hora               string `json:"hora"`
}

func handler(ctx context.Context, event Input) (string, error) {

	TABLE_NAME := os.Getenv("DB")

	sess, err := session.NewSession(&aws.Config{})
	if err != nil {
		return "", err
	}

	svc := dynamodb.New(sess)

	carga := &Carga{
		Pk:                 "TRAMA",
		Sk:                 event.Input.Poliza,
		Tipo:               event.Input.Tipo,
		Poliza:             event.Input.Poliza,
		Contratante:        event.Input.Contratante,
		Producto:           event.Input.Producto,
		CanalDeVenta:       event.Input.CanalDeVenta,
		Transaccion:        event.Input.Transaccion,
		PeriodoDeclaracion: event.Input.PeriodoDeclaracion,
		RUC:                event.Input.RUC,
		Fecha:              getFecha(),
		Hora:               getHora(),
	}

	item, err := MarshalMap(carga)
	if err != nil {
		fmt.Println("error on marshal")
		return "Error on marshal", err
	}

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(TABLE_NAME),
	}

	_, err = svc.PutItem(input)
	if err != nil {
		fmt.Println("error on putitem")
		return "error on putitem", err
	}

	fmt.Println(carga)
	return "Succes", nil
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

func getFecha() string {
	currentTime := time.Now()

	return currentTime.Format("02/01/2006")
}

func getHora() string {
	currentTime := time.Now()

	return currentTime.Format("3:4:5 pm")
}

func main() {
	lambda.Start(handler)
}

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
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Atributo struct {
	Atributo string `json:"atributo"`
}
type Iobject struct {
	Key       string `json:"key"`
	Etag      string `json:"etag"`
	Sequencer string `json:"sequencer"`
}

type Evento struct {
	Object    Iobject `json:"object"`
	Pk        string  `json:"pk"`
	Filename  string  `json:"filename"`
	Structure string  `json:"structure"`
}

type Response struct {
	Asegurados int     `json:"asegurados"`
	Monto      float64 `json:"monto"`
	Moneda     string  `json:"moneda"`
}

func Handler(ctx context.Context, ev Evento) (Response, error) {

	var BUCKET_NAME = os.Getenv("BUCKET_NAME")
	var OBJECT_NAME = ev.Object.Key
	var TABLE_NAME_CONFIGURADOR = os.Getenv("TABLA_NAME_CONFIGURADOR")

	//Iniciar sesion en aws
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String(os.Getenv("us-east-1"))},
	)
	// if err != nil {
	// 	fmt.Println(err.Error())
	// 	return "", err
	// }
	svcDynamo := dynamodb.New(sess) // Dynamodb

	inputQueryConfigurador := dynamodb.QueryInput{
		TableName:              aws.String(TABLE_NAME_CONFIGURADOR),
		KeyConditionExpression: aws.String("pk=:pk"),
		ExpressionAttributeNames: map[string]*string{
			"#atributo": aws.String("atributo"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {S: aws.String(ev.Structure)},
		},
		ProjectionExpression: aws.String("#atributo"),
	}

	queryConfigurador, _ := svcDynamo.Query(&inputQueryConfigurador)

	queryResponse := []Atributo{}
	_ = dynamodbattribute.UnmarshalListOfMaps(queryConfigurador.Items, &queryResponse)

	fmt.Println("queryResponse")
	fmt.Println(queryResponse)

	svcS3 := s3.New(sess) // s3

	file, _ := svcS3.GetObject(
		&s3.GetObjectInput{
			Bucket: aws.String(BUCKET_NAME),
			Key:    aws.String(OBJECT_NAME),
		})

	result, _ := OpenFile(*file)

	rows := result.GetRows("Sheet1")

	mapFirstRows := make(map[string]int)

	for i, firstRow := range queryResponse {
		mapFirstRows[firstRow.Atributo] = i
	}
	fmt.Println(mapFirstRows)

	var monto []float64

	for i := 1; i <= len((rows))-1; i++ {

		montoInt, _ := strconv.ParseFloat(rows[i][mapFirstRows["NPRIME"]], 64) /////////////////////// TRAER DEL CONFIGURADOR
		monto = append(monto, montoInt)
	}

	montoTotal := 0.00
	for _, numb := range monto {
		montoTotal += numb
	}

	// verificar que la moneda sea la misma
	for i := 1; i <= len((rows)); i++ {
		if rows[i][mapFirstRows["MONEDA"]] != rows[i+1][mapFirstRows["MONEDA"]] {
			return Response{}, nil
		}
	}
	moneda := rows[1][mapFirstRows["MONEDA"]]

	response := Response{
		Asegurados: len((rows)) - 1,
		Monto:      montoTotal,
		Moneda:     moneda,
	}

	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(TABLE_NAME_CONFIGURADOR),
		Key: map[string]*dynamodb.AttributeValue{
			"pk": {
				S: aws.String(ev.Pk),
			},
			"sort": {
				S: aws.String("PROCESS"),
			},
		},
		UpdateExpression: aws.String("set asegurados = :asegurados, premium = :premium, currency = :currency"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":asegurados": {
				N: aws.String(strconv.Itoa(len((rows)) - 1)),
			},
			":premium": {
				N: aws.String(fmt.Sprintf("%f", montoTotal)),
			},
			":currency": {
				S: aws.String(moneda),
			},
		},
		ReturnValues: aws.String("UPDATED_NEW"),
	}
	_, err := svcDynamo.UpdateItem(input)
	if err != nil {
		panic(fmt.Sprintf("failed to Dynamodb Update Items, %v", err))
	}

	fmt.Println(response)
	return response, nil
}

func main() {
	lambda.Start(Handler)
}

// HACER QUERY A ENTIDADES PARA SABER A QUE CAMPO ENTRARIA

func OpenFile(filename s3.GetObjectOutput) (*excelize.File, error) {

	buff := new(bytes.Buffer)
	buff.ReadFrom(filename.Body)

	f, _ := excelize.OpenReader(buff)

	return f, nil
}

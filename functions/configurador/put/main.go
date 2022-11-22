package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type DatosGenerales struct {
	Pk                   string `json:"pk"`
	Sk                   string `json:"sk"`
	NombreEstructura     string `json:"nombreEstructura"`
	Transaccion          string `json:"transaccion"`
	Ramo                 string `json:"ramo"`
	DatosCabecera        string `json:"datosCabecera"`
	CertificadoPorRol    string `json:"certificadoPorRol"`
	FilasOrdenadasPorRol string `json:"filasOrdenadasPorRol"`
	FormatoArchivo       string `json:"formatoArchivo"`
}

type Poliza struct {
	Pk          string `json:"pk"`
	Sk          string `json:"sk"`
	Producto    string `json:"producto"`
	NPoliza     string `json:"nPoliza"`
	Contratante string `json:"contratante"`
	CanalVenta  string `json:"canalVenta"`
	Vigencia    string `json:"vigencia"`
	Moneda      string `json:"moneda"`
	Reglas      []int  `json:"reglas"`
}

type Atributos struct {
	Pk             string   `json:"pk"`
	Sk             string   `json:"sk"`
	Id             string   `json:"id"`
	Atributo       string   `json:"atributo"`
	TipoDato       string   `json:"tipoDato"`
	Obligatorio    string   `json:"obligatorio"`
	ValorUnico     string   `json:"valorUnico"`
	Funcion        []string `json:"funcion"`
	Origen         []string `json:"origen"`
	Argumento      []string `json:"argumento"`
	Dominio        []string `json:"dominio"`
	ColumnaDestino string   `json:"columnaDestino"`
	EntidadDestino string   `json:"entidadDestino"`
}

type Notificaciones struct {
	Pk           string `json:"pk"`
	Sk           string `json:"sk"`
	Id           string `json:"id"`
	Evento       string `json:"evento"`
	Correos      string `json:"correos"`
	Aplicacion   string `json:"aplicacion"`
	Asunto       string `json:"asunto"`
	Destinatario string `json:"destinatario"`
	Plantilla    string `json:"plantilla"`
	Fase         string `json:"fase"`
}

type Event struct {
	DatosGenerales   DatosGenerales   `json:"datosGenerales"`
	ColeccionPolizas []Poliza         `json:"coleccionPolizas"`
	Lectura          []Atributos      `json:"lectura"`
	Notificaciones   []Notificaciones `json:"notificaciones"`
}

type Numerator struct {
	Ide int `json:"ide"`
}

func handler(ctx context.Context, e Event) (string, error) {
	TABLENAME := os.Getenv("TableName")
	REGION := os.Getenv("Region")

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(REGION)},
	)

	if err != nil {
		panic(fmt.Sprintf("failed to create session, %v", err))
	}

	svc := dynamodb.New(sess)

	item, _ := svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(TABLENAME),
		Key: map[string]*dynamodb.AttributeValue{
			"pk": {
				S: aws.String("NUMERATOR"),
			},
			"sk": {
				S: aws.String("NUMERATOR"),
			},
		},
	})

	cont := Numerator{}
	err = dynamodbattribute.UnmarshalMap(item.Item, &cont)

	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal Dynamodb Record, %v", err))
	}

	nextIde := cont.Ide + 1

	e.DatosGenerales.Pk = "ESTRUCTURA"
	e.DatosGenerales.Sk = strconv.Itoa(nextIde)

	putItem, err := MarshalMap(e.DatosGenerales)

	if err != nil {
		panic(fmt.Sprintf("failed to marshal map, %v", err))
	}

	_, err = svc.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(TABLENAME),
		Item:      putItem,
	})

	if err != nil {
		panic(fmt.Sprintf("failed 1 to put item, %v", err))
	}

	for i := 0; i < len(e.ColeccionPolizas); i++ {
		e.ColeccionPolizas[i].Pk = "POLIZA"
		e.ColeccionPolizas[i].Sk = strconv.Itoa(nextIde) + "#" + e.ColeccionPolizas[i].NPoliza
		putItem, err := MarshalMap(e.ColeccionPolizas[i])

		if err != nil {
			panic(fmt.Sprintf("failed to marshal map, %v", err))
		}

		_, err = svc.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String(TABLENAME),
			Item:      putItem,
		})
		if err != nil {
			panic(fmt.Sprintf("failed 2 to put item, %v", err))
		}
	}

	for i := 0; i < len(e.Lectura); i++ {
		e.Lectura[i].Pk = strconv.Itoa(nextIde)
		e.Lectura[i].Sk = strconv.Itoa(i + 1)

		putItem, err := MarshalMap(e.Lectura[i])

		if err != nil {
			panic(fmt.Sprintf("failed to marshal map, %v", err))
		}

		_, err = svc.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String(TABLENAME),
			Item:      putItem,
		})
		if err != nil {
			panic(fmt.Sprintf("failed 3 to put item, %v", err))
		}
	}

	for i := 0; i < len(e.Notificaciones); i++ {
		e.Notificaciones[i].Pk = "NOTIFICACION"
		e.Notificaciones[i].Sk = strconv.Itoa(nextIde) + "#" + e.Notificaciones[i].Fase + "#" + (e.Notificaciones[i].Id)

		putItem, err := MarshalMap(e.Notificaciones[i])

		if err != nil {
			panic(fmt.Sprintf("failed to marshal map, %v", err))
		}

		_, err = svc.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String(TABLENAME),
			Item:      putItem,
		})
		if err != nil {
			panic(fmt.Sprintf("failed 4 to put item, %v", err))
		}
	}

	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(TABLENAME),
		Key: map[string]*dynamodb.AttributeValue{
			"pk": {
				S: aws.String("NUMERATOR"),
			},
			"sk": {
				S: aws.String("NUMERATOR"),
			},
		},
		UpdateExpression: aws.String("set ide = :ide"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":ide": {
				N: aws.String(strconv.Itoa(nextIde)),
			},
		},
		ReturnValues: aws.String("UPDATED_NEW"),
	}

	_, err = svc.UpdateItem(input)

	if err != nil {
		panic(fmt.Sprintf("failed to update item, %v", err))
	}

	return "Funciono", nil
}

func main() {
	// Make the handler available for Remote Procedure Call by Cloud Function
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

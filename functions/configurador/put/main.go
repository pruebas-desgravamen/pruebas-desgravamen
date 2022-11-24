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
	Pk          string   `json:"pk"`
	Sk          string   `json:"sk"`
	Id          string   `json:"id"`
	Atributo    string   `json:"atributo"`
	TipoDato    string   `json:"tipoDato"`
	Obligatorio string   `json:"obligatorio"`
	ValorUnico  string   `json:"valorUnico"`
	Funcion     []string `json:"funcion"`
	Origen      []string `json:"origen"`
	Argumento   []string `json:"argumento"`
	Dominio     []string `json:"dominio"`
}

type Notificaciones struct {
	Pk         string `json:"pk"`
	Sk         string `json:"sk"`
	Id         string `json:"id"`
	Evento     string `json:"evento"`
	Aplicacion string `json:"aplicacion"`
	Asunto     string `json:"asunto"`
	Plantilla  string `json:"plantilla"`
	Fase       string `json:"fase"`
}

type Entidad struct {
	Pk       string `json:"pk"`
	Sk       string `json:"sk"`
	Atributo string `json:"atributo"`
	Origen   string `json:"origen"`
	Valor    string `json:"valor"`
}

type Registrar struct {
	Cliente     []Entidad `json:"cliente"`
	Certificado []Entidad `json:"certificado"`
	Rol         []Entidad `json:"rol"`
	Poliza      []Entidad `json:"poliza"`
	Credito     []Entidad `json:"credito"`
}

type Event struct {
	DatosGenerales   DatosGenerales   `json:"datosGenerales"`
	ColeccionPolizas []Poliza         `json:"coleccionPolizas"`
	Lectura          []Atributos      `json:"lectura"`
	Registrar        Registrar        `json:"registrar"`
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

	for i := 0; i < len(e.Registrar.Cliente); i++ {
		e.Registrar.Cliente[i].Pk = "CLIENTE"
		e.Registrar.Cliente[i].Sk = strconv.Itoa(nextIde) + "#" + e.Registrar.Cliente[i].Atributo

		putItem, err := MarshalMap(e.Registrar.Cliente[i])
		fmt.Println(putItem)

		if err != nil {
			panic(fmt.Sprintf("failed to marshal map, %v", err))
		}

		_, err = svc.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String(TABLENAME),
			Item:      putItem,
		})
		if err != nil {
			panic(fmt.Sprintf("failed 5 to put item, %v", err))
		}
	}

	for i := 0; i < len(e.Registrar.Certificado); i++ {
		e.Registrar.Certificado[i].Pk = "CERTIFICADO"
		e.Registrar.Certificado[i].Sk = strconv.Itoa(nextIde) + "#" + e.Registrar.Certificado[i].Atributo

		putItem, err := MarshalMap(e.Registrar.Certificado[i])

		if err != nil {
			panic(fmt.Sprintf("failed to marshal map, %v", err))
		}

		_, err = svc.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String(TABLENAME),
			Item:      putItem,
		})
		if err != nil {
			panic(fmt.Sprintf("failed 6 to put item, %v", err))
		}
	}

	for i := 0; i < len(e.Registrar.Rol); i++ {
		e.Registrar.Rol[i].Pk = "ROL"
		e.Registrar.Rol[i].Sk = strconv.Itoa(nextIde) + "#" + e.Registrar.Rol[i].Atributo

		putItem, err := MarshalMap(e.Registrar.Rol[i])

		if err != nil {
			panic(fmt.Sprintf("failed to marshal map, %v", err))
		}

		_, err = svc.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String(TABLENAME),
			Item:      putItem,
		})

		if err != nil {
			panic(fmt.Sprintf("failed 7 to put item, %v", err))
		}
	}

	for i := 0; i < len(e.Registrar.Poliza); i++ {
		e.Registrar.Poliza[i].Pk = "POLIZAENT"
		e.Registrar.Poliza[i].Sk = strconv.Itoa(nextIde) + "#" + e.Registrar.Poliza[i].Atributo

		putItem, err := MarshalMap(e.Registrar.Poliza[i])

		if err != nil {
			panic(fmt.Sprintf("failed to marshal map, %v", err))
		}

		_, err = svc.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String(TABLENAME),
			Item:      putItem,
		})

		if err != nil {
			panic(fmt.Sprintf("failed 8 to put item, %v", err))
		}
	}

	for i := 0; i < len(e.Registrar.Credito); i++ {
		e.Registrar.Credito[i].Pk = "CREDITO"
		e.Registrar.Credito[i].Sk = strconv.Itoa(nextIde) + "#" + e.Registrar.Credito[i].Atributo

		putItem, err := MarshalMap(e.Registrar.Credito[i])

		if err != nil {
			panic(fmt.Sprintf("failed to marshal map, %v", err))
		}

		_, err = svc.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String(TABLENAME),
			Item:      putItem,
		})

		if err != nil {
			panic(fmt.Sprintf("failed 9 to put item, %v", err))
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

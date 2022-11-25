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

type Configuracion struct {
	DatosGenerales   DatosGenerales   `json:"datosGenerales"`
	ColeccionPolizas []Poliza         `json:"coleccionPolizas"`
	Lectura          []Atributos      `json:"lectura"`
	Registrar        Registrar        `json:"registrar"`
	Notificaciones   []Notificaciones `json:"notificaciones"`
}

type ConfigEvent struct {
	Event Configuracion `json:"event"`
}

type Numerator struct {
	Ide int `json:"ide"`
}

func handler(ctx context.Context, config ConfigEvent) (string, error) {
	e := config.Event
	TABLENAME := os.Getenv("TableName")
	REGION := os.Getenv("Region")

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(REGION)},
	)

	if err != nil {
		panic(fmt.Sprintf("failed to create session, %v", err))
	}

	svc := dynamodb.New(sess)

	var coleccionPolizasItem map[string]*dynamodb.AttributeValue
	var lecturaItem map[string]*dynamodb.AttributeValue
	var notificacionesItem map[string]*dynamodb.AttributeValue
	var registrarClienteItem map[string]*dynamodb.AttributeValue
	var registrarCertificadoItem map[string]*dynamodb.AttributeValue
	var registrarRolItem map[string]*dynamodb.AttributeValue
	var registrarPolizaItem map[string]*dynamodb.AttributeValue
	var registrarCreditoItem map[string]*dynamodb.AttributeValue

	var batchItems []*dynamodb.WriteRequest

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

	datosGeneralesItem, err := MarshalMap(e.DatosGenerales)

	batchItems = append(batchItems, &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{
			Item: datosGeneralesItem,
		},
	})

	for i := 0; i < len(e.ColeccionPolizas); i++ {
		e.ColeccionPolizas[i].Pk = "POLIZA"
		e.ColeccionPolizas[i].Sk = strconv.Itoa(nextIde) + "#" + e.ColeccionPolizas[i].NPoliza
		coleccionPolizasItem, err = MarshalMap(e.ColeccionPolizas[i])

		batchItems = append(batchItems, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: coleccionPolizasItem,
			},
		})

	}

	for i := 0; i < len(e.Lectura); i++ {
		e.Lectura[i].Pk = strconv.Itoa(nextIde)

		if i+1 < 10 {
			e.Lectura[i].Sk = "00" + strconv.Itoa(i+1)
		} else if i+1 < 100 {
			e.Lectura[i].Sk = "0" + strconv.Itoa(i+1)
		} else {
			e.Lectura[i].Sk = strconv.Itoa(i + 1)
		}

		lecturaItem, err = MarshalMap(e.Lectura[i])

		batchItems = append(batchItems, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: lecturaItem,
			},
		})

	}

	for i := 0; i < len(e.Notificaciones); i++ {
		e.Notificaciones[i].Pk = "NOTIFICACION"
		e.Notificaciones[i].Sk = strconv.Itoa(nextIde) + "#" + e.Notificaciones[i].Fase + "#" + (e.Notificaciones[i].Id)

		notificacionesItem, err = MarshalMap(e.Notificaciones[i])

		batchItems = append(batchItems, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: notificacionesItem,
			},
		})

	}

	for i := 0; i < len(e.Registrar.Cliente); i++ {
		e.Registrar.Cliente[i].Pk = "CLIENTE"
		e.Registrar.Cliente[i].Sk = strconv.Itoa(nextIde) + "#" + e.Registrar.Cliente[i].Atributo

		registrarClienteItem, err = MarshalMap(e.Registrar.Cliente[i])

		batchItems = append(batchItems, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: registrarClienteItem,
			},
		})

	}

	for i := 0; i < len(e.Registrar.Certificado); i++ {
		e.Registrar.Certificado[i].Pk = "CERTIFICADO"
		e.Registrar.Certificado[i].Sk = strconv.Itoa(nextIde) + "#" + e.Registrar.Certificado[i].Atributo

		registrarCertificadoItem, err = MarshalMap(e.Registrar.Certificado[i])

		batchItems = append(batchItems, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: registrarCertificadoItem,
			},
		})

	}

	for i := 0; i < len(e.Registrar.Rol); i++ {
		e.Registrar.Rol[i].Pk = "ROL"
		e.Registrar.Rol[i].Sk = strconv.Itoa(nextIde) + "#" + e.Registrar.Rol[i].Atributo

		registrarRolItem, err = MarshalMap(e.Registrar.Rol[i])

		batchItems = append(batchItems, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: registrarRolItem,
			},
		})

	}

	for i := 0; i < len(e.Registrar.Poliza); i++ {
		e.Registrar.Poliza[i].Pk = "POLIZAENT"
		e.Registrar.Poliza[i].Sk = strconv.Itoa(nextIde) + "#" + e.Registrar.Poliza[i].Atributo

		registrarPolizaItem, err = MarshalMap(e.Registrar.Poliza[i])

		batchItems = append(batchItems, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: registrarPolizaItem,
			},
		})

	}

	for i := 0; i < len(e.Registrar.Credito); i++ {
		e.Registrar.Credito[i].Pk = "CREDITO"
		e.Registrar.Credito[i].Sk = strconv.Itoa(nextIde) + "#" + e.Registrar.Credito[i].Atributo

		registrarCreditoItem, err = MarshalMap(e.Registrar.Credito[i])

		batchItems = append(batchItems, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: registrarCreditoItem,
			},
		})

	}

	chunkSize := 10

	for i := 0; i < len(batchItems); i += chunkSize {
		end := i + chunkSize

		if end > len(batchItems) {
			end = len(batchItems)
		}

		batch := batchItems[i:end]

		out, _ := svc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				TABLENAME: batch,
			},
		})

		if out.UnprocessedItems != nil {
			fmt.Println("Unprocessed items")
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

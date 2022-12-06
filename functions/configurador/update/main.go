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
)


type PolicyType struct{
	Product string `json:"product"`
	NPolicy string `json:"nPolicy"`
	Contractor string `json:"contractor"`
	SalesChannel string `json:"salesChannel"`
	StartDate string `json:"startDate"`
	ExpirDate string `json:"expirDate"`
	Currency string `json:"currency"`
	Ruc string `json:"ruc"`
	Functions []int `json:"functions"`
}

type AttributeType struct{
	AttributeId string `json:"id"`
	Attribute string `json:"attribute"`
	DataType string `json:"dataType"`
	Required string `json:"required"`
	UniqueValue string `json:"uniqueValue"`
	Function []string `json:"function"`
	Origin []string `json:"origin"`
	Argument []string `json:"argument"`
	Domain []string `json:"domain"`
}

type EntityType struct{
	EntityId string `json:"entityId"`
	Attribute string `json:"attribute"`
	Description string `json:"description"`
	Origin string `json:"origin"`
	Value string `json:"value"`
	Equivalences []string `json:"equivalences"`
}


type UpdateObject struct {
	Op string `json:"op"`
	DataObject string `json:"dataObject"`

	StructureName string `json:"structureName"`
	Transaction string `json:"transaction"`
	Branch string `json:"branch"`
	Header string `json:"header"`
	CertificateByRole string `json:"certificateByRole"`
	RowsOrderByRole string `json:"rowsOrderByRole"`
	FileFormat string `json:"fileFormat"`
	DateFormat string `json:"dateFormat"`

	Policy PolicyType `json:"policy"`

	Attribute AttributeType `json:"attributes"`

	ClientEntity EntityType `json:"clientEntity"`

	CertificateEntity EntityType `json:"certificateEntity"`

	RoleEntity EntityType `json:"roleEntity"`

	PolicyEntity EntityType `json:"policyEntity"`

	CreditEntity EntityType `json:"creditEntity"`
}

type UpdateInput struct{
	StructureId string `json:"structureId"`
	Object []UpdateObject `json:"updateObject"`
}

type UpdateEvent struct{
	Event UpdateInput `json:"event"`
}


func handler(ctx context.Context, e UpdateEvent)(bool, error) {
	event := e.Event
	TABLENAME := os.Getenv("TableName")
	REGION := os.Getenv("Region")

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(REGION)},
	)

	if err != nil {
		panic(fmt.Sprintf("failed to create session, %v", err))
	}

	svc := dynamodb.New(sess)
	
	for _, object := range event.Object {
		if object.DataObject == "detail" {
			if object.Op == "UPDATE" {
				input := &dynamodb.UpdateItemInput{
					ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
						":structureName": {
							S: aws.String(object.StructureName),
						},
						":transaction": {
							S: aws.String(object.Transaction),
						},
						":branch": {
							S: aws.String(object.Branch),
						},
						":header": {
							S: aws.String(object.Header),
						},
						":certificateByRole": {
							S: aws.String(object.CertificateByRole),
						},
						":rowsOrderByRole": {
							S: aws.String(object.RowsOrderByRole),
						},
						":fileFormat": {
							S: aws.String(object.FileFormat),
						},
						":dateFormat": {
							S: aws.String(object.DateFormat),
						},
					},
					ExpressionAttributeNames: map[string]*string{
						"#transaction": aws.String("transaction"),
					},
					Key: map[string]*dynamodb.AttributeValue{
						"pk": {
							S: aws.String(event.StructureId),
						},
						"sk": {
							S: aws.String("DETAIL"),
						},
					},
					ReturnValues:     aws.String("UPDATED_NEW"),
					TableName:        aws.String(TABLENAME),
					UpdateExpression: aws.String("set structureName = :structureName, #transaction = :transaction, branch = :branch, header = :header, certificateByRole = :certificateByRole, rowsOrderByRole = :rowsOrderByRole, fileFormat = :fileFormat, dateFormat = :dateFormat"),
				}

				_, err := svc.UpdateItem(input)
				if err != nil {
					panic(fmt.Sprintf("failed to update item, %v", err))
				}
			} else {
				panic(fmt.Sprintf("failed to process item, %v", err))
			}
		} else if object.DataObject == "policy" {
			var functionsArr []*dynamodb.AttributeValue

			for _, function := range object.Policy.Functions {
				functionsArr = append(functionsArr, &dynamodb.AttributeValue{
					N: aws.String(strconv.Itoa(function)),
				})
			}

			if object.Op == "INSERT" {
				input := &dynamodb.PutItemInput{
					Item: map[string]*dynamodb.AttributeValue{
						"pk": {
							S: aws.String(event.StructureId),
						},
						"sk": {
							S: aws.String("POL-"+object.Policy.NPolicy),
						},
						"product": {
							S: aws.String(object.Policy.Product),
						},
						"nPolicy": {
							S: aws.String(object.Policy.NPolicy),
						},
						"contractor": {
							S: aws.String(object.Policy.Contractor),
						},
						"salesChannel": {
							S: aws.String(object.Policy.SalesChannel),
						},
						"startDate": {
							S: aws.String(object.Policy.StartDate),
						},
						"expirDate": {
							S: aws.String(object.Policy.ExpirDate),
						},
						"currency": {
							S: aws.String(object.Policy.Currency),
						},
						"ruc": {
							S: aws.String(object.Policy.Ruc),
						},
						"functions": {
							L: functionsArr,
						},
						"dataObject": {
							S: aws.String("policy"),
						},
		
					},
					TableName: aws.String(TABLENAME),
				}

				_, err := svc.PutItem(input)
				if err != nil {
					panic(fmt.Sprintf("failed to put item, %v", err))
				}
			} else if object.Op == "UPDATE" {
				input := &dynamodb.UpdateItemInput{
					ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
						":product": {
							S: aws.String(object.Policy.Product),
						},
						":nPolicy": {
							S: aws.String(object.Policy.NPolicy),
						},
						":contractor": {
							S: aws.String(object.Policy.Contractor),
						},
						":salesChannel": {
							S: aws.String(object.Policy.SalesChannel),
						},
						":startDate": {
							S: aws.String(object.Policy.StartDate),
						},
						":expirDate": {
							S: aws.String(object.Policy.ExpirDate),
						},
						":currency": {
							S: aws.String(object.Policy.Currency),
						},
						":ruc": {
							S: aws.String(object.Policy.Ruc),
						},
						":functions": {
							L: functionsArr,
						},
		
					},
					ExpressionAttributeNames: map[string]*string{
						"#functions": aws.String("functions"),
					},
					Key: map[string]*dynamodb.AttributeValue{
						"pk": {
							S: aws.String(event.StructureId),
						},
						"sk": {
							S: aws.String("POL-"+object.Policy.NPolicy),
						},
					},
					ReturnValues:     aws.String("UPDATED_NEW"),
					TableName:        aws.String(TABLENAME),	
					UpdateExpression: aws.String("set product = :product, nPolicy = :nPolicy, contractor = :contractor, salesChannel = :salesChannel, startDate = :startDate, expirDate = :expirDate, currency = :currency, ruc = :ruc, #functions = :functions"),
				}

				_, err := svc.UpdateItem(input)
				if err != nil {
					panic(fmt.Sprintf("failed to update item, %v", err))
				}
			} else if object.Op == "DELETE" {
				input := &dynamodb.DeleteItemInput{
					Key: map[string]*dynamodb.AttributeValue{
						"pk": {
							S: aws.String(event.StructureId),
						},
						"sk": {
							S: aws.String("POL-"+object.Policy.NPolicy),
						},
					},
					TableName: aws.String(TABLENAME),
				}

				_, err := svc.DeleteItem(input)
				if err != nil {
					panic(fmt.Sprintf("failed to delete item, %v", err))
				}
			} else {
				panic(fmt.Sprintf("failed to process item, %v", err))
			}
		} else if object.DataObject == "attribute" {

			var functionArr []*dynamodb.AttributeValue
			var originArr []*dynamodb.AttributeValue
			var argumentArr []*dynamodb.AttributeValue
			var domainArr []*dynamodb.AttributeValue

			for _, function := range object.Attribute.Function {
				functionArr = append(functionArr, &dynamodb.AttributeValue{
					S: aws.String(function),
				})
			}

			for _, origin := range object.Attribute.Origin {
				originArr = append(originArr, &dynamodb.AttributeValue{
					S: aws.String(origin),
				})
			}

			for _, argument := range object.Attribute.Argument {
				argumentArr = append(argumentArr, &dynamodb.AttributeValue{
					S: aws.String(argument),
				})
			}

			for _, domain := range object.Attribute.Domain {
				domainArr = append(domainArr, &dynamodb.AttributeValue{
					S: aws.String(domain),
				})
			}





			if object.Op == "INSERT" {
				input := &dynamodb.PutItemInput{
					Item: map[string]*dynamodb.AttributeValue{
						"pk": {
							S: aws.String(event.StructureId),
						},
						"sk": {
							S: aws.String(object.Attribute.AttributeId),
						},
						"attribute": {
							S: aws.String(object.Attribute.Attribute),
						},
						"dataType": {
							S: aws.String(object.Attribute.DataType),
						},
						"required": {
							S: aws.String(object.Attribute.Required),
						},
						"uniqueValue": {
							S: aws.String(object.Attribute.UniqueValue),
						},
						"function": {
							L: functionArr,
						},
						"origin": {
							L: originArr,
						},
						"argument": {
							L: argumentArr,
						},
						"domain": {
							L: domainArr,
						},
						"dataObject": {
							S: aws.String("attributes"),
						},
					},
					TableName: aws.String(TABLENAME),
				}

				_, err := svc.PutItem(input)
				if err != nil {
					panic(fmt.Sprintf("failed to put item, %v", err))
				}
			} else if object.Op == "UPDATE" {
				input := &dynamodb.UpdateItemInput{
					ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
						":attribute": {
							S: aws.String(object.Attribute.Attribute),
						},
						":dataType": {
							S: aws.String(object.Attribute.DataType),
						},
						":required": {
							S: aws.String(object.Attribute.Required),
						},
						":uniqueValue": {
							S: aws.String(object.Attribute.UniqueValue),
						},
						":function": {
							L: functionArr,
						},
						":origin": {
							L: originArr,
						},
						":argument": {
							L: argumentArr,
						},
						":domain": {
							L: domainArr,
						},
					},
					ExpressionAttributeNames: map[string]*string{
						"#function": aws.String("function"),
						"#domain":   aws.String("domain"),
						"#attribute": aws.String("attribute"),
					},
					Key: map[string]*dynamodb.AttributeValue{
						"pk": {
							S: aws.String(event.StructureId),
						},
						"sk": {
							S: aws.String(object.Attribute.AttributeId),
						},
					},
					ReturnValues:     aws.String("UPDATED_NEW"),
					TableName:        aws.String(TABLENAME),	
					UpdateExpression: aws.String("SET #attribute = :attribute, dataType = :dataType, required = :required, uniqueValue = :uniqueValue, #function = :function, origin = :origin, argument = :argument, #domain = :domain"),
				}

				_, err := svc.UpdateItem(input)
				if err != nil {
					panic(fmt.Sprintf("failed to update item, %v", err))
				}
			} else if object.Op == "DELETE" {
				input := &dynamodb.DeleteItemInput{
					Key: map[string]*dynamodb.AttributeValue{
						"pk": {
							S: aws.String(event.StructureId),
						},
						"sk": {
							S: aws.String(object.Attribute.AttributeId),
						},
					},
					TableName: aws.String(TABLENAME),
				}

				_, err := svc.DeleteItem(input)
				if err != nil {
					panic(fmt.Sprintf("failed to delete item, %v", err))
				}
			} else {
				panic(fmt.Sprintf("failed to process item, %v", err))
			}
		} else if object.DataObject == "entityClient" {
			equivalenceArr := []*dynamodb.AttributeValue{}
			for _,equi := range object.ClientEntity.Equivalences {
				equivalenceArr = append(equivalenceArr, &dynamodb.AttributeValue{
					S: aws.String(equi),
				})
			}
			

			input := &dynamodb.UpdateItemInput{
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":attribute": {
						S: aws.String(object.ClientEntity.Attribute),
					},
					":description": {
						S: aws.String(object.ClientEntity.Description),
					},
					":origin": {
						S: aws.String(object.ClientEntity.Origin),
					},
					":value": {
						S: aws.String(object.ClientEntity.Value),
					},
					":equivalence": {
						L: equivalenceArr,
					},
					
				},
				ExpressionAttributeNames: map[string]*string{
					"#value": aws.String("value"),
					"#attribute": aws.String("attribute"),
				},
				Key: map[string]*dynamodb.AttributeValue{
					"pk": {
						S: aws.String(event.StructureId),
					},
					"sk": {
						S: aws.String(object.ClientEntity.EntityId),
					},
				},
				ReturnValues:     aws.String("UPDATED_NEW"),
				TableName:        aws.String(TABLENAME),
				UpdateExpression: aws.String("SET #attribute = :attribute, description = :description, origin = :origin, #value = :value , equivalence = :equivalence"),
			}

			_, err := svc.UpdateItem(input)
			if err != nil {
				panic(fmt.Sprintf("failed to update item, %v", err))
			}
		} else if object.DataObject == "entityCertificate" {
			equivalenceArr := []*dynamodb.AttributeValue{}
			
			for _,equi := range object.CertificateEntity.Equivalences {
				equivalenceArr = append(equivalenceArr, &dynamodb.AttributeValue{
					S: aws.String(equi),
				})
			}

			input := &dynamodb.UpdateItemInput{
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{

					":attribute": {
						S: aws.String(object.CertificateEntity.Attribute),
					},
					":description": {
						S: aws.String(object.CertificateEntity.Description),
					},
					":origin": {
						S: aws.String(object.CertificateEntity.Origin),
					},
					":value": {
						S: aws.String(object.CertificateEntity.Value),
					},
					":equivalence": {
						L: equivalenceArr,
					},
				},
				ExpressionAttributeNames: map[string]*string{
					"#value": aws.String("value"),
					"#attribute": aws.String("attribute"),

				},
				Key: map[string]*dynamodb.AttributeValue{
					"pk": {
						S: aws.String(event.StructureId),
					},
					"sk": {
						S: aws.String(object.CertificateEntity.EntityId),
					},
				},
				ReturnValues:     aws.String("UPDATED_NEW"),
				TableName:        aws.String(TABLENAME),
				UpdateExpression: aws.String("SET #attribute = :attribute, description = :description, origin = :origin, #value = :value , equivalence = :equivalence"),
			}

			_, err := svc.UpdateItem(input)
			if err != nil {
				panic(fmt.Sprintf("failed to update item, %v", err))
			}
		} else if object.DataObject == "entityRole" {
			equivalenceArr := []*dynamodb.AttributeValue{}

			for _,equi := range object.RoleEntity.Equivalences {
				equivalenceArr = append(equivalenceArr, &dynamodb.AttributeValue{
					S: aws.String(equi),
				})
			}

			input := &dynamodb.UpdateItemInput{
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{

					":attribute": {
						S: aws.String(object.RoleEntity.Attribute),
					},
					":description": {
						S: aws.String(object.RoleEntity.Description),
					},
					":origin": {
						S: aws.String(object.RoleEntity.Origin),
					},
					":value": {
						S: aws.String(object.RoleEntity.Value),
					},
					":equivalence": {
						L: equivalenceArr,
					},
					
				},
				ExpressionAttributeNames: map[string]*string{
					"#value": aws.String("value"),
					"#attribute": aws.String("attribute"),

				},
				Key: map[string]*dynamodb.AttributeValue{
					"pk": {
						S: aws.String(event.StructureId),
					},
					"sk": {
						S: aws.String(object.RoleEntity.EntityId),
					},
				},
				ReturnValues:     aws.String("UPDATED_NEW"),
				TableName:        aws.String(TABLENAME),
				UpdateExpression: aws.String("SET #attribute = :attribute, description = :description, origin = :origin, #value = :value , equivalence = :equivalence"),
			}

			_, err := svc.UpdateItem(input)
			if err != nil {
				panic(fmt.Sprintf("failed to update item, %v", err))
			}
		} else if object.DataObject == "entityPolicy" {
			equivalenceArr := []*dynamodb.AttributeValue{}

			for _,equi := range object.PolicyEntity.Equivalences {
				equivalenceArr = append(equivalenceArr, &dynamodb.AttributeValue{
					S: aws.String(equi),
				})
			}

			input := &dynamodb.UpdateItemInput{
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{

					":attribute": {
						S: aws.String(object.PolicyEntity.Attribute),
					},
					":description": {
						S: aws.String(object.PolicyEntity.Description),
					},
					":origin": {
						S: aws.String(object.PolicyEntity.Origin),
					},
					":value": {
						S: aws.String(object.PolicyEntity.Value),
					},
					":equivalence": {
						L: equivalenceArr,
					},
				},
				ExpressionAttributeNames: map[string]*string{
					"#value": aws.String("value"),
					"#attribute": aws.String("attribute"),

				},
				Key: map[string]*dynamodb.AttributeValue{
					"pk": {
						S: aws.String(event.StructureId),
					},
					"sk": {
						S: aws.String(object.PolicyEntity.EntityId),
					},
				},
				ReturnValues:     aws.String("UPDATED_NEW"),
				TableName:        aws.String(TABLENAME),
				UpdateExpression: aws.String("SET #attribute = :attribute, description = :description, origin = :origin, #value = :value , equivalence = :equivalence"),
			}

			_, err := svc.UpdateItem(input)
			if err != nil {
				panic(fmt.Sprintf("failed to update item, %v", err))
			}
		} else if object.DataObject == "entityCredit" {
			equivalenceArr := []*dynamodb.AttributeValue{}

			for _,equi := range object.CreditEntity.Equivalences {
				equivalenceArr = append(equivalenceArr, &dynamodb.AttributeValue{
					S: aws.String(equi),
				})
			}

			input := &dynamodb.UpdateItemInput{
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{

					":attribute": {
						S: aws.String(object.CreditEntity.Attribute),
					},
					":description": {
						S: aws.String(object.CreditEntity.Description),
					},
					":origin": {
						S: aws.String(object.CreditEntity.Origin),
					},
					":value": {
						S: aws.String(object.CreditEntity.Value),
					},
					":equivalence": {
						L: equivalenceArr,
					},
				},
				ExpressionAttributeNames: map[string]*string{
					"#value": aws.String("value"),
					"#attribute": aws.String("attribute"),

				},
				Key: map[string]*dynamodb.AttributeValue{
					"pk": {
						S: aws.String(event.StructureId),
					},
					"sk": {
						S: aws.String(object.CreditEntity.EntityId),
					},
				},
				ReturnValues:     aws.String("UPDATED_NEW"),
				TableName:        aws.String(TABLENAME),
				UpdateExpression: aws.String("SET #attribute = :attribute, description = :description, origin = :origin, #value = :value , equivalence = :equivalence"),
			}

			_, err := svc.UpdateItem(input)
			if err != nil {
				panic(fmt.Sprintf("failed to update item, %v", err))
			}
		}
	}

	return true, nil
}

func main() {
	lambda.Start(handler)
}
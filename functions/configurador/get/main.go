package main

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type Details struct {
	StructureName     string `json:"structureName"`
	Transaction       string `json:"transaction"`
	Branch            string `json:"branch"`
	Header            string `json:"header"`
	CertificateByRole string `json:"certificateByRole"`
	RowsOrderByRole   string `json:"rowsOrderByRole"`
	FileFormat        string `json:"fileFormat"`
	DateFormat        string `json:"dateFormat"`
	DataObject        string `default:"details"`
	State 		   string `json:"state"`
}

type Policy struct {
	StructureName     string `json:"structureName"`
	Product      string `json:"product"`
	NPolicy      string `json:"nPolicy"`
	Contractor   string `json:"contractor"`
	SalesChannel string `json:"salesChannel"`
	StartDate    string `json:"startDate"`
	ExpirDate    string `json:"expirDate"`
	Currency     string `json:"currency"`
	Ruc          string `json:"ruc"`
	Functions    []int  `json:"functions"`
	DataObject   string `default:"policy"`
	State 		   string `json:"state"`

}

type Atribute struct {
	Id          string   `json:"id"`
	Attribute   string   `json:"attribute"`
	DataType    string   `json:"dataType"`
	Required    string   `json:"required"`
	UniqueValue string   `json:"uniqueValue"`
	Function    []string `json:"function"`
	Origin      []string `json:"origin"`
	Argument    []string `json:"argument"`
	Domain      []string `json:"domain"`
	DataObject  string   `default:"attribute"`
}

type Entity struct {
	Attribute   string `json:"attribute"`
	Description string `json:"description"`
	Origin      string `json:"origin"`
	Value       string `json:"value"`
	DataObject  string `default:"entity"`
}

type Configuration struct {
	StructureName      string         `json:"structureName"`
	Transaction        string         `json:"transaction"`
	Branch             string         `json:"branch"`
	Header             string         `json:"header"`
	CertificateByRole  string         `json:"certificateByRole"`
	RowsOrderByRole    string         `json:"rowsOrderByRole"`
	FileFormat         string         `json:"fileFormat"`
	DateFormat         string         `json:"dateFormat"`
	CollectionPolicies []Policy       `json:"policyCollection"`
	Attributes         []Atribute     `json:"attributes"`
	Client             []Entity       `json:"client"`
	Certificate        []Entity       `json:"certificate"`
	Role               []Entity       `json:"role"`
	Policy             []Entity       `json:"policy"`
	Credit             []Entity       `json:"credit"`
}

type Detail struct {
	StructureName      string         `json:"structureName"`
	Transaction        string         `json:"transaction"`
	Branch             string         `json:"branch"`
	Header             string         `json:"header"`
	CertificateByRole  string         `json:"certificateByRole"`
	RowsOrderByRole    string         `json:"rowsOrderByRole"`
	FileFormat         string         `json:"fileFormat"`
	DateFormat         string         `json:"dateFormat"`
}


type Event struct {
	StructureId string `json:"structureId"`
}

func handler(ctx context.Context, event Event) (Configuration, error) {
	TABLENAME := os.Getenv("TableName")
	REGION := os.Getenv("Region")

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(REGION)},
	)

	if err != nil {
		panic(fmt.Sprintf("failed to create session, %v", err))
	}

	svc := dynamodb.New(sess)

	input := &dynamodb.QueryInput{
		TableName: aws.String(TABLENAME),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {
				S: aws.String(event.StructureId),
			},
		},
		KeyConditionExpression: aws.String("pk = :pk"),
	}

	result, err := svc.Query(input)

	if err != nil {
		panic(fmt.Sprintf("failed to make Query API call, %v", err))
	}



	configuration := Configuration{}
	structureDetails := Details{}
	var err2 error

	fmt.Println("result.Items", result.Items)

	for _, i := range result.Items {
		if *i["DataObject"].S == "details" {
			err2 = dynamodbattribute.UnmarshalMap(i, &structureDetails)
		}

		if *i["DataObject"].S == "policy" {
			var policy Policy
			err2 = dynamodbattribute.UnmarshalMap(i, &policy)
			configuration.CollectionPolicies = append(configuration.CollectionPolicies, policy)
		}

		if *i["DataObject"].S == "attributes" {
			var attribute Atribute
			err2 = dynamodbattribute.UnmarshalMap(i, &attribute)
			configuration.Attributes = append(configuration.Attributes, attribute)
		}

		if *i["DataObject"].S == "entityClient" {
			var entity Entity
			err2 = dynamodbattribute.UnmarshalMap(i, &entity)
			configuration.Client = append(configuration.Client, entity)
		}

		if *i["DataObject"].S == "entityCertificate" {
			var entity Entity
			err2 = dynamodbattribute.UnmarshalMap(i, &entity)
			configuration.Certificate = append(configuration.Certificate, entity)
		}

		if *i["DataObject"].S == "entityRole" {
			var entity Entity
			err2 = dynamodbattribute.UnmarshalMap(i, &entity)
			configuration.Role = append(configuration.Role, entity)
		}

		if *i["DataObject"].S == "entityPolicy" {
			var entity Entity
			err2 = dynamodbattribute.UnmarshalMap(i, &entity)
			configuration.Policy = append(configuration.Policy, entity)
		}	

		if *i["DataObject"].S == "entityCredit" {
			var entity Entity
			err2 = dynamodbattribute.UnmarshalMap(i, &entity)
			configuration.Credit = append(configuration.Credit, entity)
		}

		if err2 != nil {
			panic(fmt.Sprintf("failed to unmarshal Dynamodb Record, %v", err))
		}

		

	}

	configuration.StructureName = structureDetails.StructureName
	configuration.Transaction = structureDetails.Transaction
	configuration.Branch = structureDetails.Branch
	configuration.Header = structureDetails.Header
	configuration.CertificateByRole = structureDetails.CertificateByRole
	configuration.RowsOrderByRole = structureDetails.RowsOrderByRole
	configuration.FileFormat = structureDetails.FileFormat
	configuration.DateFormat = structureDetails.DateFormat

	return configuration, nil
}

func main() {
	lambda.Start(handler)
}

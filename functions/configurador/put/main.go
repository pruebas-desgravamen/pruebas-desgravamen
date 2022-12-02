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

type Details struct {
	Pk                string `json:"pk"`
	Sk                string `json:"sk"`
	StructureName     string `json:"structureName"`
	Transaction       string `json:"transaction"`
	Branch            string `json:"branch"`
	Header            string `json:"header"`
	CertificateByRole string `json:"certificateByRole"`
	RowsOrderByRole   string `json:"rowsOrderByRole"`
	FileFormat        string `json:"fileFormat"`
	DateFormat        string `json:"dateFormat"`
	DataObject        string `default:"details"`
	State             string `json:"state"`
}

type Policy struct {
	Pk            string `json:"pk"`
	Sk            string `json:"sk"`
	Product       string `json:"product"`
	NPolicy       string `json:"nPolicy"`
	Contractor    string `json:"contractor"`
	SalesChannel  string `json:"salesChannel"`
	StartDate     string `json:"startDate"`
	ExpirDate     string `json:"expirDate"`
	Currency      string `json:"currency"`
	Ruc           string `json:"ruc"`
	Functions     []int  `json:"functions"`
	DataObject    string `default:"policy"`
	State         string `json:"state"`
	StructureName string `json:"structureName"`
}

type Atribute struct {
	Pk          string   `json:"pk"`
	Sk          string   `json:"sk"`
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

type Notification struct {
	Pk         string `json:"pk"`
	Sk         string `json:"sk"`
	Id         string `json:"id"`
	Event      string `json:"event"`
	Aplication string `json:"aplication"`
	Subject    string `json:"subject"`
	Template   string `json:"template"`
	Fase       string `json:"fase"`
	DataObject string `default:"notifications"`
}

type Entity struct {
	Pk          string `json:"pk"`
	Sk          string `json:"sk"`
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
	Notification       []Notification `json:"notifications"`
	StructureId        int            `json:"structureId"`
}

type ConfigEvent struct {
	Event Configuration `json:"event"`
}

type Numerator struct {
	Ide int `json:"ide"`
}

func handler(ctx context.Context, config ConfigEvent) (bool, error) {
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

	var policyCollectionItem map[string]*dynamodb.AttributeValue
	var attributesItem map[string]*dynamodb.AttributeValue
	var registerClientItem map[string]*dynamodb.AttributeValue
	var registerCertificateItem map[string]*dynamodb.AttributeValue
	var registerRoleItem map[string]*dynamodb.AttributeValue
	var registerPolicyItem map[string]*dynamodb.AttributeValue
	var registerCreditItem map[string]*dynamodb.AttributeValue

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

	structureName := e.Transaction + e.Branch + "-E" + strconv.Itoa(nextIde)

	details := Details{
		Pk:                "STR-" + strconv.Itoa(nextIde),
		Sk:                "DETAIL",
		StructureName:     structureName,
		Transaction:       e.Transaction,
		Branch:            e.Branch,
		Header:            e.Header,
		CertificateByRole: e.CertificateByRole,
		RowsOrderByRole:   e.RowsOrderByRole,
		FileFormat:        e.FileFormat,
		DateFormat:        e.DateFormat,
		DataObject:        "details",
		State:             "active",
	}

	detailsItem, err := MarshalMap(details)

	batchItems = append(batchItems, &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{
			Item: detailsItem,
		},
	})

	for i := 0; i < len(e.CollectionPolicies); i++ {
		e.CollectionPolicies[i].Pk = "STR-" + strconv.Itoa(nextIde)
		e.CollectionPolicies[i].Sk = "POL-" + e.CollectionPolicies[i].NPolicy
		e.CollectionPolicies[i].DataObject = "policy"
		e.CollectionPolicies[i].StructureName = structureName
		e.CollectionPolicies[i].State = "active"
		policyCollectionItem, err = MarshalMap(e.CollectionPolicies[i])
		batchItems = append(batchItems, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: policyCollectionItem,
			},
		})

	}

	for i := 0; i < len(e.Attributes); i++ {
		e.Attributes[i].Pk = "STR-" + strconv.Itoa(nextIde)
		if i+1 < 10 {
			e.Attributes[i].Sk = "ATTR-" + "00" + strconv.Itoa(i+1)
		} else if i+1 < 100 {
			e.Attributes[i].Sk = "ATTR-" + "0" + strconv.Itoa(i+1)
		} else {
			e.Attributes[i].Sk = "ATTR-" + strconv.Itoa(i+1)
		}
		e.Attributes[i].DataObject = "attributes"
		attributesItem, err = MarshalMap(e.Attributes[i])

		batchItems = append(batchItems, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: attributesItem,
			},
		})

	}

	for i := 0; i < len(e.Client); i++ {
		e.Client[i].Pk = "STR-" + strconv.Itoa(nextIde)
		if i+1 < 10 {
			e.Client[i].Sk = "CLIENTE#" + "00" + strconv.Itoa(i+1)
		} else if i+1 < 100 {
			e.Client[i].Sk = "CLIENTE#" + "0" + strconv.Itoa(i+1)
		} else {
			e.Client[i].Sk = "CLIENTE#" + strconv.Itoa(i+1)
		}
		e.Client[i].DataObject = "entityClient"
		registerClientItem, err = MarshalMap(e.Client[i])

		batchItems = append(batchItems, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: registerClientItem,
			},
		})

	}

	for i := 0; i < len(e.Certificate); i++ {
		e.Certificate[i].Pk = "STR-" + strconv.Itoa(nextIde)
		if i+1 < 10 {
			e.Certificate[i].Sk = "CERTIFICATE#" + "00" + strconv.Itoa(i+1)
		} else if i+1 < 100 {
			e.Certificate[i].Sk = "CERTIFICATE#" + "0" + strconv.Itoa(i+1)
		} else {
			e.Certificate[i].Sk = "CERTIFICATE#" + strconv.Itoa(i+1)
		}
		e.Certificate[i].DataObject = "entityCertificate"
		registerCertificateItem, err = MarshalMap(e.Certificate[i])

		batchItems = append(batchItems, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: registerCertificateItem,
			},
		})

	}

	for i := 0; i < len(e.Role); i++ {
		e.Role[i].Pk = "STR-" + strconv.Itoa(nextIde)
		if i+1 < 10 {
			e.Role[i].Sk = "ROLE#" + "00" + strconv.Itoa(i+1)
		} else if i+1 < 100 {
			e.Role[i].Sk = "ROLE#" + "0" + strconv.Itoa(i+1)
		} else {
			e.Role[i].Sk = "ROLE#" + strconv.Itoa(i+1)
		}
		e.Role[i].DataObject = "entityRole"
		registerRoleItem, err = MarshalMap(e.Role[i])

		batchItems = append(batchItems, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: registerRoleItem,
			},
		})

	}

	for i := 0; i < len(e.Policy); i++ {
		e.Policy[i].Pk = "STR-" + strconv.Itoa(nextIde)
		if i+1 < 10 {
			e.Policy[i].Sk = "POLICY#" + "00" + strconv.Itoa(i+1)
		} else if i+1 < 100 {
			e.Policy[i].Sk = "POLICY#" + "0" + strconv.Itoa(i+1)
		} else {
			e.Policy[i].Sk = "POLICY#" + strconv.Itoa(i+1)
		}
		e.Policy[i].DataObject = "entityPolicy"

		registerPolicyItem, err = MarshalMap(e.Policy[i])

		batchItems = append(batchItems, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: registerPolicyItem,
			},
		})

	}

	for i := 0; i < len(e.Credit); i++ {
		e.Credit[i].Pk = "STR-" + strconv.Itoa(nextIde)
		if i+1 < 10 {
			e.Credit[i].Sk = "CREDIT#" + "00" + strconv.Itoa(i+1)
		} else if i+1 < 100 {
			e.Credit[i].Sk = "CREDIT#" + "0" + strconv.Itoa(i+1)
		} else {
			e.Credit[i].Sk = "CREDIT#" + strconv.Itoa(i+1)
		}
		e.Credit[i].DataObject = "entityCredit"
		registerCreditItem, err = MarshalMap(e.Credit[i])

		batchItems = append(batchItems, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: registerCreditItem,
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

		out, err := svc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				TABLENAME: batch,
			},
		})

		fmt.Println(err)

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

	return true, nil
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

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



type StructureType struct {
	Pk string `json:"pk"`
	Sk string `json:"sk"`
}

type Event struct{
	Structure []map[string]*dynamodb.AttributeValue `json:"structure"`
}


func handler(ctx context.Context, event Event) (bool, error) {

	TABLENAME := os.Getenv("TableName")
	REGION := os.Getenv("Region")
    sess, err := session.NewSession(&aws.Config{
		Region: aws.String(REGION)},
	)

	if err != nil {
		panic(fmt.Sprintf("failed to create session, %v", err))
	}
    svc := dynamodb.New(sess)
	
	var batchItems []*dynamodb.WriteRequest

	for _, i := range event.Structure {
		var structure StructureType
		err = dynamodbattribute.UnmarshalMap(i, &structure)
		if err != nil {
			panic(fmt.Sprintf("failed to unmarshal Record, %v", err))
		}
		
		batchItems = append(batchItems, &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: map[string]*dynamodb.AttributeValue{
					"pk": {
						S: aws.String(structure.Pk),
					},
					"sk": {
						S: aws.String(structure.Sk),
					},
				},
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

		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				TABLENAME: batch,
			},
		}

		_, err = svc.BatchWriteItem(input)

		if err != nil {
			panic(fmt.Sprintf("failed to make BatchWriteItem API call, %v", err))
		}
	}

	return true, nil
}

func main() {
	lambda.Start(handler)
}

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func handler() (string, error) {

	var BUCKET_NAME = os.Getenv("BUCKET_NAME")

	//Iniciar sesion en aws
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(os.Getenv("us-east-1"))},
	)
	if err != nil {
		fmt.Println(err.Error())
		return "", nil
	}

	svc := s3.New(sess) // s3

	// Prepare the S3 request so a signature can be generated

	r, _ := svc.PutObjectRequest(&s3.PutObjectInput{
		Bucket: aws.String(BUCKET_NAME),
		Key:    aws.String("README.md"),
	})

	// Create the pre-signed url with an expiry
	url, err := r.Presign(15 * time.Minute)
	if err != nil {
		fmt.Println("Failed to generate a pre-signed url: ", err)
		return "", nil
	}

	// Display the pre-signed url
	// fmt.Println("Pre-signed URL", url)
	return url, nil
}

func main() {
	lambda.Start(handler)
}

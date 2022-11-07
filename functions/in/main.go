package main

import (
	"context"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
)

func handler() (string, error) {

	var BUCKET_NAME = os.Getenv("BUCKET_NAME")

	//Iniciar sesion en aws
	// sess, err := session.NewSession(&aws.Config{
	// 	Region: aws.String(os.Getenv("us-east-1"))},
	// )
	// if err != nil {
	// 	fmt.Println(err.Error())
	// 	return "", nil
	// }

	svc := s3.New(s3.Options{
		Region: os.Getenv("us-east-1"),
	})

	presignClient := s3.NewPresignClient(svc)
	// Prepare the S3 request so a signature can be generated

	// r, _ := svc.PutObjectRequest(&s3.PutObjectInput{
	// 	Bucket: aws.String(BUCKET_NAME),
	// 	Key:    aws.String("1050135.jpg"),
	// })

	presignParams := &s3.PutObjectInput{
		Bucket: aws.String(BUCKET_NAME),
		Key:    aws.String("1050135.jpg"),
	}

	presignDuration := func(po *s3.PresignOptions) {
		po.Expires = 5 * time.Minute
	}

	presignResult, err := presignClient.PresignPutObject(context.TODO(), presignParams, presignDuration)

	if err != nil {
		panic("Couldn't get presigned URL for GetObject")
	}
	// // Create the pre-signed url with an expiry
	// url, err := r.Presign(15 * time.Minute)
	// if err != nil {
	// 	fmt.Println("Failed to generate a pre-signed url: ", err)
	// 	return "", nil
	// }

	// // Display the pre-signed url
	// fmt.Println("Pre-signed URL", url)
	// return url, nil

	return presignResult.URL, nil
}

func main() {
	lambda.Start(handler)
}

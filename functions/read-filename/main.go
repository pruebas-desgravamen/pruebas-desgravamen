package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-lambda-go/lambda"
)

type Output struct {
	Filename    string
	NPolicy     string
	Transaction string
}

type Iobject struct {
	Key       string `json:"key"`
	Etag      string `json:"etag"`
	Sequencer string `json:"sequencer"`
}

type Evento struct {
	Object Iobject `json:"object"`
}

func handler(ctx context.Context, ev Evento) (Output, error) {

	var OBJECT_NAME = ev.Object.Key

	// Get data from File Name
	transaccion := OBJECT_NAME[0:2]
	fmt.Println(transaccion)
	if transaccion == "VE" {
		transaccion = "VENTA"
	}
	fmt.Println(transaccion)

	nPolicy := OBJECT_NAME[2:17]
	nPolicyInt, _ := strconv.Atoi(nPolicy)
	nPolicy = strconv.Itoa(nPolicyInt)
	fmt.Println(nPolicy)

	return Output{Filename: OBJECT_NAME, NPolicy: nPolicy, Transaction: transaccion}, nil
}

func main() {
	lambda.Start(handler)
}

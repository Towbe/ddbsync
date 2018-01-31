package ddbsync

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type item struct {
	Name    string
	Created int64
}

var db *dynamodb.DynamoDB

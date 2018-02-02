// Copyright 2012 Ryan Smith. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package ddbsync provides DynamoDB-backed synchronization primitives such
// as mutual exclusion locks. This package is designed to behave like pkg/sync.

package ddbsync

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	session2 "github.com/aws/aws-sdk-go/aws/session"
	"github.com/Towbe/TowbeApi/utils/helpers/env"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/Towbe/TowbeApi/utils/log"
)

type MutexKey struct {
	Id string `json:"id"`
}

// A Mutex is a mutual exclusion lock.
// Mutexes can be created as part of other structures.
type Mutex struct {
	MutexKey
	Ttl  int64
}

var session *session2.Session

func NewMutex() *Mutex {
	var config *aws.Config

	if env.IsAWS() {
		config = aws.NewConfig().WithRegion("us-east-1")
	} else {
		creds := credentials.NewSharedCredentials("", "towbe-dev")
		config = aws.NewConfig().WithRegion("us-east-1").WithCredentials(creds)
	}
	if db == nil {
		if session == nil {
			session = session2.New(config)
		}
		db = dynamodb.New(session)
	}

	return &Mutex{}
}

// Lock will write an item in a DynamoDB table if the item does not exist.
// Before writing the lock, we will clear any locks that are expired.
// Calling this function will block until a lock can be acquired.
func (m *Mutex) Lock() error {
	for {
		m.PruneExpired()
		putItemInput := &dynamodb.PutItemInput{
			TableName:	aws.String("towbe_locks"),
		}
		marshalledMutex, err := dynamodbattribute.MarshalMap(m)
		if err != nil {
			return err
		}
		putItemInput.SetConditionExpression("attribute_not_exists(hash)")
		putItemInput.SetItem(marshalledMutex)
		_, err = db.PutItem(putItemInput)
		if err == nil {
			return nil
		}
	}
}

func (m *Mutex) LockOrFail() error {
	//m.PruneExpired()
	log.Log("Locking " + m.Id)
	putItemInput := &dynamodb.PutItemInput{
		TableName:	aws.String("towbe_locks"),
	}

	marshalledMutex, err := dynamodbattribute.MarshalMap(m)
	if err != nil {
		return err
	}
	putItemInput.SetConditionExpression("attribute_not_exists(id)")
	putItemInput.SetItem(marshalledMutex)
	_, err = db.PutItem(putItemInput)
	return err
}

// Unlock will delete an item in a DynamoDB table.
func (m *Mutex) Unlock() error {
	for {
		deleteItemInput := &dynamodb.DeleteItemInput{
			TableName:	aws.String("towbe_locks"),
		}
		//deleteItemInput.SetConditionExpression("attributeExists(name)")
		marshalledKey, err := dynamodbattribute.MarshalMap(m.MutexKey)
		if err != nil {
			return err
		}

		deleteItemInput.SetKey(marshalledKey)
		_, err = db.DeleteItem(deleteItemInput)
		if err == nil {
			return nil
		}
	}
}



// PruneExpired delete all locks that have lived past their TTL.
// This is to prevent deadlock from processes that have taken locks
// but never removed them after execution. This commonly happens when a
// processor experiences network failure.
func (m *Mutex) PruneExpired() error {
	getItemInput := &dynamodb.GetItemInput{
		TableName:	aws.String("towbe_locks"),
	}

	marshalledKey, err := dynamodbattribute.MarshalMap(m.MutexKey)
	if err != nil {
		return err
	}

	getItemInput.SetKey(marshalledKey)
	_, err = db.GetItem(getItemInput)
	return err
}

func (m *Mutex) CheckIfExists() bool {
	getItemInput := &dynamodb.GetItemInput{
		TableName:	aws.String("towbe_locks"),
	}

	marshalledKey, err := dynamodbattribute.MarshalMap(m.MutexKey)
	if err != nil {
		return false
	}

	getItemInput.SetKey(marshalledKey)
	output, err := db.GetItem(getItemInput)
	if output.String() == "" {
		return true
	}
	return false
}

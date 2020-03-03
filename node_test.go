package quickstore

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var tn *node

func init() {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("ap-southeast-2"),
	}))
	client := dynamodb.New(sess)

	var err error
	tn, err = newNode(client, "quickstore-test", 1<<16, 20)
	if err != nil {
		panic(err)
	}
}

type Item struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func Test_node_insert(t *testing.T) {
	item := Item{
		Name:  "Hello",
		Value: "Hi",
	}
	err := tn.insert(Key{
		Parent: "a_parent",
		Kind: "static_key",
		Identifier: "hello",
	}, item)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	tn.close()
	<-tn.done
}

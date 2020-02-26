package qskey

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestParse_Success(t *testing.T) {
	Registry.Register("prj")
	key := Key{
		Parent:     "",
		Kind:       "prj",
		Identifier: RandIdentifier(),
	}
	parsed := Parse(key.String())
	if key != parsed {
		t.FailNow()
	}
}

func TestParse_Fail(t *testing.T) {
	key := Key{
		Parent:     "",
		Kind:       "prk",
		Identifier: RandIdentifier(),
	}
	parsed := Parse(key.String())
	if !parsed.Incomplete() {
		t.FailNow()
	}
}

func TestAnyKey_String(t *testing.T) {
	key := Key{
		Parent:     "key.parent",
		Kind:       "apk",
		Identifier: "3dF1k",
	}
	if key.String() != "key.parent.apk3dF1k" {
		t.FailNow()
	}
}

func TestAnyKey_MarshalDynamoDBAttributeValue(t *testing.T) {
	Registry.Register("abc")
	key := Key{
		Parent:     "",
		Kind:       "abc",
		Identifier: "1234",
	}
	av := dynamodb.AttributeValue{}
	err := key.MarshalDynamoDBAttributeValue(&av)
	if err != nil {
		t.Fatal(err)
	}
	if *av.S != "abc1234" {
		t.FailNow()
	}
}

func TestAnyKey_UnmarshalDynamoDBAttributeValue(t *testing.T) {
	Registry.Register("abc")
	av := dynamodb.AttributeValue{S: aws.String("abc1234")}
	key := Key{}
	err := key.UnmarshalDynamoDBAttributeValue(&av)
	if err != nil {
		t.Fatal(err)
	}
	expected := Key{
		Parent:     "",
		Kind:       "abc",
		Identifier: "1234",
	}
	if key != expected {
		t.FailNow()
	}
}

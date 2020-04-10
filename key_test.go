package quickstore

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func TestParse_Success(t *testing.T) {
	Registry.Register("prj")
	key := Key{
		Parent:     "",
		Kind:       "prj",
		Identifier: RandIdentifier(),
	}
	parsed := Parse(key.String())
	assert.Equal(t, key, parsed)
}

func TestParse_Fail(t *testing.T) {
	key := Key{
		Parent:     "",
		Kind:       "prk",
		Identifier: RandIdentifier(),
	}
	parsed := Parse(key.String())
	assert.True(t, parsed.Incomplete())
}

func TestAnyKey_String(t *testing.T) {
	key := Key{
		Parent:     "key.parent",
		Kind:       "apk",
		Identifier: "3dF1k",
	}
	assert.Equal(t, "key.parent.apk3dF1k", key.String())
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
	assert.NoError(t, err)
	assert.Equal(t, "abc1234", *av.S)
}

func TestAnyKey_UnmarshalDynamoDBAttributeValue(t *testing.T) {
	Registry.Register("abc")
	av := dynamodb.AttributeValue{S: aws.String("abc1234")}
	key := Key{}
	err := key.UnmarshalDynamoDBAttributeValue(&av)
	assert.NoError(t, err)
	expected := Key{
		Parent:     "",
		Kind:       "abc",
		Identifier: "1234",
	}
	assert.Equal(t, expected, key)
}

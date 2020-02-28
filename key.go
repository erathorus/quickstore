package quickstore

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	delim = '.'
)

type Key struct {
	Parent     string
	Kind       string
	Identifier string
}

func (k *Key) Incomplete() bool {
	return k.Kind == ""
}

func (k *Key) String() string {
	if k.Incomplete() {
		return ""
	}
	s := k.Kind + k.Identifier
	if k.Parent == "" {
		return s
	}
	return k.Parent + string(delim) + s
}

func Parse(s string) Key {
	front, rear := divideKey(s)
	for i := 1; i <= len(rear); i++ {
		if Registry.kinds[rear[:i]] {
			return Key{
				Parent: front,
				Kind: rear[:i],
				Identifier: rear[i:],
			}
		}
	}
	return Key{}
}

func (k *Key) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	av.S = aws.String(k.String())
	return nil
}

func (k *Key) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	s := ""
	if av.S != nil {
		s = *av.S
	}
	kk := Parse(s)
	k.Parent = kk.Parent
	k.Kind = kk.Kind
	k.Identifier = kk.Identifier
	return nil
}

func divideKey(s string) (string, string) {
	p := strings.LastIndexByte(s, delim)
	if p == -1 {
		return "", s
	}
	return s[:p], s[p+1:]
}

func RandIdentifier() string {
	b := make([]byte, 9)
	_, err := io.ReadFull(rand.Reader, b[:])
	if err != nil {
		panic(fmt.Sprintf("cannot generate unique bytes: %v", err))
	}
	return base64.URLEncoding.EncodeToString(b)
}

type KeyProvider interface {
	MarshalQuickstoreKey() (Key, error)
}


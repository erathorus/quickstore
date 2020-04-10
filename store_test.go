package quickstore

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/stretchr/testify/assert"
)

var store *Store

type Item struct {
	Name    string `json:"name"`
	Content string `json:"content"`
}

var (
	firstKey  = generateKey()
	firstItem = Item{
		Name:    "First Name",
		Content: "First Content",
	}

	secondKey  = generateKey()
	secondItem = Item{
		Name:    "Second Name",
		Content: "Second Content",
	}

	thirdKey  = generateKey()
	thirdItem = Item{
		Name:    "Third Name",
		Content: "Third Content",
	}
)

func TestStore_Insert(t *testing.T) {
	withContext(func(ctx context.Context) {
		err := store.Insert(firstKey, firstItem)
		assert.NoError(t, err)

		av, err := store.Get(ctx, firstKey)
		assert.NoError(t, err)

		actual := Item{}
		err = dynamodbattribute.Unmarshal(av, &actual)
		assert.NoError(t, err)

		assert.Equal(t, firstItem, actual)

		err = store.Delete(firstKey)
		assert.NoError(t, err)
	})
}

func TestStore_InsertTwice(t *testing.T) {
	err := store.Insert(firstKey, firstItem)
	assert.NoError(t, err)

	err = store.Insert(firstKey, secondItem)
	assert.IsType(t, err, &ErrItemExisted{})

	err = store.Delete(firstKey)
	assert.NoError(t, err)
}

func TestStore_Upsert(t *testing.T) {
	withContext(func(ctx context.Context) {
		err := store.Upsert(secondKey, secondItem)
		assert.NoError(t, err)

		av, err := store.Get(ctx, secondKey)
		assert.NoError(t, err)

		actual := Item{}
		err = dynamodbattribute.Unmarshal(av, &actual)
		assert.NoError(t, err)

		assert.Equal(t, secondItem, actual)

		err = store.Delete(secondKey)
		assert.NoError(t, err)
	})
}

func TestStore_UpsertTwice(t *testing.T) {
	err := store.Upsert(thirdKey, thirdItem)
	assert.NoError(t, err)

	err = store.Upsert(thirdKey, firstItem)
	assert.NoError(t, err)

	err = store.Delete(thirdKey)
	assert.NoError(t, err)
}

func TestStore_GetMulti(t *testing.T) {
	withContext(func(ctx context.Context) {
		err := store.Insert(firstKey, firstItem)
		assert.NoError(t, err)

		err = store.Insert(secondKey, secondItem)
		assert.NoError(t, err)

		err = store.Insert(thirdKey, thirdItem)
		assert.NoError(t, err)

		items, err := store.GetMulti(ctx, map[Key]bool{firstKey: true, secondKey: true, thirdKey: true})
		assert.NoError(t, err)

		firstActual := Item{}
		err = dynamodbattribute.Unmarshal(items[firstKey], &firstActual)
		assert.NoError(t, err)
		assert.Equal(t, firstItem, firstActual)

		secondActual := Item{}
		err = dynamodbattribute.Unmarshal(items[secondKey], &secondActual)
		assert.NoError(t, err)
		assert.Equal(t, secondItem, secondActual)

		thirdActual := Item{}
		err = dynamodbattribute.Unmarshal(items[thirdKey], &thirdActual)
		assert.NoError(t, err)
		assert.Equal(t, thirdItem, thirdActual)
	})
}

func generateKey() Key {
	return Key{
		Parent:     "",
		Kind:       "itm",
		Identifier: RandIdentifier(),
	}
}

func withContext(f func(ctx context.Context)) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f(ctx)
}

func beforeAll() {
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("ap-southeast-2")}))
	client := dynamodb.New(sess)
	var err error
	store, err = NewStore(client, "quickstore-test")
	if err != nil {
		panic(err)
	}
}

func afterAll() {
	store.CloseAndWait()
}

func TestMain(m *testing.M) {
	beforeAll()
	r := m.Run()
	afterAll()
	os.Exit(r)
}

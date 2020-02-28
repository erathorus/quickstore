package quickstore

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/hashicorp/golang-lru/simplelru"
)

const (
	cacheCapacity = 1 << 16
	timeout       = 60 * time.Second
)

type node struct {
	client *dynamodb.DynamoDB
	table  string

	qe     queue
	cache  *simplelru.LRU
	closed bool

	mu   sync.Mutex
	cond sync.Cond

	done chan struct{}
}

func newNode(client *dynamodb.DynamoDB, table string, bufSize int) (*node, error) {
	cache, err := simplelru.NewLRU(cacheCapacity, nil)
	if err != nil {
		return nil, err
	}
	n := &node{
		client: client,
		table:  table,
		qe: queue{
			cap:  bufSize,
			muts: make([]mutation, bufSize),
		},
		cache:  cache,
		closed: false,
		done:   make(chan struct{}, 1),
	}
	n.cond.L = &n.mu
	go n.flush()
	return n, nil
}

func (n *node) insert(key Key, value interface{}) error {
	ks := key.String()
	avs, err := encodeItem(key, value)
	if err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	cached, err := n.getOrSaveCache(key)
	if err != nil {
		return err
	}
	if cached.state == stateExist {
		return newErrItemExisted(key)
	}
	n.cache.Add(ks, cacheValue{
		state: stateExist,
		avs:   avs,
	})
	n.mutate(mutation{
		op:  opInsert,
		avs: avs,
	})
	return nil
}

func (n *node) upsert(key Key, value interface{}) error {
	ks := key.String()
	avs, err := encodeItem(key, value)
	if err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cache.Add(ks, cacheValue{
		state: stateExist,
		avs:   avs,
	})
	n.mutate(mutation{
		op:  opUpsert,
		avs: avs,
	})
	return nil
}

func (n *node) update(key Key, value interface{}) error {
	ks := key.String()
	avs, err := encodeItem(key, value)
	if err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	cached, err := n.getOrSaveCache(key)
	if err != nil {
		return err
	}
	if cached.state == stateNotExist {
		return newErrItemNotExisted(key)
	}
	n.cache.Add(ks, cacheValue{
		state: stateExist,
		avs:   avs,
	})
	n.mutate(mutation{
		op:  opUpdate,
		avs: avs,
	})
	return nil
}

func (n *node) delete(key Key) {
	ks := key.String()
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cache.Add(ks, cacheValue{state: stateNotExist})
	n.mutate(mutation{op: opDelete})
}

func (n *node) mutate(mut mutation) {

}

func (n *node) flush() {

}

type state int

const (
	stateNotExist state = iota
	stateExist
	stateUnknown
)

type cacheValue struct {
	state state
	avs   map[string]*dynamodb.AttributeValue
}

func (n *node) getOrSaveCache(key Key) (cacheValue, error) {
	ks := key.String()
	for {
		untyped, ok := n.cache.Get(ks)
		if !ok {
			break
		}
		cached := untyped.(cacheValue)
		if cached.state != stateUnknown {
			return cached, nil
		}
		n.cond.Wait()
	}
	value := cacheValue{state: stateUnknown}
	n.cache.Add(ks, value)
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	encoded, err := encodeKey(key)
	if err != nil {
		n.mu.Lock()
		return cacheValue{}, err
	}
	input := dynamodb.GetItemInput{
		Key:       encoded,
		TableName: &n.table,
	}
	output, err := n.client.GetItemWithContext(ctx, &input)
	if err != nil {
		n.mu.Lock()
		return cacheValue{}, newErrDynamoDBException(err)
	}
	if len(output.Item) == 0 {
		value.state = stateNotExist
	} else {
		value.state = stateExist
		value.avs = output.Item
	}
	n.mu.Lock()
	untyped, ok := n.cache.Get(key)
	if !ok || untyped.(cacheValue).state == stateUnknown {
		n.cache.Add(ks, value)
		return value, nil
	}
	return untyped.(cacheValue), nil
}

const (
	keyField = "_key"
)

func encodeItem(key Key, value interface{}) (map[string]*dynamodb.AttributeValue, error) {
	avs, err := dynamodbattribute.MarshalMap(value)
	if err != nil {
		return nil, newErrSerializeException("cannot marshal value", err)
	}
	keyAV := &dynamodb.AttributeValue{}
	err = key.MarshalDynamoDBAttributeValue(keyAV)
	if err != nil {
		return nil, newErrSerializeException("cannot marshal item's key", err)
	}
	avs[keyField] = keyAV
	return avs, nil
}

func encodeKey(key Key) (map[string]*dynamodb.AttributeValue, error) {
	av := &dynamodb.AttributeValue{}
	err := key.MarshalDynamoDBAttributeValue(av)
	if err != nil {
		return nil, newErrSerializeException("cannot marshal key", err)
	}
	avs := make(map[string]*dynamodb.AttributeValue)
	avs[keyField] = av
	return avs, nil
}

type opCode int

const (
	opInsert opCode = iota
	opUpsert
	opUpdate
	opDelete
)

type mutation struct {
	op  opCode
	avs map[string]*dynamodb.AttributeValue
}

type queue struct {
	muts []mutation
	cap  int
	len  int
	l, r int
}

func (q *queue) push(mut mutation) {
	q.muts[q.r] = mut
	q.r++
	if q.r == q.cap {
		q.r = 0
	}
	q.len++
}

func (q *queue) pop() mutation {
	mut := q.muts[q.l]
	q.l++
	if q.l == q.cap {
		q.l = 0
	}
	q.len--
	return mut
}

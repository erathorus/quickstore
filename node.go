package quickstore

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/hashicorp/golang-lru/simplelru"
)

const (
	cacheCapacity = 1 << 16
	maxGet        = 1 << 16
	maxThreshold  = 25
	timeout       = 60 * time.Second
)

type node struct {
	client    *dynamodb.DynamoDB
	table     string
	threshold int

	queue  queue
	cache  *simplelru.LRU
	closed bool

	locker    sync.Mutex
	keyConds  condSet
	flushCond sync.Cond

	done chan struct{}
}

func newNode(client *dynamodb.DynamoDB, table string, bufSize int, flushThreshold int) (*node, error) {
	cache, err := simplelru.NewLRU(cacheCapacity, nil)
	if err != nil {
		return nil, err
	}
	n := &node{
		client:    client,
		table:     table,
		threshold: flushThreshold,
		cache:     cache,
		closed:    false,
		done:      make(chan struct{}, 1),
	}
	n.queue = newQueue(bufSize, &n.locker)
	n.keyConds = newCondSet(maxGet, &n.locker)
	n.flushCond.L = &n.locker
	if n.threshold > maxThreshold {
		n.threshold = maxThreshold
	}
	go n.flush()
	return n, nil
}

func (n *node) insert(key Key, value interface{}) error {
	avs, err := encodeItem(key, value)
	if err != nil {
		return err
	}
	n.locker.Lock()
	defer n.locker.Unlock()
	if n.closed {
		return newErrClosed()
	}
	cached, err := n.getOrSaveCache(key)
	if err != nil {
		return err
	}
	if cached.state == stateExist {
		return newErrItemExisted(key)
	}
	n.mutate(key, mutation{
		op:  opInsert,
		avs: avs,
	})
	return nil
}

func (n *node) upsert(key Key, value interface{}) error {
	avs, err := encodeItem(key, value)
	if err != nil {
		return err
	}
	n.locker.Lock()
	defer n.locker.Unlock()
	if n.closed {
		return newErrClosed()
	}
	n.mutate(key, mutation{
		op:  opUpsert,
		avs: avs,
	})
	return nil
}

func (n *node) update(key Key, value interface{}) error {
	avs, err := encodeItem(key, value)
	if err != nil {
		return err
	}
	n.locker.Lock()
	defer n.locker.Unlock()
	if n.closed {
		return newErrClosed()
	}
	cached, err := n.getOrSaveCache(key)
	if err != nil {
		return err
	}
	if cached.state == stateNotExist {
		return newErrItemNotExisted(key)
	}
	n.mutate(key, mutation{
		op:  opUpdate,
		avs: avs,
	})
	return nil
}

func (n *node) delete(key Key) error {
	avs, err := encodeKey(key)
	if err != nil {
		return err
	}
	n.locker.Lock()
	defer n.locker.Unlock()
	if n.closed {
		return newErrClosed()
	}
	n.mutate(key, mutation{
		op:  opDelete,
		avs: avs,
	})
	return nil
}

func (n *node) mutate(key Key, mut mutation) {
	n.queue.push(mut)
	if n.queue.len >= n.threshold {
		n.flushCond.Signal()
	}
	switch mut.op {
	case opInsert:
		n.cache.Add(key, cacheValue{
			state: stateExist,
			avs:   mut.avs,
		})
	case opUpsert:
		n.cache.Add(key, cacheValue{
			state: stateExist,
			avs:   mut.avs,
		})
	case opUpdate:
		n.cache.Add(key, cacheValue{
			state: stateExist,
			avs:   mut.avs,
		})
	case opDelete:
		n.cache.Add(key, cacheValue{state: stateNotExist})
	}
}

func (n *node) close() {
	n.locker.Lock()
	defer n.locker.Unlock()
	if n.closed {
		return
	}
	n.closed = true
	n.flushCond.Signal()
}

func (n *node) flush() {
	defer func() {
		n.done <- struct{}{}
	}()
	muts := make([]mutation, n.queue.cap)
	var closed bool
	var err error
	for {
		n.locker.Lock()
		for !n.closed && n.queue.len < n.threshold {
			n.flushCond.Wait()
		}
		closed = n.closed
		muts = muts[:0]
		for !n.queue.empty() {
			muts = append(muts, n.queue.pop())
		}
		n.locker.Unlock()
		ok := true
		for i, mut := range muts {
			err = n.execute(mut)
			if err != nil {
				muts = muts[i:]
				ok = false
				break
			}
		}
		if !ok {
			break
		}
		if closed {
			return
		}
	}
	n.locker.Lock()
	n.closed = true
	for n.queue.len > 0 {
		muts = append(muts, n.queue.pop())
	}
	n.locker.Unlock()
	panic(err)
}

func (n *node) execute(mut mutation) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	switch mut.op {
	case opInsert:
		fallthrough
	case opUpsert:
		fallthrough
	case opUpdate:
		input := dynamodb.PutItemInput{
			Item:      mut.avs,
			TableName: &n.table,
		}
		_, err := n.client.PutItemWithContext(ctx, &input)
		return err
	case opDelete:
		input := dynamodb.DeleteItemInput{
			Key:       mut.avs,
			TableName: &n.table,
		}
		_, err := n.client.DeleteItemWithContext(ctx, &input)
		return err
	}

	return nil
}

type state int

const (
	stateNotExist state = iota
	stateExist
	stateBusy
)

type cacheValue struct {
	state state
	avs   map[string]*dynamodb.AttributeValue
}

func (n *node) getOrSaveCache(key Key) (cacheValue, error) {
	for {
		untyped, ok := n.cache.Get(key)
		if !ok {
			break
		}
		cached := untyped.(cacheValue)
		if cached.state != stateBusy {
			return cached, nil
		}
		ok = n.keyConds.waitAndSignal(key)
		if !ok {
			return cacheValue{}, newErrTooManyRequests(fmt.Sprintf("trying to access too many different keys"))
		}
	}
	value := cacheValue{state: stateBusy}
	n.cache.Add(key, value)
	n.locker.Unlock()

	defer n.keyConds.signal(key)

	encoded, err := encodeKey(key)
	if err != nil {
		n.locker.Lock()
		n.cache.Remove(key)
		return cacheValue{}, err
	}
	input := dynamodb.GetItemInput{
		Key:       encoded,
		TableName: &n.table,
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	output, err := n.client.GetItemWithContext(ctx, &input)
	if err != nil {
		n.locker.Lock()
		n.cache.Remove(key)
		return cacheValue{}, newErrDynamoDBException(err)
	}
	if len(output.Item) == 0 {
		value.state = stateNotExist
	} else {
		value.state = stateExist
		value.avs = output.Item
	}
	n.locker.Lock()
	untyped, ok := n.cache.Get(key)
	if !ok || untyped.(cacheValue).state == stateBusy {
		n.cache.Add(key, value)
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

	notFull  sync.Cond
	notEmpty sync.Cond
}

func newQueue(cap int, locker sync.Locker) queue {
	q := queue{
		muts: make([]mutation, cap),
		cap:  cap,
	}
	q.notFull.L = locker
	q.notEmpty.L = locker
	return q
}

func (q *queue) full() bool {
	return q.len == q.cap
}

func (q *queue) empty() bool {
	return q.len == 0
}

func (q *queue) push(mut mutation) {
	for q.full() {
		q.notFull.Wait()
	}
	q.muts[q.r] = mut
	q.r++
	if q.r == q.cap {
		q.r = 0
	}
	q.len++
	q.notEmpty.Signal()
}

func (q *queue) pop() mutation {
	for q.empty() {
		q.notEmpty.Wait()
	}
	mut := q.muts[q.l]
	q.l++
	if q.l == q.cap {
		q.l = 0
	}
	q.len--
	q.notFull.Signal()
	return mut
}

type condSet struct {
	locker sync.Locker
	cap    int

	entries map[Key]condCounter
	notFull sync.Cond
}

type condCounter struct {
	cond *sync.Cond
	cnt  int
}

func (c condCounter) inc() condCounter {
	c.cnt++
	return c
}

func (c condCounter) dec() condCounter {
	c.cnt--
	return c
}

func newCondSet(cap int, locker sync.Locker) condSet {
	c := condSet{
		entries: make(map[Key]condCounter),
		locker:  locker,
		cap:     cap,
	}
	c.notFull.L = locker
	return c
}

func (c *condSet) full() bool {
	return len(c.entries) == c.cap
}

func (c *condSet) waitAndSignal(key Key) bool {
	cc, ok := c.entries[key]
	if !ok {
		if c.full() {
			return false
		}
		cc = condCounter{cond: &sync.Cond{L: c.locker}}
	}
	c.entries[key] = cc.inc()
	cc.cond.Wait()
	cc = c.entries[key]
	if cc.cnt > 1 {
		c.entries[key] = cc.dec()
		cc.cond.Signal()
	} else {
		delete(c.entries, key)
	}
	return true
}

func (c *condSet) signal(key Key) {
	cc, ok := c.entries[key]
	if ok {
		cc.cond.Signal()
	}
}

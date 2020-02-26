package quickstore

import (
	"sync"

	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/erathorus/quickstore/qskey"
)

type node struct {
	client *dynamodb.DynamoDB

	qe     queue
	closed bool

	mu   sync.Mutex
	cond sync.Cond

	done chan struct{}
}

func newNode(client *dynamodb.DynamoDB, bufSize int) *node {
	n := &node{
		client: client,
		qe: queue{
			cap:  bufSize,
			muts: make([]mutation, bufSize),
		},
		closed: false,
		done:   make(chan struct{}, 1),
	}
	n.cond.L = &n.mu
	go n.flush()
	return n
}

func (n *node) insert(key qskey.Key, value interface{}) {

}

func (n *node) mutate(mut mutation) {

}

func (n *node) flush() {

}

type opCode int

const (
	opInsert opCode = iota
	opUpsert
	opUpdate
	opDelete
)

type mutation struct {
	op    opCode
	key   qskey.Key
	value interface{}
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

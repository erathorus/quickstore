package quickstore

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cespare/xxhash"
)

const (
	numNodes       = 16
	bufSize        = 1 << 16
	flushThreshold = 20
)

type Store struct {
	nodes []*node
}

func NewStore(client *dynamodb.DynamoDB, table string) (*Store, error) {
	nodes := make([]*node, numNodes)
	var err error
	for i := 0; i < len(nodes); i++ {
		nodes[i], err = newNode(client, table, bufSize, flushThreshold)
		if err != nil {
			return nil, err
		}
	}
	return &Store{
		nodes: nodes,
	}, nil
}

func (s *Store) Insert(kp KeyProvider, value interface{}) error {
	key, err := kp.MarshalQuickstoreKey()
	if err != nil {
		return err
	}
	return s.nodes[s.nodeOf(key)].insert(key, value)
}

func (s *Store) Upsert(kp KeyProvider, value interface{}) error {
	key, err := kp.MarshalQuickstoreKey()
	if err != nil {
		return err
	}
	return s.nodes[s.nodeOf(key)].upsert(key, value)
}

func (s *Store) Update(kp KeyProvider, value interface{}) error {
	key, err := kp.MarshalQuickstoreKey()
	if err != nil {
		return err
	}
	return s.nodes[s.nodeOf(key)].update(key, value)
}

func (s *Store) Delete(kp KeyProvider) error {
	key, err := kp.MarshalQuickstoreKey()
	if err != nil {
		return err
	}
	return s.nodes[s.nodeOf(key)].delete(key)
}

func (s *Store) nodeOf(key Key) int {
	return int(xxhash.Sum64String(key.String()) % uint64(len(s.nodes)))
}

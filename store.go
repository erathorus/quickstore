package quickstore

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cespare/xxhash"
)

const (
	numNodes       = 16
	bufSize        = 1 << 16
	flushThreshold = 20
)

type Store struct {
	nodes [numNodes]*node
}

func NewStore(client *dynamodb.DynamoDB, table string) (*Store, error) {
	var nodes [numNodes]*node
	var err error
	for i := 0; i < numNodes; i++ {
		nodes[i], err = newNode(client, table, bufSize, flushThreshold)
		if err != nil {
			return nil, err
		}
	}
	s := &Store{
		nodes: nodes,
	}
	return s, nil
}

func (s *Store) Insert(key Key, value interface{}) error {
	return s.nodes[s.nodeOf(key)].insert(key, value)
}

func (s *Store) Upsert(key Key, value interface{}) error {
	return s.nodes[s.nodeOf(key)].upsert(key, value)
}

func (s *Store) Update(key Key, value interface{}) error {
	return s.nodes[s.nodeOf(key)].update(key, value)
}

func (s *Store) Delete(key Key) error {
	return s.nodes[s.nodeOf(key)].delete(key)
}

func (s *Store) Get(ctx context.Context, key Key) (*dynamodb.AttributeValue, error) {
	return s.nodes[s.nodeOf(key)].get(ctx, key)
}

func (s *Store) GetMulti(ctx context.Context, keys map[Key]bool) (map[Key]*dynamodb.AttributeValue, error) {
	items := make(map[Key]*dynamodb.AttributeValue)
	var p [numNodes]map[Key]bool

	for i := 0; i < numNodes; i++ {
		p[i] = make(map[Key]bool)
	}

	for key := range keys {
		p[s.nodeOf(key)][key] = true
	}

	for i := 0; i < numNodes; i++ {
		if len(p[i]) == 0 {
			continue
		}
		output, err := s.nodes[i].getMulti(ctx, p[i])
		if err != nil {
			return nil, err
		}
		for key, item := range output {
			items[key] = item
		}
	}

	return items, nil
}

func (s *Store) DoesItemExist(ctx context.Context, key Key) (bool, error) {
	_, err := s.nodes[s.nodeOf(key)].get(ctx, key)
	if err != nil {
		if _, ok := err.(ErrItemNotExisted); ok {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Store) CloseAndWait() {
	for i := 0; i < numNodes; i++ {
		s.nodes[i].close()
	}
	for i := 0; i < numNodes; i++ {
		<-s.nodes[i].done
	}
}

func (s *Store) nodeOf(key Key) int {
	return int(xxhash.Sum64String(key.String()) % uint64(len(s.nodes)))
}

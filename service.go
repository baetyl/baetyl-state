package main

import (
	"context"

	"github.com/baetyl/baetyl-go/kv"
	"github.com/gogo/protobuf/types"
)

// KV kv interface
type KV interface {
	Set(kv *kv.KV) error
	Get(key string) (*kv.KV, error)
	Del(key string) error
	List(prefix string) (*kv.KVs, error)
}

// KVService kv server
type KVService struct {
	kv KV
}

// NewKVService new kv service
func NewKVService(kv KV) kv.KVServiceServer {
	return &KVService{kv: kv}
}

// Set set kv
func (s *KVService) Set(_ context.Context, kv *kv.KV) (*types.Empty, error) {
	return new(types.Empty), s.kv.Set(kv)
}

// Get get kv
func (s *KVService) Get(_ context.Context, kv *kv.KV) (*kv.KV, error) {
	return s.kv.Get(kv.Key)
}

// Del del kv
func (s *KVService) Del(_ context.Context, kv *kv.KV) (*types.Empty, error) {
	return new(types.Empty), s.kv.Del(kv.Key)
}

// List list kvs with prefix
func (s *KVService) List(_ context.Context, kv *kv.KV) (*kv.KVs, error) {
	return s.kv.List(kv.Key)
}

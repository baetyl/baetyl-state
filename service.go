package main

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/baetyl/baetyl-go/kv"
	"github.com/baetyl/baetyl-go/log"
	"github.com/gogo/protobuf/types"
	routing "github.com/qiangxue/fasthttp-routing"
	"github.com/valyala/fasthttp"
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
	kv  KV
	log *log.Logger
}

// KVHandler kv http handler
type KVHandler struct {
	kv  KV
	log *log.Logger
}

// NewKVService new kv service
func NewKVService(kv KV, log *log.Logger) kv.KVServiceServer {
	return &KVService{
		kv:  kv,
		log: log,
	}
}

// NewKVService new kv service
func NewKVHandler(kv KV, log *log.Logger) *KVHandler {
	return &KVHandler{
		kv:  kv,
		log: log,
	}
}

// Set set kv
func (s *KVService) Set(_ context.Context, kv *kv.KV) (*types.Empty, error) {
	s.log.Debug("storage set kv", log.Any("key", kv.Key), log.Any("value", kv.Value))
	return new(types.Empty), s.kv.Set(kv)
}

// Get get kv
func (s *KVService) Get(_ context.Context, kv *kv.KV) (*kv.KV, error) {
	s.log.Debug("storage get kv", log.Any("key", kv.Key))
	return s.kv.Get(kv.Key)
}

// Del del kv
func (s *KVService) Del(_ context.Context, kv *kv.KV) (*types.Empty, error) {
	s.log.Debug("storage del kv", log.Any("key", kv.Key))
	return new(types.Empty), s.kv.Del(kv.Key)
}

// List list kvs with prefix
func (s *KVService) List(_ context.Context, kv *kv.KV) (*kv.KVs, error) {
	s.log.Debug("storage list kvs", log.Any("key", kv.Key))
	return s.kv.List(kv.Key)
}

func (h *KVHandler) initRouter() fasthttp.RequestHandler {
	router := routing.New()
	router.Get("/", h.HList)
	router.Get("/<key>", h.HGet)
	router.Post("/", h.HSet)
	router.Delete("/<key>", h.HDelete)

	return router.HandleRequest
}

// HGet HGet
func (h *KVHandler) HGet(c *routing.Context) error {
	key := c.Param("key")
	_kv, err := h.kv.Get(key)
	if err != nil {
		respondError(c, 500, "ERR_DB", err.Error())
		return nil
	}
	data, err := json.Marshal(_kv)
	if err != nil {
		respondError(c, 500, "ERR_JSON", err.Error())
		return nil
	}
	respond(c, http.StatusOK, data)
	return nil
}

// HSet HSet
func (h *KVHandler) HSet(c *routing.Context) error {
	_kv := new(kv.KV)
	err := json.Unmarshal(c.Request.Body(), _kv)
	if err != nil {
		respondError(c, 500, "ERR_JSON", err.Error())
		return nil
	}
	err = h.kv.Set(_kv)
	if err != nil {
		respondError(c, 500, "ERR_DB", err.Error())
		return nil
	}

	respond(c, http.StatusOK, []byte(""))
	return nil
}

// HDelete HDelete
func (h *KVHandler) HDelete(c *routing.Context) error {
	key := c.Param("key")
	err := h.kv.Del(key)
	if err != nil {
		respondError(c, 500, "ERR_DB", err.Error())
		return nil
	}
	respond(c, http.StatusOK, []byte(""))
	return nil
}

// HList HList
func (h *KVHandler) HList(c *routing.Context) error {
	key := string(c.QueryArgs().Peek("prefix"))
	_kvs, err := h.kv.List(key)
	if err != nil {
		respondError(c, 500, "ERR_DB", err.Error())
		return nil
	}
	data, err := json.Marshal(_kvs)
	if err != nil {
		respondError(c, 500, "ERR_JSON", err.Error())
		return nil
	}
	respond(c, http.StatusOK, data)
	return nil
}

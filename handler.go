package main

import (
	"encoding/json"
	"net/http"

	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-state/database"
	routing "github.com/qiangxue/fasthttp-routing"
	"github.com/valyala/fasthttp"
)

// KVHandler kv http handler
type KVHandler struct {
	db  database.DB
	log *log.Logger
}

// NewKVHandler new kv service
func NewKVHandler(db database.DB, log *log.Logger) *KVHandler {
	return &KVHandler{
		db:  db,
		log: log,
	}
}

func (h *KVHandler) initRouter() fasthttp.RequestHandler {
	router := routing.New()
	router.Get("/", h.List)
	router.Get("/<key>", h.Get)
	router.Post("/", h.Set)
	router.Delete("/<key>", h.Delete)

	return router.HandleRequest
}

// Get Get
func (h *KVHandler) Get(c *routing.Context) error {
	key := c.Param("key")
	_kv, err := h.db.Get(key)
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

// Set Set
func (h *KVHandler) Set(c *routing.Context) error {
	kv := new(database.KV)
	err := json.Unmarshal(c.Request.Body(), kv)
	if err != nil {
		respondError(c, 500, "ERR_JSON", err.Error())
		return nil
	}
	err = h.db.Set(kv)
	if err != nil {
		respondError(c, 500, "ERR_DB", err.Error())
		return nil
	}

	respond(c, http.StatusOK, []byte(""))
	return nil
}

// Delete Delete
func (h *KVHandler) Delete(c *routing.Context) error {
	key := c.Param("key")
	err := h.db.Del(key)
	if err != nil {
		respondError(c, 500, "ERR_DB", err.Error())
		return nil
	}
	respond(c, http.StatusOK, []byte(""))
	return nil
}

// List List
func (h *KVHandler) List(c *routing.Context) error {
	key := string(c.QueryArgs().Peek("prefix"))
	_kvs, err := h.db.List(key)
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

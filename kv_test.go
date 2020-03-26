package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/baetyl/baetyl-go/utils"
	"github.com/baetyl/baetyl-state/database"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
)

func TestKV(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	_conf := Config{}
	_, err = NewServer(_conf)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "no such kind database")

	//ctx := context.Background()
	conf := struct {
		serverConf Config
		cliConf    mockClientConfig
	}{
		serverConf: Config{
			Database: database.Conf{
				Driver: "boltdb",
				Source: path.Join(dir, "kv3.db"),
			},
			Server: ServerConfig{
				Address: "127.0.0.1:50030",
			},
		},
		cliConf: mockClientConfig{
			Address: "http://127.0.0.1:50030",
		},
	}
	server, err := NewServer(conf.serverConf)
	assert.NoError(t, err)
	assert.NotEmpty(t, server)
	time.Sleep(time.Second)

	kv1 := database.KV{
		Key:   "key1",
		Value: []byte("value1"),
	}
	data, err := json.Marshal(kv1)
	assert.NoError(t, err)

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	url := conf.cliConf.Address
	req.SetRequestURI(url)
	req.Header.SetMethod("POST")
	req.SetBody(data)

	client := &fasthttp.Client{}
	err = client.Do(req, resp)
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode(), 200)

	req2 := fasthttp.AcquireRequest()
	resp2 := fasthttp.AcquireResponse()
	url2 := fmt.Sprintf("%s/%s", conf.cliConf.Address, "key1")
	req2.SetRequestURI(url2)
	req2.Header.SetMethod("GET")
	err = client.Do(req2, resp2)
	assert.NoError(t, err)
	assert.Equal(t, resp2.StatusCode(), 200)

	_kv2 := new(database.KV)
	err = json.Unmarshal(resp2.Body(), _kv2)
	assert.NoError(t, err)
	assert.Equal(t, _kv2.Key, "key1")
	assert.Equal(t, _kv2.Value, []byte("value1"))

	req3 := fasthttp.AcquireRequest()
	resp3 := fasthttp.AcquireResponse()
	url3 := fmt.Sprintf("%s/%s", conf.cliConf.Address, "key1")
	req3.SetRequestURI(url3)
	req3.Header.SetMethod("DELETE")
	err = client.Do(req3, resp3)
	assert.NoError(t, err)
	assert.Equal(t, resp3.StatusCode(), 200)

	req4 := fasthttp.AcquireRequest()
	resp4 := fasthttp.AcquireResponse()
	url4 := fmt.Sprintf("%s/%s", conf.cliConf.Address, "key1")
	req4.SetRequestURI(url4)
	req4.Header.SetMethod("GET")
	err = client.Do(req4, resp4)
	assert.NoError(t, err)
	assert.Equal(t, resp4.StatusCode(), 200)

	_kv4 := new(database.KV)
	err = json.Unmarshal(resp4.Body(), _kv4)
	assert.NoError(t, err)
	assert.Equal(t, _kv4.Key, "key1")

	kv2 := database.KV{
		Key:   "key2",
		Value: []byte("value2"),
	}
	kv3 := database.KV{
		Key:   "xey3",
		Value: []byte("value3"),
	}
	data2, err := json.Marshal(kv2)
	assert.NoError(t, err)
	data3, err := json.Marshal(kv3)
	assert.NoError(t, err)

	req5 := fasthttp.AcquireRequest()
	resp5 := fasthttp.AcquireResponse()
	url5 := conf.cliConf.Address
	req5.SetRequestURI(url5)
	req5.Header.SetMethod("POST")
	req5.SetBody(data2)
	err = client.Do(req5, resp5)
	assert.NoError(t, err)
	assert.Equal(t, resp5.StatusCode(), 200)

	req6 := fasthttp.AcquireRequest()
	resp6 := fasthttp.AcquireResponse()
	url6 := conf.cliConf.Address
	req6.SetRequestURI(url6)
	req6.Header.SetMethod("POST")
	req6.SetBody(data3)
	err = client.Do(req6, resp6)
	assert.NoError(t, err)
	assert.Equal(t, resp6.StatusCode(), 200)

	req8 := fasthttp.AcquireRequest()
	resp8 := fasthttp.AcquireResponse()
	url8 := conf.cliConf.Address
	req8.SetRequestURI(url8)
	req8.URI().SetQueryString("prefix=")
	req8.Header.SetMethod("GET")
	err = client.Do(req8, resp8)
	assert.NoError(t, err)
	assert.Equal(t, resp8.StatusCode(), 200)
	var _kvs8 []database.KV
	err = json.Unmarshal(resp8.Body(), &_kvs8)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(_kvs8))

	req9 := fasthttp.AcquireRequest()
	resp9 := fasthttp.AcquireResponse()
	url9 := conf.cliConf.Address
	req9.SetRequestURI(url9)
	req9.URI().SetQueryString("prefix=xey")
	req9.Header.SetMethod("GET")
	err = client.Do(req9, resp9)
	assert.NoError(t, err)
	assert.Equal(t, resp9.StatusCode(), 200)
	var _kvs9 []database.KV
	err = json.Unmarshal(resp9.Body(), &_kvs9)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(_kvs9))
}

func TestHttps(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	conf := struct {
		serverConf Config
		cliConf    mockClientConfig
	}{
		serverConf: Config{
			Database: database.Conf{
				Driver: "boltdb",
				Source: path.Join(dir, "kv3.db"),
			},
			Server: ServerConfig{
				Address: "127.0.0.1:50060",
				Certificate: utils.Certificate{
					Cert: "example/var/lib/baetyl/testcert/server.pem",
					Key:  "example/var/lib/baetyl/testcert/server.key",
				},
			},
		},
		cliConf: mockClientConfig{
			Address: "https://127.0.0.1:50060",
			Certificate: utils.Certificate{
				Key:                "example/var/lib/baetyl/testcert/client.key",
				Cert:               "example/var/lib/baetyl/testcert/client.pem",
				InsecureSkipVerify: true,
			},
		},
	}
	server, err := NewServer(conf.serverConf)
	assert.NoError(t, err)
	assert.NotEmpty(t, server)
	time.Sleep(time.Second)

	kv1 := database.KV{
		Key:   "key1",
		Value: []byte("value1"),
	}
	data, err := json.Marshal(kv1)
	assert.NoError(t, err)

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	url := conf.cliConf.Address
	req.SetRequestURI(url)
	req.Header.SetMethod("POST")
	req.SetBody(data)

	_tls, err := utils.NewTLSConfigClient(conf.cliConf.Certificate)
	assert.NoError(t, err)
	client := &fasthttp.Client{
		TLSConfig: _tls,
	}
	err = client.Do(req, resp)
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode(), 200)

	req2 := fasthttp.AcquireRequest()
	resp2 := fasthttp.AcquireResponse()
	url2 := fmt.Sprintf("%s/%s", conf.cliConf.Address, "key1")
	req2.SetRequestURI(url2)
	req2.Header.SetMethod("GET")
	err = client.Do(req2, resp2)
	assert.NoError(t, err)
	assert.Equal(t, resp2.StatusCode(), 200)

	_kv2 := new(database.KV)
	err = json.Unmarshal(resp2.Body(), _kv2)
	assert.NoError(t, err)
	assert.Equal(t, _kv2.Key, "key1")
	assert.Equal(t, _kv2.Value, []byte("value1"))

	req3 := fasthttp.AcquireRequest()
	resp3 := fasthttp.AcquireResponse()
	url3 := fmt.Sprintf("%s/%s", conf.cliConf.Address, "key1")
	req3.SetRequestURI(url3)
	req3.Header.SetMethod("DELETE")
	err = client.Do(req3, resp3)
	assert.NoError(t, err)
	assert.Equal(t, resp3.StatusCode(), 200)

	req4 := fasthttp.AcquireRequest()
	resp4 := fasthttp.AcquireResponse()
	url4 := fmt.Sprintf("%s/%s", conf.cliConf.Address, "key1")
	req4.SetRequestURI(url4)
	req4.Header.SetMethod("GET")
	err = client.Do(req4, resp4)
	assert.NoError(t, err)
	assert.Equal(t, resp4.StatusCode(), 200)

	_kv4 := new(database.KV)
	err = json.Unmarshal(resp4.Body(), _kv4)
	assert.NoError(t, err)
	assert.Equal(t, _kv4.Key, "key1")

	kv2 := database.KV{
		Key:   "key2",
		Value: []byte("value2"),
	}
	kv3 := database.KV{
		Key:   "xey3",
		Value: []byte("value3"),
	}
	data2, err := json.Marshal(kv2)
	assert.NoError(t, err)
	data3, err := json.Marshal(kv3)
	assert.NoError(t, err)

	req5 := fasthttp.AcquireRequest()
	resp5 := fasthttp.AcquireResponse()
	url5 := conf.cliConf.Address
	req5.SetRequestURI(url5)
	req5.Header.SetMethod("POST")
	req5.SetBody(data2)
	err = client.Do(req5, resp5)
	assert.NoError(t, err)
	assert.Equal(t, resp5.StatusCode(), 200)

	req6 := fasthttp.AcquireRequest()
	resp6 := fasthttp.AcquireResponse()
	url6 := conf.cliConf.Address
	req6.SetRequestURI(url6)
	req6.Header.SetMethod("POST")
	req6.SetBody(data3)
	err = client.Do(req6, resp6)
	assert.NoError(t, err)
	assert.Equal(t, resp6.StatusCode(), 200)

	req8 := fasthttp.AcquireRequest()
	resp8 := fasthttp.AcquireResponse()
	url8 := conf.cliConf.Address
	req8.SetRequestURI(url8)
	req8.URI().SetQueryString("prefix=")
	req8.Header.SetMethod("GET")
	err = client.Do(req8, resp8)
	assert.NoError(t, err)
	assert.Equal(t, resp8.StatusCode(), 200)
	var _kvs8 []database.KV
	err = json.Unmarshal(resp8.Body(), &_kvs8)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(_kvs8))

	req9 := fasthttp.AcquireRequest()
	resp9 := fasthttp.AcquireResponse()
	url9 := conf.cliConf.Address
	req9.SetRequestURI(url9)
	req9.URI().SetQueryString("prefix=xey")
	req9.Header.SetMethod("GET")
	err = client.Do(req9, resp9)
	assert.NoError(t, err)
	assert.Equal(t, resp9.StatusCode(), 200)
	var _kvs9 []database.KV
	err = json.Unmarshal(resp9.Body(), &_kvs9)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(_kvs9))
}

func TestHttpKVFail(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)
	registerMockDB()

	conf := struct {
		serverConf Config
		cliConf    mockClientConfig
	}{
		serverConf: Config{
			Database: database.Conf{
				Driver: "errdb",
				Source: path.Join(dir, "kv3.db"),
			},
			Server: ServerConfig{
				Address: "127.0.0.1:50040",
			},
		},
		cliConf: mockClientConfig{
			Address: "http://127.0.0.1:50040",
		},
	}
	server, err := NewServer(conf.serverConf)
	assert.NoError(t, err)
	assert.NotEmpty(t, server)
	time.Sleep(time.Second)

	kv1 := database.KV{
		Key:   "key1",
		Value: []byte("value1"),
	}
	data, err := json.Marshal(kv1)
	assert.NoError(t, err)

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	url := conf.cliConf.Address
	req.SetRequestURI(url)
	req.Header.SetMethod("POST")
	req.SetBody(data)
	client := &fasthttp.Client{}
	err = client.Do(req, resp)
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode(), 500)

	req2 := fasthttp.AcquireRequest()
	resp2 := fasthttp.AcquireResponse()
	url2 := fmt.Sprintf("%s/%s", conf.cliConf.Address, "key1")
	req2.SetRequestURI(url2)
	req2.Header.SetMethod("GET")
	err = client.Do(req2, resp2)
	assert.NoError(t, err)
	assert.Equal(t, resp2.StatusCode(), 500)

	req3 := fasthttp.AcquireRequest()
	resp3 := fasthttp.AcquireResponse()
	url3 := fmt.Sprintf("%s/%s", conf.cliConf.Address, "key1")
	req3.SetRequestURI(url3)
	req3.Header.SetMethod("DELETE")
	err = client.Do(req3, resp3)
	assert.NoError(t, err)
	assert.Equal(t, resp3.StatusCode(), 500)

	req4 := fasthttp.AcquireRequest()
	resp4 := fasthttp.AcquireResponse()
	url4 := conf.cliConf.Address
	req4.SetRequestURI(url4)
	req4.URI().SetQueryString("prefix=")
	req4.Header.SetMethod("GET")
	err = client.Do(req4, resp4)
	assert.NoError(t, err)
	assert.Equal(t, resp4.StatusCode(), 500)
}

func TestAddress(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	conf := struct {
		serverConf Config
		cliConf    mockClientConfig
	}{
		serverConf: Config{
			Database: database.Conf{
				Driver: "boltdb",
				Source: path.Join(dir, "kv3.db"),
			},
			Server: ServerConfig{
				Address: "http://127.0.0.1:50070",
			},
		},
		cliConf: mockClientConfig{
			Address: "http://127.0.0.1:50070",
		},
	}

	server, err := NewServer(conf.serverConf)
	assert.NoError(t, err)
	assert.NotEmpty(t, server)
	time.Sleep(time.Second)

	kv1 := database.KV{
		Key:   "key1",
		Value: []byte("value1"),
	}
	data, err := json.Marshal(kv1)
	assert.NoError(t, err)

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	url := conf.cliConf.Address
	req.SetRequestURI(url)
	req.Header.SetMethod("POST")
	req.SetBody(data)
	client := &fasthttp.Client{}
	err = client.Do(req, resp)
	assert.Error(t, err)

	conf2 := struct {
		serverConf Config
		cliConf    mockClientConfig
	}{
		serverConf: Config{
			Database: database.Conf{
				Driver: "boltdb",
				Source: path.Join(dir, "kv4.db"),
			},
			Server: ServerConfig{
				Address: "127.0.0.1:50080",
			},
		},
		cliConf: mockClientConfig{
			Address: "http://127.0.0.1:50080",
		},
	}

	server2, err := NewServer(conf2.serverConf)
	assert.NoError(t, err)
	assert.NotEmpty(t, server2)
	time.Sleep(time.Second)

	kv2 := database.KV{
		Key:   "key2",
		Value: []byte("value2"),
	}
	data2, err := json.Marshal(kv2)
	assert.NoError(t, err)

	req2 := fasthttp.AcquireRequest()
	resp2 := fasthttp.AcquireResponse()
	url2 := conf2.cliConf.Address
	req2.SetRequestURI(url2)
	req2.Header.SetMethod("POST")
	req2.SetBody(data2)
	client2 := &fasthttp.Client{}
	err = client2.Do(req2, resp2)
	assert.NoError(t, err)
}

type mockDB struct{}

func registerMockDB() {
	database.Factories["errdb"] = newMockDB
}

// New creates a new sql database
func newMockDB(conf database.Conf) (database.DB, error) {
	return new(mockDB), nil
}

func (d *mockDB) Conf() database.Conf {
	return database.Conf{}
}

// Set put key and value into SQL DB
func (d *mockDB) Set(kv *database.KV) error {
	return errors.New("custom error")
}

// Get gets value by key from SQL DB
func (d *mockDB) Get(key string) (*database.KV, error) {
	return nil, errors.New("custom error")
}

// Del deletes key and value from SQL DB
func (d *mockDB) Del(key string) error {
	return errors.New("custom error")
}

// List list kvs with the prefix
func (d *mockDB) List(prefix string) ([]database.KV, error) {
	return nil, errors.New("custom error")
}

func (d *mockDB) Close() error {
	return nil
}

// ClientConfig api client config
type mockClientConfig struct {
	Address           string `yaml:"address" json:"address"`
	utils.Certificate `yaml:",inline" json:",inline"`
}

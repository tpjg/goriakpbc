package riak

import (
	"github.com/bmizerany/assert"
	"testing"
)

func setupDefaultConnection(t *testing.T) {
	err := ConnectClient("127.0.0.1:8087")
	assert.T(t, err == nil)
}

func setupDefaultConnections(t *testing.T, count int) {
	err := ConnectClientPool("127.0.0.1:8087", count)
	assert.T(t, err == nil)
}

func TestDefaultCanConnect(t *testing.T) {
	setupDefaultConnection(t)
}

func TestDefaultPing(t *testing.T) {
	setupDefaultConnection(t)
	assert.T(t, Ping() == nil)
}

func TestDefaultGetServerVersion(t *testing.T) {
	setupDefaultConnection(t)

	node, response, err := ServerVersion()
	assert.T(t, err == nil)
	assert.T(t, node != "")
	assert.T(t, response != "")

	t.Logf("Riak server : %s with version %s\n", node, response)
}

func TestDefaultStoreObject(t *testing.T) {
	setupDefaultConnection(t)

	bucket, _ := NewBucket("client_test.go")
	assert.T(t, bucket != nil)
	obj := bucket.New("abc", PW1, DW1)
	assert.T(t, obj != nil)
	obj.ContentType = "text/plain"
	obj.Data = []byte("some more data")
	err := obj.Store()
	assert.T(t, err == nil)
	assert.T(t, obj.Vclock != nil)
}

func TestDefaultGetAndDeleteDirectly(t *testing.T) {
	setupDefaultConnection(t)

	obj, err := GetFrom("client_test.go", "abc", R1, PR1)
	assert.T(t, err == nil)
	assert.T(t, obj != nil)
	err = DeleteFrom("client_test.go", "abc")
	assert.T(t, err == nil)
}

func TestDefaultRunMapReduce(t *testing.T) {
	// Preparations
	setupDefaultConnection(t)
	bucket, _ := NewBucket("client_test.go")
	assert.T(t, bucket != nil)
	// Create object 1
	obj1 := bucket.New("mrobj1")
	assert.T(t, obj1 != nil)
	obj1.ContentType = "application/json"
	obj1.Data = []byte(`{"k":"v"}`)
	err := obj1.Store()
	assert.T(t, err == nil)
	// Create object 2
	obj2 := bucket.New("mrobj2")
	assert.T(t, obj2 != nil)
	obj2.ContentType = "application/json"
	obj2.Data = []byte(`{"k":"v2"}`)
	err = obj2.Store()
	assert.T(t, err == nil)
	// Link them
	obj1.LinkTo(obj2, "test")
	obj1.Store()

	//q := `{"inputs":[["client_test.go","mrobj1"]],"query":[{"map":{"language":"javascript","keep":true,"source":"function(v) { return [JSON.parse(v.values[0].data)]; }"}}]}`
	q := `{"inputs":[["client_test.go","mrobj1"]],"query":[{"map":{"language":"javascript","keep":true,"source":"function(v) { return [v]; }"}}]}`

	mr, err := RunMapReduce(q)
	assert.T(t, err == nil)
	assert.T(t, len(mr) == 1)
}

func TestDefaultMapReduce(t *testing.T) {
	// Preparations
	setupDefaultConnection(t)
	bucket, _ := NewBucket("client_test.go")
	assert.T(t, bucket != nil)
	// Create object 1
	obj1 := bucket.New("mrobj1")
	assert.T(t, obj1 != nil)
	obj1.ContentType = "application/json"
	obj1.Data = []byte(`{"k":"v"}`)
	err := obj1.Store()
	assert.T(t, err == nil)
	// Create object 2
	obj2 := bucket.New("mrobj2")
	assert.T(t, obj2 != nil)
	obj2.ContentType = "application/json"
	obj2.Data = []byte(`{"k":"v2"}`)
	err = obj2.Store()
	assert.T(t, err == nil)
	// Link them
	obj1.LinkTo(obj2, "test")
	obj1.Store()

	mr := NewMapReduce()
	mr.Add("client_test.go", "mrobj1")
	//mr.LinkBucket("bucketname", false)
	mr.Map("function(v) {return [v];}", true)
	res, err := mr.Run()
	assert.T(t, err == nil)
	assert.T(t, len(res) == 1)

	mr = NewMapReduce()
	mr.Add("client_test.go", "mrobj1")
	mr.MapObjectValue(true)
	res, err = mr.Run()
	assert.T(t, err == nil)
	assert.T(t, len(res) == 1)
}

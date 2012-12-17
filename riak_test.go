package riak

import (
	"fmt"
	"github.com/bmizerany/assert"
	"strconv"
	"testing"
)

func setupConnection(t *testing.T) (client *Client) {
	client = New("127.0.0.1:8087")
	err := client.Connect()
	assert.T(t, client != nil)
	assert.T(t, err == nil)

	return client
}

func TestCanConnect(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)
}

func TestPing(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)
	assert.T(t, client.Ping() == nil)
}

func TestGetServerVersion(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	node, response, err := client.ServerVersion()
	assert.T(t, err == nil)
	assert.T(t, node != "")
	assert.T(t, response != "")

	fmt.Printf("Riak server : %s with version %s\n", node, response)
}

func TestStoreObject(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	bucket := client.Bucket("client_test.go")
	assert.T(t, bucket != nil)
	obj := bucket.New("abc")
	assert.T(t, obj != nil)
	obj.ContentType = "text/plain"
	obj.Data = []byte("some more data")
	err := obj.Store()
	assert.T(t, err == nil)
	assert.T(t, obj.Vclock != nil)
	/*
		if err != nil {
			fmt.Printf("Fetch resulted in %s\n", err.Error())
		}
		fmt.Printf("obj data : %s\n", string(obj.Data))
		fmt.Printf("obj key  : %s\n", obj.Key)
		fmt.Printf("vclock   : %s\n", obj.Vclock)
	*/
}

func TestGetAndDeleteObject(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	bucket := client.Bucket("client_test.go")
	assert.T(t, bucket != nil)
	obj, err := bucket.Get("abc")
	assert.T(t, err == nil)
	assert.T(t, obj != nil)
	/*
		fmt.Printf("obj key  : %s\n", obj.Key)
		fmt.Printf("vclock   : %s\n", obj.Vclock)
		if obj.conflict {
			for i, v := range obj.Siblings {
				fmt.Printf("%d - %s\n", i, v.Data)
			}
		} else {
			fmt.Printf("obj data : %s\n", string(obj.Data))
		}
	*/
	err = bucket.Delete("abc")
	assert.T(t, err == nil)
}

func TestObjectsWithSiblings(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	bucket := client.Bucket("client_test.go")
	assert.T(t, bucket != nil)
	err := bucket.SetAllowMult(true)
	assert.T(t, err == nil)

	// Create an object with two siblings
	_ = bucket.Delete("def")
	obj := bucket.New("def")
	obj.ContentType = "text/plain"
	obj.Data = []byte("data 1")
	err = obj.Store()
	assert.T(t, err == nil)

	obj = bucket.New("def")
	obj.ContentType = "text/plain"
	obj.Data = []byte("data 2")
	err = obj.Store()
	assert.T(t, err == nil)

	obj, err = bucket.Get("def")
	assert.T(t, err == nil)
	assert.T(t, obj != nil)
	assert.T(t, obj.Conflict() == true)
	assert.T(t, len(obj.Siblings) == 2)

	// Cleanup
	err = obj.Destroy()
	assert.T(t, err == nil)
	err = bucket.SetAllowMult(false)
	assert.T(t, err == nil)
}

func TestObjectReload(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	bucket := client.Bucket("client_test.go")
	assert.T(t, bucket != nil)

	// Create an object
	obj := bucket.New("ghi")
	assert.T(t, obj != nil)
	obj.ContentType = "text/plain"
	obj.Data = []byte("test1")
	err := obj.Store()
	assert.T(t, err == nil)
	// Get the object in another variable
	obj2, err := bucket.Get("ghi")
	assert.T(t, err == nil)
	assert.T(t, obj2 != nil)
	// Change the original object
	obj.Data = []byte("test2")
	err = obj.Store()
	assert.T(t, err == nil)
	// Reload the second object
	err = obj2.Reload()
	assert.T(t, err == nil)
	assert.T(t, obj.Data != nil)
	assert.T(t, obj2.Data != nil)
	assert.T(t, string(obj.Data) == string(obj2.Data))

	// Cleanup
	err = obj.Destroy()
	assert.T(t, err == nil)
}

func TestExists(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	bucket := client.Bucket("client_test.go")
	assert.T(t, bucket != nil)

	// Check for a non-existing key
	e, err := bucket.Exists("alskgqwioetuioweqadfh")
	assert.T(t, err == nil)
	assert.T(t, e == false)

	// Create an object
	obj := bucket.New("alskgqwioetuioweqadfh")
	assert.T(t, obj != nil)
	obj.ContentType = "text/plain"
	obj.Data = []byte("test1")
	err = obj.Store()
	assert.T(t, err == nil)

	// Test if it exists now
	e, err = bucket.Exists("alskgqwioetuioweqadfh")
	assert.T(t, err == nil)
	assert.T(t, e == true)

	// Cleanup
	err = obj.Destroy()
	assert.T(t, err == nil)
}

func TestObjectLinks(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	bucket := client.Bucket("client_test.go")
	assert.T(t, bucket != nil)

	// Create object 1
	obj1 := bucket.New("sourceobj")
	assert.T(t, obj1 != nil)
	obj1.ContentType = "text/plain"
	obj1.Data = []byte("test1")
	err := obj1.Store()
	assert.T(t, err == nil)
	// Create object 2
	obj2 := bucket.New("targetobj")
	assert.T(t, obj2 != nil)
	obj2.ContentType = "text/plain"
	obj2.Data = []byte("test2")
	err = obj2.Store()
	assert.T(t, err == nil)

	// Link them
	obj1.LinkTo(obj2, "test")
	obj1.Store()

	// Fetch object and check if there is a link
	obj, err := bucket.Get("sourceobj")
	assert.T(t, err == nil)
	assert.T(t, obj != nil)
	assert.T(t, len(obj.Links) == 1)

	// Cleanup
	err = obj1.Destroy()
	assert.T(t, err == nil)
	err = obj2.Destroy()
	assert.T(t, err == nil)
}

func TestObjectMetadata(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	bucket := client.Bucket("client_test.go")
	assert.T(t, bucket != nil)

	// Create object
	obj := bucket.New("metadata")
	assert.T(t, obj != nil)
	obj.ContentType = "text/plain"
	obj.Data = []byte("metadata")
	obj.Meta["test"] = "something"
	err := obj.Store()
	assert.T(t, err == nil)

	// Fetch the object and check
	obj, err = bucket.Get("metadata")
	assert.T(t, err == nil)
	assert.T(t, obj != nil)
	assert.T(t, obj.Meta["test"] == "something")
	assert.T(t, obj.Meta["notest"] != "something")

	// Cleanup
	err = obj.Destroy()
	assert.T(t, err == nil)
}

func TestObjectIndexes(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	bucket := client.Bucket("client_test.go")
	assert.T(t, bucket != nil)

	// Create object
	obj := bucket.New("indexes")
	assert.T(t, obj != nil)
	obj.ContentType = "text/plain"
	obj.Data = []byte("indexes to keep")
	obj.Indexes["test_int"] = strconv.Itoa(123)
	obj.Indexes["and_bin"] = "blurb"
	err := obj.Store()
	assert.T(t, err == nil)
	// Create a second object
	obj2 := bucket.New("indexes2")
	assert.T(t, obj2 != nil)
	obj2.ContentType = "text/plain"
	obj2.Data = []byte("indexes to keep")
	obj2.Indexes["test_int"] = strconv.Itoa(124)
	obj2.Indexes["and_bin"] = "blurb"
	err = obj2.Store()
	assert.T(t, err == nil)

	// Fetch the object and check
	obj, err = bucket.Get("indexes")
	assert.T(t, err == nil)
	assert.T(t, obj != nil)
	assert.T(t, obj.Indexes["test_int"] == strconv.Itoa(123))
	assert.T(t, obj.Indexes["and_bin"] == "blurb")

	// Get a list of keys using the index queries
	keys, err := bucket.IndexQuery("test_int", strconv.Itoa(123))
	if err == nil {
		fmt.Printf("2i query returned : %v\n", keys)
	}
	assert.T(t, err == nil)
	assert.T(t, len(keys) == 1)
	assert.T(t, keys[0] == "indexes")
	// Get a list of keys using the index range query
	keys, err = bucket.IndexQueryRange("test_int", strconv.Itoa(120), strconv.Itoa(130))
	if err == nil {
		fmt.Printf("2i range query returned : %v\n", keys)
	}
	assert.T(t, err == nil)
	assert.T(t, len(keys) == 2)
	assert.T(t, keys[0] == "indexes" || keys[1] == "indexes")
	assert.T(t, keys[0] == "indexes2" || keys[1] == "indexes2")

	// Cleanup
	err = obj.Destroy()
	assert.T(t, err == nil)
	err = obj2.Destroy()
	assert.T(t, err == nil)
}

func TestBigObject(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	bucket := client.Bucket("client_test.go")
	assert.T(t, bucket != nil)

	// Create object
	size := 2 * 1024 * 1024 // 2Mb
	obj := bucket.New("bigobject")
	assert.T(t, obj != nil)
	obj.ContentType = "application/octet-stream"
	obj.Data = make([]byte, size)
	obj.Data[0] = 1
	obj.Data[size-1] = 0xFF
	assert.T(t, len(obj.Data) == size)
	err := obj.Store()
	assert.T(t, err == nil)

	// Get the object and verify its size and some contents
	obj, err = bucket.Get("bigobject")
	assert.T(t, err == nil)
	assert.T(t, obj != nil)
	assert.T(t, len(obj.Data) == size)
	assert.T(t, obj.Data[0] == 1)
	assert.T(t, obj.Data[1] == 0)
	assert.T(t, obj.Data[size-1] == 0xFF)

	// Cleanup
	err = obj.Destroy()
	assert.T(t, err == nil)
}

func TestRunMapReduce(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)
	bucket := client.Bucket("client_test.go")
	assert.T(t, bucket != nil)
	// Create object 1
	obj1 := bucket.New("mrobj1")
	assert.T(t, obj1 != nil)
	obj1.ContentType = "application/json"
	obj1.Data = []byte("{\"k\":\"v\"}")
	err := obj1.Store()
	assert.T(t, err == nil)
	// Create object 2
	obj2 := bucket.New("mrobj2")
	assert.T(t, obj2 != nil)
	obj2.ContentType = "application/json"
	obj2.Data = []byte("{\"k\":\"v2\"}")
	err = obj2.Store()
	assert.T(t, err == nil)
	// Link them
	obj1.LinkTo(obj2, "test")
	obj1.Store()

	//q := "{\"inputs\":[[\"client_test.go\",\"mrobj1\"]],\"query\":[{\"map\":{\"language\":\"javascript\",\"keep\":true,\"source\":\"function(v) { return [JSON.parse(v.values[0].data)]; }\"}}]}"
	q := "{\"inputs\":[[\"client_test.go\",\"mrobj1\"]],\"query\":[{\"map\":{\"language\":\"javascript\",\"keep\":true,\"source\":\"function(v) { return [v]; }\"}}]}"

	mr, err := client.RunMapReduce(q)
	assert.T(t, err == nil)
	assert.T(t, len(mr) == 1)
}

func TestMapReduce(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)
	bucket := client.Bucket("client_test.go")
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

	mr := client.MapReduce()
	mr.Add("client_test.go", "mrobj1")
	//mr.LinkBucket("bucketname", false)
	mr.Map("function(v) {return [v];}", true)
	res, err := mr.Run()
	assert.T(t, err == nil)
	assert.T(t, len(res) == 1)

	mr = client.MapReduce()
	mr.Add("client_test.go", "mrobj1")
	mr.MapObjectValue(true)
	res, err = mr.Run()
	assert.T(t, err == nil)
	assert.T(t, len(res) == 1)
}

type DocumentModel struct {
	FieldS    string
	FieldF    float64
	FieldB    bool
	RiakModel Model
}

func TestModel(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create a new "DocumentModel" and save it
	doc := DocumentModel{FieldS: "text", FieldF: 1.2, FieldB: true}
	err := client.New("testmodel.go", "TestModel", &doc)
	assert.T(t, err == nil)
	//err = client.Save(&doc)
	err = doc.RiakModel.Save()
	assert.T(t, err == nil)

	// Load it from Riak and check that the fields of the DocumentModel struct are set correctly
	doc2 := DocumentModel{}
	err = client.Load("testmodel.go", "TestModel", &doc2)
	assert.T(t, err == nil)
	assert.T(t, doc2.FieldS == doc.FieldS)
	assert.T(t, doc2.FieldF == doc.FieldF)
	assert.T(t, doc2.FieldB == doc.FieldB)

	// Get the key
	key, err := client.Key(&doc2)
	assert.T(t, err == nil)
	assert.T(t, key == "TestModel")
	// Set it differently
	err = client.SetKey("newTestModel", &doc2)
	assert.T(t, err == nil)
	// And test that it changed by getting it again
	key, err = client.Key(&doc2)
	assert.T(t, err == nil)
	assert.T(t, key == "newTestModel")

	// Cleanup
	bucket := client.Bucket("testmodel.go")
	err = bucket.Delete("TestModel")
	assert.T(t, err == nil)
}

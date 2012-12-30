package riak

import (
	"fmt"
	"github.com/bmizerany/assert"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func setupConnection(t *testing.T) (client *Client) {
	client = New("127.0.0.1:8087")
	err := client.Connect()
	assert.T(t, client != nil)
	assert.T(t, err == nil)

	return client
}

func setupConnections(t *testing.T, count int) (client *Client) {
	client = NewPool("127.0.0.1:8087", count)
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

	t.Logf("Riak server : %s with version %s\n", node, response)
}

func TestStoreObject(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	bucket, _ := client.Bucket("client_test.go")
	assert.T(t, bucket != nil)
	obj := bucket.New("abc", PW1, DW1)
	assert.T(t, obj != nil)
	obj.ContentType = "text/plain"
	obj.Data = []byte("some more data")
	err := obj.Store()
	assert.T(t, err == nil)
	assert.T(t, obj.Vclock != nil)
	/*
		if err != nil {
			t.Logf("Fetch resulted in %s\n", err.Error())
		}
		t.Logf("obj data : %s\n", string(obj.Data))
		t.Logf("obj key  : %s\n", obj.Key)
		t.Logf("vclock   : %s\n", obj.Vclock)
	*/
}

func TestGetAndDeleteObject(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	bucket, _ := client.Bucket("client_test.go")
	assert.T(t, bucket != nil)
	obj, err := bucket.Get("abc", R1, PR1)
	assert.T(t, err == nil)
	assert.T(t, obj != nil)
	/*
		t.Logf("obj key  : %s\n", obj.Key)
		t.Logf("vclock   : %s\n", obj.Vclock)
		if obj.conflict {
			for i, v := range obj.Siblings {
				t.Logf("%d - %s\n", i, v.Data)
			}
		} else {
			t.Logf("obj data : %s\n", string(obj.Data))
		}
	*/
	err = bucket.Delete("abc")
	assert.T(t, err == nil)
}

func TestObjectsWithSiblings(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	bucket, _ := client.Bucket("client_test.go")
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
	assert.T(t, len(obj.Siblings[0].Data) > 0)
	assert.T(t, len(obj.Siblings[1].Data) > 0)
	assert.T(t, string(obj.Siblings[0].Data) != string(obj.Siblings[1].Data))

	// Cleanup
	err = obj.Destroy()
	assert.T(t, err == nil)
	err = bucket.SetAllowMult(false)
	assert.T(t, err == nil)
	_ = bucket.Delete("def")
}

func TestObjectReload(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	bucket, _ := client.Bucket("client_test.go")
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

	bucket, _ := client.Bucket("client_test.go")
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

	bucket, _ := client.Bucket("client_test.go")
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

	bucket, _ := client.Bucket("client_test.go")
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

	bucket, _ := client.Bucket("client_test.go")
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
		t.Logf("2i query returned : %v\n", keys)
	} else {
		if err.Error() == "EOF" {
			fmt.Println("2i queries over protobuf is not supported, maybe running a pre 1.2 version of Riak - skipping 2i tests.")
			return
		} else if err.Error() == "{error,{indexes_not_supported,riak_kv_bitcask_backend}}" {
			fmt.Println("2i queries not support on bitcask backend - skipping 2i tests.")
			return
		} else if strings.Contains(err.Error(), "indexes_not_supported") {
			t.Logf("2i queries not supported - skipping 2i tests (%v).\n", err)
			return
		}
		t.Logf("2i query returned error : %v\n", err)
	}
	assert.T(t, err == nil)
	assert.T(t, len(keys) == 1)
	assert.T(t, keys[0] == "indexes")
	// Get a list of keys using the index range query
	keys, err = bucket.IndexQueryRange("test_int", strconv.Itoa(120), strconv.Itoa(130))
	if err == nil {
		t.Logf("2i range query returned : %v\n", keys)
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

	bucket, _ := client.Bucket("client_test.go")
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
	bucket, _ := client.Bucket("client_test.go")
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

	mr, err := client.RunMapReduce(q)
	assert.T(t, err == nil)
	assert.T(t, len(mr) == 1)
}

func TestMapReduce(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)
	bucket, _ := client.Bucket("client_test.go")
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
	FieldS string
	FieldF float64
	FieldB bool
	Model
}

func TestModel(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create a new "DocumentModel" and save it
	doc := DocumentModel{FieldS: "text", FieldF: 1.2, FieldB: true}
	err := client.New("testmodel.go", "TestModelKey", &doc)
	assert.T(t, err == nil)
	//err = client.Save(&doc)
	err = doc.Save()
	assert.T(t, err == nil)

	// Load it from Riak and check that the fields of the DocumentModel struct are set correctly
	doc2 := DocumentModel{}
	err = client.Load("testmodel.go", "TestModelKey", &doc2)
	assert.T(t, err == nil)
	assert.T(t, doc2.FieldS == doc.FieldS)
	assert.T(t, doc2.FieldF == doc.FieldF)
	assert.T(t, doc2.FieldB == doc.FieldB)

	// Get the key
	key, err := client.Key(&doc2)
	assert.T(t, err == nil)
	assert.T(t, key == "TestModelKey")
	// Set it differently
	err = client.SetKey("newTestModelKey", &doc2)
	assert.T(t, err == nil)
	// And test that it changed by getting it again
	key, err = client.Key(&doc2)
	assert.T(t, err == nil)
	assert.T(t, key == "newTestModelKey")

	// Cleanup
	bucket, _ := client.Bucket("testmodel.go")
	err = bucket.Delete("TestModelKey")
	assert.T(t, err == nil)
}

type DocumentModelWithLinks struct {
	FieldS string
	ALink  One "tag_as_parent"
	BLink  One // Will automatically use own name as a tag when linking
	Model
}

func TestModelWithLinks(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create a new "DocumentModel" to use as a parent and save it
	parent := DocumentModel{FieldS: "text", FieldF: 1.2, FieldB: true}
	err := client.New("testmodel.go", "TestModelKey", &parent)
	assert.T(t, err == nil)
	//err = client.Save(&doc)
	err = parent.Save()
	assert.T(t, err == nil)

	// Create a new DocumentModelWithLinks and save it, adding a link to the parent
	doc := DocumentModelWithLinks{FieldS: "textinlinked", ALink: One{model: &parent}}
	doc.BLink.Set(&parent) // testing One.Set while we're at it
	err = client.New("testmodellinks.go", "TestModelKey", &doc)
	assert.T(t, err == nil)
	//err = client.Save(&doc)
	err = doc.Save()
	assert.T(t, err == nil)

	// Load it from Riak and check that the fields of the struct are set correctly, including the link to the parent
	doc2 := DocumentModelWithLinks{}
	err = client.Load("testmodellinks.go", "TestModelKey", &doc2)
	assert.T(t, err == nil)
	assert.T(t, doc2.FieldS == doc.FieldS)
	assert.T(t, doc2.ALink.model == nil) // Related documents are not loaded automatically, only the link is populated
	assert.T(t, doc2.ALink.link.Tag == "tag_as_parent")
	assert.T(t, doc2.BLink.link.Tag == "BLink")
	t.Logf("Testing DocumentModelWithLinks - One - %v - %v\n", doc2.ALink.model, doc2.ALink.link)
	t.Logf("Testing DocumentModelWithLinks - One - %v - %v\n", doc2.BLink.model, doc2.BLink.link)

	// Load the parent from the link
	parent2 := DocumentModel{}
	err = doc2.ALink.Get(&parent2)
	assert.T(t, err == nil)
	assert.T(t, parent.FieldS == parent2.FieldS)
	assert.T(t, parent.FieldF == parent2.FieldF)
	assert.T(t, parent.FieldB == parent2.FieldB)
	assert.T(t, parent.Key() == parent2.Key())

	// Cleanup
	bucket, _ := client.Bucket("testmodel.go")
	err = bucket.Delete("TestModelKey")
	assert.T(t, err == nil)
	bucket, _ = client.Bucket("testmodellinks.go")
	err = bucket.Delete("TestModelKey")
	assert.T(t, err == nil)
}

/*
Test multiple simultaneous connections. This runs a long running MapReduce in a
goroutine and a quick Riak operation in another goroutine that is started 100ms
later. Since the operations are not blocking each other the second operation
should finish before the first (long running) operation.
*/
func TestRunConnectionPool(t *testing.T) {
	// Skip this test if test.short is set
	if testing.Short() {
		t.Log("Skipping TestRunConnectionPool")
		return
	}
	// Preparations
	fmt.Printf("Testing multiple simultaneous connections .")
	client := setupConnections(t, 2)
	assert.T(t, client != nil)
	assert.T(t, client.conn_count == 2)
	bucket, _ := client.Bucket("client_test.go")
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

	receiver := make(chan int)
	// MR job with (very dirty) sleep of 2 seconds, send "1" to the channel afterwards
	go func() {
		q := `{"inputs":[["client_test.go","mrobj1"]],"query":[{"map":{"language":"javascript","keep":true,"source":"function(v) { var start = new Date().getTime(); while (new Date().getTime() < start + 2000); return [v]; }"}}]}`
		mr, err := client.RunMapReduce(q)
		assert.T(t, err == nil)
		assert.T(t, len(mr) == 1)
		fmt.Printf("1")
		os.Stdout.Sync()
		receiver <- 1
	}()
	// Sleep 100 milliseconds and then fetch a single value, send "2" after this fetch
	<-time.After(100 * time.Millisecond)
	go func() {
		obj, err := bucket.Get("mrobj1")
		assert.T(t, err == nil)
		assert.T(t, obj != nil)
		fmt.Printf("2")
		os.Stdout.Sync()
		receiver <- 2
	}()
	t1 := <-receiver
	t2 := <-receiver
	// Now "2" should be before "1" if multiple connections were really used (and Riak answered on time...)
	assert.T(t, t2 < t1)

	// Do some more queries, just to make sure the connections get properly re-used
	for i := 0; i < 50; i++ {
		obj, err := bucket.Get("mrobj1", R1)
		assert.T(t, err == nil)
		assert.T(t, obj != nil)
		fmt.Printf(".")
		os.Stdout.Sync()
	}
	fmt.Printf("\n")
}

type FriendLinks struct {
	Friends Many "friend"
	Model
}

func TestModelWithManyLinks(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create two new "DocumentModel"s to use as friends and save it
	f1 := DocumentModel{FieldS: "friend1", FieldF: 1.0, FieldB: true}
	err := client.New("testmodel.go", "f1", &f1)
	assert.T(t, err == nil)
	err = f1.Save()
	assert.T(t, err == nil)
	f2 := DocumentModel{FieldS: "friend2", FieldF: 2.0, FieldB: true}
	err = client.New("testmodel.go", "f2", &f2)
	assert.T(t, err == nil)
	err = f2.Save()
	assert.T(t, err == nil)

	// Create a new "FriendLinks" to and save it
	doc := FriendLinks{Friends: Many{One{model: &f1}}}
	// Testing Many.Add while we're at it.
	doc.Friends.Add(&f2)
	err = client.New("testmodel.go", "TestMany", &doc)
	assert.T(t, err == nil)
	err = doc.Save()

	// Now load a new document and verify it has two links
	var doc2 FriendLinks
	err = client.Load("testmodel.go", "TestMany", &doc2)
	assert.T(t, err == nil)
	assert.T(t, len(doc2.Friends) == 2)
	for i, v := range doc2.Friends {
		var f DocumentModel
		err = v.Get(&f)
		assert.T(t, err == nil)
		t.Logf("TestingModelWithManyLinks - %v - %v - %v\n", i, v, f)
	}
}

func TestBrokenConnection(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	/* 
		Abuse direct access to underlying TCP connection, send something Riak
		does not accept so it closes the connection on that end. This will result
		in a few writes still succeeding, receiving an EOF and finally the write
		failing with a broken pipe message.
	*/
	t.Logf("Testing a broken connection to Riak ...")
	msg := []byte{250, 0, 0, 1, 250, 250}
	c, cerr := client.conn.Write(msg)
	t.Logf("%v bytes written - err=%v\n", c, cerr)

	bucket, cerr := client.Bucket("client_test.go")
	t.Logf("1: %v\n", cerr)
	bucket, cerr = client.Bucket("client_test.go")
	t.Logf("2: %v\n", cerr)
	bucket, cerr = client.Bucket("client_test.go")
	t.Logf("3: %v\n", cerr)
	bucket, cerr = client.Bucket("client_test.go")
	t.Logf("4: %v\n", cerr)
	bucket, cerr = client.Bucket("client_test.go")
	t.Logf("5: %v\n", cerr)
	assert.T(t, bucket != nil)
	obj := bucket.New("abcdefghijk", PW1, DW1)
	assert.T(t, obj != nil)
	obj.ContentType = "text/plain"
	obj.Data = []byte("some more data")
	err := obj.Store()
	assert.T(t, err == nil)
	assert.T(t, obj.Vclock != nil)
}

/*
Example resolve function for DocumentModel. This selects the longest FieldS
from the siblings, the largest FieldF and sets FieldB to true if any of the
siblings have it set to true.
*/
func (d *DocumentModel) Resolve(count int) (err error) {
	//fmt.Printf("Resolving DocumentModel = %v, with count = %v\n", d, count)
	siblings := make([]DocumentModel, count, count)
	err = d.GetSiblings(siblings)
	if err != nil {
		return err
	}
	//for i, s := range siblings {
	//	fmt.Printf("DocumentModel %v - %v\n", i, s)
	//}
	d.FieldB = false
	for _, s := range siblings {
		if len(s.FieldS) > len(d.FieldS) {
			d.FieldS = s.FieldS
		}
		if s.FieldF > d.FieldF {
			d.FieldF = s.FieldF
		}
		if s.FieldB {
			d.FieldB = true
		}
	}
	return
}

func TestConflictingModel(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create a bucket where siblings are allowed
	bucket, err := client.Bucket("testconflict.go")
	assert.T(t, err == nil)
	err = bucket.SetAllowMult(true)
	assert.T(t, err == nil)

	// Delete earlier work ...
	err = bucket.Delete("TestModelKey")
	assert.T(t, err == nil)

	// Create a new "DocumentModel" and save it
	doc := DocumentModel{FieldS: "text", FieldF: 1.2, FieldB: true}
	err = client.New("testconflict.go", "TestModelKey", &doc)
	assert.T(t, err == nil)
	err = doc.Save()
	assert.T(t, err == nil)

	// Create the same again (with the same key)
	doc2 := DocumentModel{FieldS: "longer_text", FieldF: 1.4, FieldB: false}
	err = client.New("testconflict.go", "TestModelKey", &doc2)
	assert.T(t, err == nil)
	err = doc2.Save()
	assert.T(t, err == nil)

	// Now load it from Riak to test conflicts
	doc3 := DocumentModel{}
	err = client.Load("testconflict.go", "TestModelKey", &doc3)
	t.Logf("Loading model - %v\n", err)
	t.Logf("DocumentModel = %v\n", doc3)
	assert.T(t, err == nil)
	assert.T(t, doc3.FieldS == doc2.FieldS) // doc2 has longer FieldS
	assert.T(t, doc3.FieldF == doc2.FieldF) // doc2 has larger FieldF
	assert.T(t, doc3.FieldB == doc.FieldB)  // doc has FieldB set to true

	// Cleanup
	err = bucket.Delete("TestModelKey")
	assert.T(t, err == nil)
}

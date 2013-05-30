package riak

import (
	"fmt"
	"github.com/bmizerany/assert"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	riakhost = "127.0.0.1:8087"
)

func setupConnection(t *testing.T) (client *Client) {
	client = New(riakhost)
	err := client.Connect()
	assert.T(t, client != nil)
	assert.T(t, err == nil)

	return client
}

func setupConnections(t *testing.T, count int) (client *Client) {
	client = NewPool(riakhost, count)
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

func TestGetServerVersionAndId(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	node, response, err := client.ServerVersion()
	assert.T(t, err == nil)
	assert.T(t, node != "")
	assert.T(t, response != "")

	t.Logf("Riak server : %s with version %s\n", node, response)

	// Test the same with the default client
	err = ConnectClient(riakhost)
	assert.T(t, err == nil)
	n2, r2, err := ServerVersion()
	assert.T(t, err == nil)
	assert.T(t, node == n2)
	assert.T(t, response == r2)

	// Test Id functions
	err = client.SetId("MYID123")
	assert.T(t, err == nil)
	err = SetId("MYID456")
	assert.T(t, err == nil)

	id, err := client.Id()
	assert.T(t, err == nil)
	id2, err := Id()
	assert.T(t, err == nil)
	assert.T(t, id != id2)
}

func TestStoreObject(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	bucket, _ := client.Bucket("client_test.go")
	assert.T(t, bucket != nil)
	obj := bucket.New("abc", PW1, DW1, W1)
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
	err = bucket.Delete("abc", R1, PR1, W1, DW1, PW1)
	assert.T(t, err == nil)
}

func TestObjectsWithSiblings(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	bucket, _ := client.Bucket("client_test.go")
	assert.T(t, bucket != nil)
	err := bucket.SetAllowMult(true)
	assert.T(t, err == nil)

	// Create a target so links can also be tested, include some riak store/read options
	target, err := client.NewObjectIn("client_test.go", "targetkey", R1, PR1, W1, DW1, PW1)
	assert.T(t, err == nil)
	target.ContentType = "text/plain"
	target.Data = []byte("data")
	err = target.Store()
	assert.T(t, err == nil)

	// Create an object with two siblings
	_ = bucket.Delete("def")
	obj := bucket.New("def")
	obj.ContentType = "text/plain"
	obj.Data = []byte("data 1")
	obj.Meta["mymeta"] = "meta1"
	obj.Indexes["myindex_bin"] = "index1"
	obj.LinkTo(target, "mytag")

	err = obj.Store()
	assert.T(t, err == nil)

	obj = bucket.New("def")
	obj.ContentType = "text/plain"
	obj.Data = []byte("data 2")
	obj.Meta["mymeta"] = "meta2"
	obj.Indexes["myindex_bin"] = "index2"
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
	assert.T(t, obj.Siblings[0].Meta["mymeta"] != obj.Siblings[1].Meta["mymeta"])
	assert.T(t, obj.Siblings[0].Indexes["myindex_bin"] != obj.Siblings[1].Indexes["myindex_bin"])
	assert.T(t, len(obj.Siblings[0].Links) != len(obj.Siblings[1].Links))

	// Cleanup
	err = obj.Destroy()
	assert.T(t, err == nil)
	err = bucket.SetAllowMult(false)
	assert.T(t, err == nil)
	_ = bucket.Delete("def")

	err = target.Destroy()
	assert.T(t, err == nil)
}

func TestObjectReload(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	bucket, _ := client.Bucket("client_test.go")
	assert.T(t, bucket != nil)

	// Create an object
	obj := bucket.NewObject("ghi", R1, PR1)
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
	e, err := bucket.Exists("alskgqwioetuioweqadfh", R1, PR1)
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
		receiver <- 1
	}()
	// Sleep 100 milliseconds and then fetch a single value, send "2" after this fetch
	<-time.After(100 * time.Millisecond)
	go func() {
		obj, err := bucket.Get("mrobj1")
		assert.T(t, err == nil)
		assert.T(t, obj != nil)
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
	}
}

func TestBucketProperties(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)
	bucket, _ := client.Bucket("client_test_props.go")
	assert.T(t, bucket != nil)

	err := bucket.SetNVal(3)
	assert.T(t, err == nil)
	err = bucket.SetAllowMult(true)
	assert.T(t, err == nil)

	bucket, _ = client.Bucket("client_test_props.go")
	assert.T(t, bucket != nil)

	assert.T(t, bucket.NVal() == 3)
	assert.T(t, bucket.AllowMult() == true)
}

func TestBadConnection(t *testing.T) {
	// Tests for a connection that cannot be established, is this caught correctly?
	err := ConnectClient("127.0.0.1:8088") // connecting to port 8088 should not work ... (and return fast)
	assert.T(t, err != nil)

	node, version, err := ServerVersion()
	assert.T(t, err != nil)
	assert.T(t, node == "")
	assert.T(t, version == "")

	// Same for a dedicated, rather then the shared client
	c := NewClient("1.does/not/resolve::at_all")
	assert.T(t, c != nil)
	err = c.Connect()
	assert.T(t, err != nil) // cannot be resolved
	// Bad number of connections
	c = NewClientPool(riakhost, -5)
	assert.T(t, c != nil)
	err = c.Connect()
	assert.T(t, err != nil) // cannot have negative number of connections

	// Check for no panic on double Close()
	c.Close()
	c.Close()
}

func TestDefaultClientNotInitialized(t *testing.T) {
	// Check what happens with some calls if the defaultClient is not initialized
	// Basically this just makes sure there is no panic ...
	defaultClient.Close()
	defaultClient = nil

	node, version, err := ServerVersion()
	assert.T(t, err != nil)
	assert.T(t, node == "")
	assert.T(t, version == "")

	_, err = RunMapReduce("no query")
	assert.T(t, err != nil)

	_, err = GetFrom("bucketname", "key")
	assert.T(t, err != nil)

	_, err = Id()
	assert.T(t, err != nil)

	err = LoadModel("key", nil)
	assert.T(t, err != nil)

}

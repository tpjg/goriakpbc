package riak

import (
	"fmt"
	"github.com/bmizerany/assert"

	"testing"
)

func TestSearch(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)
	_, version, err := client.ServerVersion()
	assert.T(t, err == nil)

	major, minor := parseVersion(version)

	if (major < 1) || (major == 1 && minor < 4) {
		t.Log("running a pre 1.4 version of riak - skipping search tests.")
		return
	}

	bucket, err := client.NewBucket("default")
	assert.T(t, err == nil)
	err = bucket.SetSearch(true)
	assert.T(t, err == nil)

	for i, _ := range []int{0, 1, 2} {
		obj := bucket.New(fmt.Sprintf("doc-%v", i), PW1, DW1)
		assert.T(t, obj != nil)
		obj.ContentType = "application/json"
		obj.Data = []byte("{\"a\":\"b\", \"c\":1}")
		err = obj.Store()
		assert.T(t, err == nil)
		assert.T(t, obj.Vclock != nil)
	}

	s := &Search{Q: "b", Index: "search_test.go", Df: "a", Rows: 10, Fields: []string{"a", "c"}}
	docs, _, _, err := client.Search(s)
	assert.T(t, (err == nil) || (err.Error() == "No index <<\"search_test.go\">> found."))

	// skipping these for now until there's a way to install
	// a schema via code, or ensure a test env. has a schema:

	//assert.T(t, err == nil)
	//assert.T(t, len(docs) == 3)

	for _, doc := range docs {
		a, ok := doc["a"]
		assert.T(t, ok)
		assert.T(t, string(a) == "b")
		c, ok := doc["c"]
		assert.T(t, ok)
		assert.T(t, string(c) == "1")

	}
}

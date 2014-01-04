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

	bucket, err := client.NewBucket("search_test.go")
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
	docs, err := client.Search(s)
	assert.T(t, err == nil)
	assert.T(t, len(docs) == 3)

	for _, doc := range docs {
		a, ok := doc["a"]
		assert.T(t, ok)
		assert.T(t, a == "b")
		c, ok := doc["c"]
		assert.T(t, ok)
		assert.T(t, c == "1")

	}
}

package riak

import (
	"strconv"
	"strings"
	"testing"

	"github.com/bmizerany/assert"
)

func parseVersion(version string) (major, minor int) {
	var tmp []string
	tmp = strings.Split(version, ".")
	if len(tmp) > 0 {
		major, _ = strconv.Atoi(tmp[0])
	}
	if len(tmp) > 1 {
		minor, _ = strconv.Atoi(tmp[1])
	}

	return
}

func TestBucket(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	_, version, err := client.ServerVersion()
	assert.T(t, err == nil)

	major, minor := parseVersion(version)

	if (major < 1) || (major == 1 && minor < 4) {
		t.Log("running a pre 1.4 version of riak - skipping last_write_wins bucket tests.")
	}

	// Find bucket and set properties
	bucket, err := client.NewBucket("bucket_test.go")
	assert.T(t, err == nil)
	err = bucket.SetNVal(2)
	assert.T(t, err == nil)
	err = bucket.SetAllowMult(true)
	assert.T(t, err == nil)
	err = bucket.SetLastWriteWins(false)
	assert.T(t, err == nil)

	err = bucket.SetSearchIndex("search_test.go")
	assert.T(t, (err == nil) || (err.Error()[0:42] == "Invalid bucket properties: [{search_index,"))

	// Read and verify properties
	bucket2, err := client.NewBucket("bucket_test.go")
	assert.T(t, err == nil)
	assert.T(t, bucket2.NVal() == 2)
	assert.T(t, bucket2.AllowMult() == true)

	if (major > 1) || (major == 1 && minor >= 4) {
		assert.T(t, bucket2.LastWriteWins() == false)
		assert.T(t, bucket2.SearchIndex() == "")
	}

	// Set alternate properties
	err = bucket.SetNVal(3)
	assert.T(t, err == nil)
	err = bucket.SetAllowMult(false)
	assert.T(t, err == nil)
	err = bucket.SetLastWriteWins(true)
	assert.T(t, err == nil)

	// Read and verify properties
	bucket3, err := client.NewBucket("bucket_test.go")
	assert.T(t, err == nil)
	assert.T(t, bucket3.NVal() == 3)
	assert.T(t, bucket3.AllowMult() == false)

	if (major > 1) || (major == 1 && minor >= 4) {
		assert.T(t, bucket3.LastWriteWins() == true)
	}
}

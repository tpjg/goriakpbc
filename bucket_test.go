package riak

import (
	"github.com/bmizerany/assert"
	"strconv"
	"strings"
	"testing"
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

	// Read and verify properties
	bucket2, err := client.NewBucket("bucket_test.go")
	assert.T(t, err == nil)
	assert.T(t, bucket2.NVal() == 2)
	assert.T(t, bucket2.AllowMult() == true)

	if (major > 1) || (major == 1 && minor >= 4) {
		assert.T(t, bucket2.LastWriteWins() == false)
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

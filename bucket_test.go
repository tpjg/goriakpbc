package riak

import (
	"github.com/bmizerany/assert"
	"testing"
)

func TestBucket(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

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
	assert.T(t, bucket2.LastWriteWins() == false)

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
	assert.T(t, bucket3.LastWriteWins() == true)
}

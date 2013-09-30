package riak

import (
	"github.com/bmizerany/assert"
	"strconv"
	"strings"
	"testing"
)

func TestCounter(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	_, version, err := client.ServerVersion()
	assert.T(t, err == nil)

	major, minor := parseVersion(version)

	if (major < 1) || (major == 1 && minor < 4) {
		t.Log("running a pre 1.4 version of riak - skipping counter tests.")
		return
	}

	// Find bucket and set properties
	bucket, err := client.NewBucket("counter_test.go")
	assert.T(t, err == nil)
	err = bucket.SetAllowMult(true)
	assert.T(t, err == nil)

	c1, err := bucket.GetCounter("counter_1")
	assert.T(t, err == nil)
	base := c1.Value

	// Increment and refresh
	err = c1.IncrementAndReload(5)
	assert.T(t, err == nil)
	assert.T(t, c1.Value == (base+5))

	// Increment without refresh
	err = c1.Increment(5)
	assert.T(t, err == nil)
	assert.T(t, c1.Value == (base+5))

	// Reload
	err = c1.Reload()
	assert.T(t, err == nil)
	assert.T(t, c1.Value == (base+10))

	// Decrement multiple times
	err = c1.Decrement(2)
	assert.T(t, err == nil)
	err = c1.Decrement(2)
	assert.T(t, err == nil)

	// Decrement and refresh
	err = c1.DecrementAndReload(3)
	assert.T(t, err == nil)
	assert.T(t, c1.Value == (base+3))

	c2, err := bucket.GetCounter("counter_2")
	assert.T(t, err == nil)
	base = c2.Value

	// Increment another counter
	err = c2.Increment(3)
	assert.T(t, err == nil)
	err = c2.Increment(5)
	assert.T(t, err == nil)

	// Reload the counter
	err = c2.Reload()
	assert.T(t, err == nil)
	assert.T(t, c2.Value == (base+8))

	// Get directly from bucket
	c3, err := client.GetCounterFrom("counter_test.go", "counter_2")
	assert.T(t, err == nil)
	assert.T(t, c3.Value == (base+8))

	// Destroy the first counter
	err = c1.Destroy()
	assert.T(t, err == nil)
	err = c1.Reload()
	assert.T(t, c1.Value == 0)

	// Write a new value back to the first counter
	err = c1.Increment(5)
	assert.T(t, err == nil)
	err = c1.Reload()
	assert.T(t, c1.Value == 5)
}

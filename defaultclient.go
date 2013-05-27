package riak

import (
	"errors"
)

var (
	NoDefaultClientConnection = errors.New("No (default) client connection")
)

// Introduce a default client
var defaultClient *Client = nil

// Create the default client with a single connection to Riak.
func ConnectClient(addr string) (err error) {
	if defaultClient != nil {
		defaultClient.Close()
	}
	defaultClient = &Client{addr: addr, connected: false, readTimeout: 1e8, writeTimeout: 1e8, conn_count: 1}
	return defaultClient.Connect()
}

// Create the default client, using a pool of connections. This is the recommended
// method to connect to Riak in an application. A single client instance can be
// used by multiple threads/goroutines and will not block operations if there are
// enough connections in the pool.
// NOTE: If an application needs connections to different Riak clusters it can use
// riak.NewClientPool or riak.NewClient.
func ConnectClientPool(addr string, count int) (err error) {
	if defaultClient != nil {
		defaultClient.Close()
	}
	defaultClient = &Client{addr: addr, connected: false, readTimeout: 1e8, writeTimeout: 1e8, conn_count: count}
	return defaultClient.Connect()
}

// Ping the server
func Ping() (err error) {
	if defaultClient == nil {
		return NoDefaultClientConnection
	}
	return defaultClient.Ping()
}

// Get the client Id
func Id() (id string, err error) {
	if defaultClient == nil {
		err = NoDefaultClientConnection
		return
	}
	return defaultClient.Id()
}

// Set the client Id
func SetId(id string) (err error) {
	if defaultClient == nil {
		return NoDefaultClientConnection
	}
	return defaultClient.SetId(id)
}

// Get the server version for the default client
func ServerVersion() (node string, version string, err error) {
	if defaultClient == nil {
		err = NoDefaultClientConnection
		return
	}
	return defaultClient.ServerVersion()
}

// Return a new bucket using the client connection
func NewBucket(name string) (*Bucket, error) {
	if defaultClient == nil {
		return nil, NoDefaultClientConnection
	}
	return defaultClient.NewBucket(name)
}

// Create a MapReduce object that can be used to build a MR query
func NewMapReduce() *MapReduce {
	if defaultClient == nil {
		return nil
	}
	return defaultClient.MapReduce()
}

// Run a MapReduce query directly
func RunMapReduce(query string) (resp [][]byte, err error) {
	if defaultClient == nil {
		err = NoDefaultClientConnection
		return
	}
	return defaultClient.RunMapReduce(query)
}

// Get directly from a bucket, without creating a bucket first
func GetFrom(bucketname string, key string, options ...map[string]uint32) (obj *RObject, err error) {
	if defaultClient == nil {
		err = NoDefaultClientConnection
		return
	}
	return defaultClient.GetFrom(bucketname, key, options...)
}

// Delete directly from a bucket, without creating a bucket object first
func DeleteFrom(bucketname string, key string, options ...map[string]uint32) (err error) {
	if defaultClient == nil {
		return NoDefaultClientConnection
	}
	return defaultClient.DeleteFrom(bucketname, key, options...)
}

// Create a new RObject in a bucket directly, without creating a bucket object first
func NewObjectIn(bucketname string, key string, options ...map[string]uint32) (*RObject, error) {
	if defaultClient == nil {
		return nil, NoDefaultClientConnection
	}
	return defaultClient.NewObjectIn(bucketname, key, options...)
}

// Test if an object exists in a bucket directly, without creating a bucket object first
func ExistsIn(bucketname string, key string, options ...map[string]uint32) (exists bool, err error) {
	if defaultClient == nil {
		return false, NoDefaultClientConnection
	}
	return defaultClient.ExistsIn(bucketname, key, options...)
}

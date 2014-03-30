riak (goriakpbc) [![Build Status](https://travis-ci.org/tpjg/goriakpbc.png?branch=master)](https://travis-ci.org/tpjg/goriakpbc)
=======

Package riak is a riak-client, inspired by the Ruby riak-client gem and the riakpbc go package from mrb (github.com/mrb/riakpbc).
It implements a connection to Riak using Protocol Buffers.

A simple program using goriakpbc:

```go
package main

import (
	"fmt"
	"github.com/tpjg/goriakpbc"
)

func main() {
	err := riak.ConnectClient("127.0.0.1:8087")
	if err != nil {
		fmt.Println("Cannot connect, is Riak running?")
		return
	}

	bucket, _ := riak.NewBucket("tstriak")
	obj := bucket.NewObject("tstobj")
	obj.ContentType = "application/json"
	obj.Data = []byte("{'field':'value'}")
	obj.Store()

	fmt.Printf("Stored an object in Riak, vclock = %v\n", obj.Vclock)

	riak.Close()
}
```

Parts of the library are specifically designed to facilitate projects that use both Ruby and Go. See the "Document Models" below.
To install run `go get github.com/tpjg/goriakpbc` and use import as in the example above. If the Document Models (ORM) features are not needed simply run `rm $GOPATH/src/github.com/tpjg/goriakpbc/model*.go` after `go get`.

### Documentation

More documentation is available in the Wiki (https://github.com/tpjg/goriakpbc/wiki), below are some examples of the features implemented in this library. Full API documentation (automatically generated including protobuf definitions) is available at http://go.pkgdoc.org/github.com/tpjg/goriakpbc or through `go doc`.

### Secondary indexes (2i)

WARNING: The API has slightly changed and this may break existing applications. The "Indexes" are changed to store multiple values now. Please see https://github.com/tpjg/goriakpbc/issues/71 for some history and a rationale for choosing to break the API in a very clear and predictable way.

Secondary indexes are supported and can be queried for equality using IndexQuery and for a range using IndexQueryRange. Indexes must be added as strings, even when adding a "_int" index. See the example below, taken from riak_test.go:

```go

obj, _ := bucket.NewObject("some_key")
obj.ContentType = "text/plain"
obj.Data = []byte("testing indexes")
obj.Indexes["test_int"] = []string{strconv.Itoa(123)}
err := obj.Store()
...
keys, err := bucket.IndexQuery("test_int", strconv.Itoa(123))
...
keys, err = bucket.IndexQueryRange("test_int", strconv.Itoa(120), strconv.Itoa(130))

```

Using Riak 1.4 and greater, you can pagination through keys in secondary indexes:

```go
keys, continuation, err := bucket.IndexQueryPage("test_int", strconv.Itoa(123), 10, "")
...
keys, continuation, err = bucket.IndexQueryPage("test_int", strconv.Itoa(123), 10, continuation)
...
keys, continuation, err = bucket.IndexQueryRangePage("test_int", strconv.Itoa(120), strconv.Itoa(130), 10, "")
...
keys, continuation, err = bucket.IndexQueryRangePage("test_int", strconv.Itoa(120), strconv.Itoa(130), 10, continuation)

```

### Map Reduce

There is a function to run a MapReduce directly:
```go
func (c *Client) RunMapReduce(query string) (resp [][]byte, err error)
```

And MapReduce queries can be build similar to how the MapReduce class from the Ruby riak-client works:
```go
mr := riak.NewMapReduce()
mr.Add("bucket", "key")
mr.LinkBucket("otherbucket", false)
mr.Map("function(v) {return [JSON.parse(v.values[0].data)];}", true)
res, err := mr.Run()
```
Map functions using Erlang instead of Javascript must be added using "MapErlang" instead of "Map" and there is a predefined function "MapObjectValue" that uses the riak_kv_mapreduce module's map_object_value function. Reduce functions can be added similarly using "Reduce" and "ReduceErlang". For efficiently counting the number of objects the "ReduceObjectCount" can be used that uses the riak_kv_mapreduce module's reduce_count_inputs function.

If the backend supports secondary indexes a whole bucket can be added as input to a MapReduce query. Alternatively range queries and single key queries on 2i are also supported:
```go
mr := riak.NewMapReduce()
mr.AddBucket("bucket")
// mr.AddBucketRange("bucket", "a", "k")
// mr.AddIndexRange("bucket", "key", "a", "k")
// mr.AddIndex("bucket", "key", "somekey1234")
mr.MapObjectValue(true)
res, err := mr.Run()
```

### Counters

Example:

```go
// Get counter from bucket, loads value
c = bucket.GetCounter("key")

// Get counter without existing bucket instance, loads value
c = cli.GetCounterFrom("bucket", "key")

c.Value                  // 0
c.Reload
c.Increment(1)           // 1 on server, 0 in strucct
c.IncrementAndReload(1)  // 2 on server and in struct
c.Decrement(1)           // 1 on server, 2 in struct
c.DecrementAndReload(1)  // 0 on server and in struct
```

The "AndReload" methods exist to take advantage of an option in update that returns the current value, thus saving a req/resp cycle.

### Search

Example:

```go

bucket := client.NewBucket("rocket_launchers")
bucket.SetSearch(true)

if docs, err := client.Search(&Search{Q: "quake", Index: "rocket_launchers"}); err == nil {
    for i, doc := range docs {
        fmt.Printf("Rocket launcher number: %s has key: %s\n", i, doc["key"])
    }
}

```

The `Search` struct has fields for row count, start, sorting, etc.  See
http://godoc.org/github.com/tpjg/goriakpbc#Search for all of them.


### Riak Document Models

Document Models, commonly referred to as ORM (Object-Relational Mapping) in other database drivers, maps Go structs to an object in Riak and supports links between objects. This is done by parsing the JSON data from an object in Riak and mapping it to a struct's fields.

The library allows for easy integration of a Go application into a project that also uses Ruby (on Rails) with the "ripple" gem (https://github.com/basho/ripple). To enable easy integration with Ruby/ripple projects the struct "tag" feature of Go is used to get around the naming convention differences between Go and Ruby (Uppercase starting letter required for export versus Uppercase being constants and typically CamelCase versus snake_case). Also it stores the model/struct name as _type in Riak just like ripple does.

For example the following Ruby/Ripple class:
```ruby
    class Device
      include Ripple::Document
      property :ip, String
      property :description, String
      property :download_enabled, Boolean
    end
```
can be mapped to the following Go struct:
```go
    type Device struct {
        DownloadEnabled  bool    `riak:"download_enabled"`
        Ip               string  `riak:"ip"`
        Description      string  `riak:"description"`
        riak.Model       `riak:"devices"`
    }
```
Note that it is required to have an (anonymous) riak.Model field. If the riak.Model field is an anonymous field this has the benefit that the functions like "Save" or "SaveAs" can be called directly as in the example below.

To get an instantiated struct from Riak would then require only a call to the riak.Client "Load" function, and to store it call "Save" or "SaveAs":
```go
err := riak.ConnectClient("127.0.0.1:8087")
var dev Device 
err = riak.LoadModel("abcdefghijklm", &dev)
dev.Description = "something else"
err = dev.SaveAs("newkey")
```

### Large object support

Storing really large values (over 10Mb) in Riak is not efficient and is not recommended. If you care about worst case latencies it is recommended to keep values under 100Kb (see http://lists.basho.com/pipermail/riak-users_lists.basho.com/2014-March/014938.html). Changing small parts of a large value is also not efficient because the complete value must be PUT on every change (e.g. when storing files that grow over time like daily log files).

For storing these a value can be split into multiple segments, goriakpbc provides an RFile object for this. This object abstracts the chunking of data and exposes the familiar io.Reader, io.Writer and io.Seeker interfaces similar to os.File.

```go
src, err := os.Open("file.mp4")
// Create a file in Riak and split the data into 100Kb chunks
dst, err := riak.CreateFile("bucket", "key", "video/mp4", 102400)

size, err := io.Copy(dst, src)
```

Some meta-data, like the segment size and number of segments about the "RFile" will be stored in the value at "key" using the Meta tag feature of Riak, the actual data in segments with keys named "key-00000", "key-000001", et-cetera.

### Licensing

goriakpbc is distributed under the Apache license, see `LICENSE.txt` file or http://www.apache.org/licenses/LICENSE-2.0 for details. The model_json_*.go files are a copy from the original Go distribution with minor changes and are governed by a BSD-style license, see `LICENSE.go.txt`.

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

Secondary indexes are supported and can be queried for equality using IndexQuery and for a range using IndexQueryRange. Indexes must be added as strings, even when adding a "_int" index. See the example below, taken from riak_test.go:

```go

obj, _ := bucket.NewObject("some_key")
obj.ContentType = "text/plain"
obj.Data = []byte("testing indexes")
obj.Indexes["test_int"] = strconv.Itoa(123)
err := obj.Store()
...
keys, err := bucket.IndexQuery("test_int", strconv.Itoa(123))
...
keys, err = bucket.IndexQueryRange("test_int", strconv.Itoa(120), strconv.Itoa(130))

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
Map functions using Erlang instead of Javascript must be added using "MapErlang" instead of "Map" and there is a predefined function "MapObjectValue" that uses the riak_kv_mapreduce module's map_object_value function.

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

### Licensing

goriakpbc is distributed under the Apache license, see `LICENSE.txt` file or http://www.apache.org/licenses/LICENSE-2.0 for details. The model_json_*.go files are a copy from the original Go distribution with minor changes and are governed by a BSD-style license, see `LICENSE.go.txt`.

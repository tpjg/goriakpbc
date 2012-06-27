riak (goriakpbc)
=======

Package riak is a riak-client, inspired by the Ruby riak-client gem and the riakpbc go package from mrb (github.com/mrb/riakpbc).
It implements a connection to Riak using protobuf.

```go
package main

import (
	"fmt"
	"github.com/tpjg/goriakpbc"
)

func main() {
	client := riak.New("127.0.0.1:8087")
	err := client.Connect()
	if err != nil {
		fmt.Println("Cannot connect, is Riak running?")
		return
	}

	bucket := client.Bucket("tstriak")
	obj := bucket.New("tstobj")
	obj.ContentType = "application/json"
	obj.Data = []byte("{'field':'value'}")
	obj.Store()

	fmt.Printf("Stored an object in Riak, vclock = %v\n", obj.Vclock)

	client.Close()
}
```

The library is still a work in progress, the API should resemble that of the Ruby riak-client.
To install run `go get github.com/tpjg/goriakpbc` and use import as in the example above.

Especially MapReduce and Document Models still need a lot of work, however some functions that are already implemented are described below.

### Map Reduce

There is a function to run a MapReduce directly:
```go
func (c *Client) RunMapReduce(query string) (resp [][]byte, err error)
```

And MapReduce queries can be build similar to how the MapReduce class from the Ruby riak-client works:
```go
mr := client.MapReduce()
mr.Add("bucket", "key")
mr.LinkBucket("otherbucket", false)
mr.Map("function(v) {return [JSON.parse(v.values[0].data)];}", true)
res, err := mr.Run()
```
Map functions using Erlang instead of Javascript must be added using "MapErlang" instead of "Map" and there is a predefined function "MapObjectValue" that uses the riak_kv_mapreduce module's map_object_value function.

### Riak Document Models

The package now contains some rudimentary support for "Document Models". This is implemented in such a way to easily integrate a Go application into a project that also uses Ruby (on Rails) with the "ripple" gem (https://github.com/seancribbs/ripple).

This is done by parsing the JSON data and mapping it to a struct's fields. To enable easy integration with Ruby/ripple projects the struct "tag" feature of Go is used to possibly get around the naming convention differences between Go and Ruby (Uppercase starting letter required for export versus Uppercase being constants and typically CamelCase versus snake_case). Also it stores the model/struct name as _type in Riak just like ripple does.

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
        DownloadEnabled  bool    "download_enabled"
        Ip               string  "ip"
        Description      string  "description"
        RiakModel        riak.Model
    }
```
Note that it is required to have a "RiakModel" field that is a riak.Model.

To get an instantiated struct from Riak would then require only a call to the riak.Client "Load" function, and to store it call "Save" or "SaveAs":
```go
client := riak.New("127.0.0.1:8087")
err := client.Connect()
var dev Device 
err = client.Load("devices", "abcdefghijklm", &dev)
dev.Description = "something else"
err = client.SaveAs("newkey", &dev) // or: err=dev.RiakModel.SaveAs("newKey")
```

### Full documentation (including protobuf generated)

`http://go.pkgdoc.org/github.com/tpjg/goriakpbc` or `go doc`

### Licensing

goriakpbc is distributed under the Apache license, see `LICENSE.txt` file or http://www.apache.org/licenses/LICENSE-2.0 for details.

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

### Riak Document Models

The package now contains some rudimentary support for "Document Models". This is implemented in such a way to easily integrate a Go application into a project that also uses Ruby (on Rails) with the "ripple" gem (https://github.com/seancribbs/ripple).

This is done by parsing the JSON data and mapping it to a struct's fields. To enable easy integration with Ruby/ripple projects the struct "tag" feature of Go is used to possibly get around the naming convention differences between Go and Ruby (Uppercase starting letter required for export versus Uppercase being constants and typically CamelCase versus underscores). Also it stores the model/struct name as _type in Riak just like ripple does.

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

To get an instantiated struct from Riak would then require only a call to the riak.Client "Get" function:
```go
client := riak.New("127.0.0.1:8087")
err := client.Connect()
var dev Device 
err = client.Get("devices", "abcdefghijklm", &dev)
```

### Full documentation (including protobuf generated)

`http://go.pkgdoc.org/github.com/tpjg/goriakpbc` or `go doc`

### Licensing

goriakpbc is distributed under the Apache license, see `LICENSE` file or `http://www.apache.org/licenses/LICENSE-2.0` for details.

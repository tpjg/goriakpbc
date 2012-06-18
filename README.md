goriakpbc
=======

Package riak is a riak-client, inspired by the Ruby riak-client gem and the riakpbc go package from mrb (github.com/mrb/riakpbc).
It implements a connection to Riak using protobuf.

```go
package main

import (
	"fmt"
	"riak"
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

### Full documentation (including protobuf generated)

`http://go.pkgdoc.org/github.com/tpjg/goriakpbc` or `go doc`

### Licensing

goriakpbc is distributed under the Apache license, see `LICENSE` file or `http://www.apache.org/licenses/LICENSE-2.0` for details.

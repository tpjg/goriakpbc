package riak

import (
	"github.com/bmizerany/assert"
	"io"
	"testing"
	"time"
)

func TestRFile(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)
	f, err := client.CreateFile("rfile_test.go", "test", "text/plain", 1024)
	assert.T(t, err == nil)
	assert.T(t, f != nil)
	b, err := f.Write([]byte{'1', '2', '3', '4', '5', '6', '7', '8'})
	assert.T(t, err == nil)
	assert.T(t, b == 8)
	// Add a full chunk
	extra := make([]byte, 1024)
	b, err = f.Write(extra)
	assert.T(t, err == nil)
	assert.T(t, b == 1024)

	// Now rewind and change some content
	f.Seek(0, 0)
	b, err = f.Write([]byte{'a', 'b', 'c'}) // Overwriting first three bytes
	assert.T(t, err == nil)
	assert.T(t, b == 3)
	f.Seek(1024, 0)
	b, err = f.Write([]byte{'d', 'e', 'f'}) // Overwriting first three bytes in 2nd chunk
	assert.T(t, err == nil)
	assert.T(t, b == 3)
	// Seek to the end
	bp, err := f.Seek(0, 2)
	assert.T(t, err == nil)
	assert.T(t, bp == 1032)
	b, err = f.Write([]byte{'z'}) // Append one byte
	assert.T(t, err == nil)
	assert.T(t, b == 1)

	// Test the Reader functionality as well as OpenFile
	f, err = client.OpenFile("rfile_test.go", "test")
	assert.T(t, err == nil)
	buf := make([]byte, 1024)
	// Check if the Last Modified time is more-or-less now, should be well within 3 seconds unless something is really wrong with Riak
	d := time.Now().Sub(f.LastModified())
	if d > 0 {
		assert.T(t, d < 3*time.Second)
	} else {
		assert.T(t, -d < 3*time.Second)
	}
	f.Seek(0, 0)
	// Read the first chunk
	b, err = f.Read(buf)
	assert.T(t, err == nil)
	assert.T(t, b == 1024)
	assert.T(t, buf[0] == 'a')
	assert.T(t, buf[1] == 'b')
	assert.T(t, buf[2] == 'c')
	assert.T(t, buf[3] == '4')
	assert.T(t, buf[7] == '8')
	assert.T(t, buf[8] == 0)
	// Read the second chunk
	b, err = f.Read(buf)
	assert.T(t, err == io.EOF)
	assert.T(t, b == 9)
	assert.T(t, buf[0] == 'd')
	assert.T(t, buf[1] == 'e')
	assert.T(t, buf[2] == 'f')
	assert.T(t, buf[3] == 0)
	assert.T(t, buf[8] == 'z')

	// Now there should be three Riak objects: test, test-000000 and test-000001 in the rfile_test.go bucket
	// Check <test> and cleanup
	obj, err := GetFrom("rfile_test.go", "test")
	assert.T(t, err == nil)
	assert.T(t, obj.ContentType == "text/plain")
	assert.T(t, obj.Meta["chunk_size"] == "1024")
	assert.T(t, obj.Meta["chunk_count"] == "2")
	obj.Destroy()
	// Check <test-0000000> and cleanup
	obj, err = GetFrom("rfile_test.go", "test-000000")
	assert.T(t, err == nil)
	assert.T(t, obj.ContentType == "text/plain")
	assert.T(t, len(obj.Data) == 1024)
	assert.T(t, obj.Data[0] == 'a')
	assert.T(t, obj.Data[1] == 'b')
	assert.T(t, obj.Data[2] == 'c')
	assert.T(t, obj.Data[3] == '4')
	assert.T(t, obj.Data[7] == '8')
	assert.T(t, obj.Data[8] == 0)
	obj.Destroy()
	// Check <test-0000001> and cleanup
	obj, err = GetFrom("rfile_test.go", "test-000001")
	assert.T(t, err == nil)
	assert.T(t, obj.ContentType == "text/plain")
	assert.T(t, len(obj.Data) == 9)
	assert.T(t, obj.Data[0] == 'd')
	assert.T(t, obj.Data[1] == 'e')
	assert.T(t, obj.Data[2] == 'f')
	assert.T(t, obj.Data[3] == 0)
	assert.T(t, obj.Data[8] == 'z')
	obj.Destroy()
}

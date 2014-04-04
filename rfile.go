package riak

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"
)

/* The RFile struct stores (large) values in Riak and behaves very similar
to a regular os.File object. It implements the io.Reader, io.Writer and
io.Seeker interfaces. The value is split into chunks because really large
values (>10Mb) can't be stored efficiently in Riak and also because growing
or changing large values is inefficient (changing a single byte would require
a PUT of the entire, possibly large, value).
*/
type RFile struct {
	client     *Client
	root       *RObject // The "root", holding meta-data only
	chunk      *RObject // The current chunk or segment of data
	chunk_size int
	pos        int
	size       int
}

var (
	NotFile     = errors.New("Not suitable to use as RFile")
	ErrorInFile = errors.New("Error in RFile")
)

// Return the Key for a specific chunk
func chunkKey(key string, chunkno int) string {
	return fmt.Sprintf("%v-%06d", key, chunkno)
}

// Create a new RFile. Will overwrite/truncate existing data.
func (c *Client) CreateFile(bucketname string, key string, contentType string, chunk_size int, options ...map[string]uint32) (*RFile, error) {
	bucket, err := c.Bucket(bucketname)
	if err != nil {
		return nil, err
	}
	// Create the root object, holding meta-data only
	root := bucket.NewObject(key, options...)
	root.ContentType = contentType
	root.Data = []byte{}
	root.Meta["chunk_size"] = strconv.Itoa(chunk_size)
	root.Meta["chunk_count"] = strconv.Itoa(0)
	err = root.Store()
	if err != nil {
		return nil, err
	}
	// Return the completed struct
	return &RFile{c, root, nil, chunk_size, 0, 0}, nil
}

func CreateFile(bucketname string, key string, contentType string, chunk_size int, options ...map[string]uint32) (*RFile, error) {
	return defaultClient.CreateFile(bucketname, key, contentType, chunk_size, options...)
}

// Open a File. Will return an error if it does not exist in Riak yet or does
// not have the correct meta-tags to support File-like operations.
func (c *Client) OpenFile(bucketname string, key string, options ...map[string]uint32) (*RFile, error) {
	root, err := c.GetFrom(bucketname, key, options...)
	if err != nil {
		return nil, err
	}
	chunk_size, err := strconv.Atoi(root.Meta["chunk_size"])
	if err != nil || chunk_size < 1 || chunk_size > 100*1024*1024 {
		// Supports chunks up to 100Mb, there is some conflicting information about maximum
		// value size in Riak ranging from 100Kb (for low latency guarantees) to 20Mb.
		return nil, NotFile
	}
	chunk_count, err := strconv.Atoi(root.Meta["chunk_count"])
	if err != nil || chunk_count < 0 {
		return nil, NotFile
	}
	// Determine the size by looking at the last chunk
	if chunk_count > 0 {
		chunk, err := c.GetFrom(bucketname, chunkKey(key, chunk_count-1), options...)
		if err != nil {
			return nil, ErrorInFile
		}
		return &RFile{c, root, chunk, chunk_size, 0, (chunk_count-1)*chunk_size + len(chunk.Data)}, nil
	}
	// Otherwise size is 0
	return &RFile{c, root, nil, chunk_size, 0, 0}, nil
}

func OpenFile(bucketname string, key string, options ...map[string]uint32) (*RFile, error) {
	return defaultClient.OpenFile(bucketname, key, options...)
}

// Implements the io.Seeker interface
func (r *RFile) Seek(offset int64, whence int) (int64, error) {
	if whence == 0 {
		if offset < 0 || int(offset) > r.size {
			return int64(r.pos), io.EOF
		}
		r.pos = int(offset)
	} else if whence == 1 {
		if r.pos+int(offset) < 0 || r.pos+int(offset) > r.size {
			return int64(r.pos), io.EOF
		}
		r.pos += int(offset)
	} else if whence == 2 {
		if r.size+int(offset) < 0 || r.size+int(offset) > r.size {
			return int64(r.pos), io.EOF
		}
		r.pos = r.size + int(offset)
	}
	return int64(r.pos), nil
}

// Implements the io.Writer interface
func (r *RFile) Write(p []byte) (n int, err error) {
	wpos := 0 // Keep track how much of p has been written
	for wpos < len(p) {
		k := chunkKey(r.root.Key, r.pos/r.chunk_size)
		// Check if a chunk must be loaded (position<size or not completely written)
		if r.pos < r.size || r.size%r.chunk_size != 0 {
			// If the current chunk happens to be the one we're looking for, then skip loading.
			if !(r.chunk != nil && r.chunk.Key == k) {
				r.chunk, err = r.client.GetFrom(r.root.Bucket.name, k, r.root.Options...)
				if err != nil {
					return wpos, err
				}
			}
		} else {
			// Create a new chunk
			r.chunk, err = r.client.NewObjectIn(r.root.Bucket.name, k, r.root.Options...)
			r.chunk.ContentType = r.root.ContentType
			if err != nil {
				return wpos, err
			}
		}
		// Check where to start writing within the chunk
		cpos := r.pos % r.chunk_size
		// Determine how many bytes to write into this chunk (fill up to chunk_size)
		towrite := len(p) - wpos
		if towrite > (r.chunk_size - cpos) {
			towrite = r.chunk_size - cpos
		}
		// Check if the chunk Data is large enough, otherwise write the first part
		// and then append the last part.
		check := 0
		if len(r.chunk.Data) == 0 {
			// Specialized case for a new chunk
			r.chunk.Data = make([]byte, towrite)
			check = copy(r.chunk.Data, p[wpos:wpos+towrite])
		} else if len(r.chunk.Data) < cpos+towrite {
			if cpos == len(r.chunk.Data) {
				// Just append to the chunk
				r.chunk.Data = append(r.chunk.Data, p[wpos:wpos+towrite]...)
				check = towrite // just assume
			} else {
				// Copy the part that fits, then append
				part := len(r.chunk.Data) - cpos
				check = copy(r.chunk.Data[cpos:], p[wpos:wpos+part])
				r.chunk.Data = append(r.chunk.Data, p[wpos+part:wpos+towrite-part]...)
				check += towrite - part
			}
		} else {
			// Copy to the chunk
			check = copy(r.chunk.Data[cpos:], p[wpos:wpos+towrite])
		}
		if check != towrite {
			errors.New("Should never happen!")
		}
		// Save the chunk to Riak
		err = r.chunk.Store()
		if err != nil {
			return wpos, err
		}
		// Update the counters
		r.pos += towrite
		wpos += towrite
		// Update the size if necessary
		if r.pos > r.size {
			if (r.pos / r.chunk_size) > (r.size / r.chunk_size) {
				// Update the root KV
				r.root.Meta["chunk_count"] = strconv.Itoa(r.pos/r.chunk_size + 1)
				err = r.root.Store()
				if err != nil {
					return wpos, nil
				}
			}
			r.size = r.pos
		}
	}
	// Return the number of bytes written
	return wpos, nil
}

// Implements the io.Reader interface
func (r *RFile) Read(p []byte) (n int, err error) {
	rpos := 0 // Keep track how much of p has been read
	for rpos < len(p) {
		k := chunkKey(r.root.Key, r.pos/r.chunk_size)
		// Check if a chunk must be loaded
		if r.pos < r.size || r.size%r.chunk_size != 0 {
			// If the current chunk happens to be the one we're looking for, then skip loading.
			if !(r.chunk != nil && r.chunk.Key == k) {
				r.chunk, err = r.client.GetFrom(r.root.Bucket.name, k, r.root.Options...)
				if err != nil {
					return rpos, err
				}
			}
		} else {
			// Reading beyong EOF
			return rpos, io.EOF
		}
		// Check where to start reading within the chunk
		cpos := r.pos % r.chunk_size
		// Determine how many bytes to write into this chunk (fill up to chunk_size)
		toread := len(p) - rpos
		if toread > (r.chunk_size - cpos) {
			toread = r.chunk_size - cpos
		}
		// Check what is left to read
		if toread > (len(p) - rpos) {
			toread = len(p) - rpos
		}
		// Check if the chunk Data is large enough, otherwise read it and return EOF
		if len(r.chunk.Data[cpos:]) < toread {
			copy(p[rpos:], r.chunk.Data[cpos:])
			return rpos + len(r.chunk.Data[cpos:]), io.EOF
		}

		// Read the chunk
		copy(p[rpos:], r.chunk.Data[cpos:cpos+toread])
		// Update counters
		r.pos += len(r.chunk.Data[cpos : cpos+toread])
		rpos += len(r.chunk.Data[cpos : cpos+toread])
	}
	// Return the number of bytes written
	return rpos, nil
}

// Expose ContentType from the underlying root RObject
func (r *RFile) ContentType() string {
	return r.root.ContentType
}

// Expose VTag from the underlying root RObject
func (r *RFile) VTag() string {
	return r.root.Vtag
}

// Expose LastMod from the underlying root RObject
func (r *RFile) LastMod() uint32 {
	return r.root.LastMod
}

// Expose LastModUSecs from the underlying root RObject
func (r *RFile) LastModUsecs() uint32 {
	return r.root.LastModUsecs
}

// Return Last Modified timestamp from the underlying root RObject
func (r *RFile) LastModified() time.Time {
	return time.Unix(int64(r.root.LastMod), int64(r.root.LastModUsecs*1000))
}

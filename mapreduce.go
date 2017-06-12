package riak

import (
	"encoding/json"
	"fmt"
	"github.com/atticlab/goriakpbc/pb"
)

// An object to build a MapReduce job similar to how the Ruby client can
// build it by adding different stages.
type MapReduce struct {
	client  *Client
	inputs  [][]string
	index   string
	phases  []string
	request string
}

func (c *Client) MapReduce() *MapReduce {
	return &MapReduce{client: c, inputs: make([][]string, 0), phases: make([]string, 0), request: ""}
}

func (mr *MapReduce) Add(bucket string, key string) (err error) {
	if mr.index != "" {
		return BadMapReduceInputs
	}
	mr.inputs = append(mr.inputs, []string{bucket, key})
	return
}

// Add a whole bucket as input. Note that this ONLY works on buckets that have secondary indexes (2i) enabled since
// listing keys on a bucket without using indexes is dangerous on production clusters.
func (mr *MapReduce) AddBucket(bucket string) (err error) {
	if len(mr.inputs) > 0 || mr.index != "" {
		return BadMapReduceInputs
	}
	mr.index = fmt.Sprintf(`"bucket":"%v","index":"$bucket","key":"%v"`, bucket, bucket)
	return
}

// Add a range of keys from one bucket using secondary indexes.
func (mr *MapReduce) AddBucketRange(bucket string, start string, end string) (err error) {
	if len(mr.inputs) > 0 || mr.index != "" {
		return BadMapReduceInputs
	}
	mr.index = fmt.Sprintf(`"bucket":"%v","index":"$key","start":"%v","end":"%v"`, bucket, start, end)
	return
}

// Add a keys using a secondary index.
func (mr *MapReduce) AddIndex(bucket string, index string, key string) (err error) {
	if len(mr.inputs) > 0 || mr.index != "" {
		return BadMapReduceInputs
	}
	mr.index = fmt.Sprintf(`"bucket":"%v","index":"%v","key":"%v","end":"%v"`, bucket, index, key)
	return
}

// Add a range of keys using a secondary index.
func (mr *MapReduce) AddIndexRange(bucket string, index string, start string, end string) (err error) {
	if len(mr.inputs) > 0 || mr.index != "" {
		return BadMapReduceInputs
	}
	mr.index = fmt.Sprintf(`"bucket":"%v","index":"%v","start":"%v","end":"%v"`, bucket, index, start, end)
	return
}

func (mr *MapReduce) LinkBucket(name string, keep bool) {
	link := `{"link":{"bucket":"` + name + `", "tag":"_","keep":`
	if keep {
		link = link + "true}}"
	} else {
		link = link + "false}}"
	}
	mr.phases = append(mr.phases, link)
}

func (mr *MapReduce) Map(fun string, keep bool) {
	m := `{"map":{"language":"javascript","keep":`
	if keep {
		m = m + "true,"
	} else {
		m = m + "false,"
	}
	m = m + `"source":"` + fun + `"}}`
	mr.phases = append(mr.phases, m)
}

func (mr *MapReduce) Reduce(fun string, keep bool) {
	m := `{"reduce":{"language":"javascript","keep":`
	if keep {
		m = m + "true,"
	} else {
		m = m + "false,"
	}
	m = m + `"source":"` + fun + `"}}`
	mr.phases = append(mr.phases, m)
}

func (mr *MapReduce) MapErlang(module string, fun string, keep bool) {
	m := `{"map":{"language":"erlang","module":"` + module + `","function":"` + fun + `","keep":`
	if keep {
		m = m + `true`
	} else {
		m = m + `false`
	}
	m = m + `}}`
	mr.phases = append(mr.phases, m)
}

func (mr *MapReduce) ReduceErlang(module string, fun string, arg string, keep bool) {
	m := `{"reduce":{"language":"erlang","module":"` + module + `","function":"` + fun + `"`
	if arg != "" {
		m = m + `,"arg":` + arg + `,"keep":`
	} else {
		m = m + `,"keep":`
	}
	if keep {
		m = m + `true`
	} else {
		m = m + `false`
	}
	m = m + `}}`
	mr.phases = append(mr.phases, m)
}

func (mr *MapReduce) MapObjectValue(keep bool) {
	//{"map":{"language":"erlang","module":"riak_kv_mapreduce","function":"map_object_value"}}
	mr.MapErlang("riak_kv_mapreduce", "map_object_value", keep)
}

func (mr *MapReduce) ReduceObjectCount(keep bool) {
	mr.ReduceErlang("riak_kv_mapreduce", "reduce_count_inputs", `{"do_prereduce":true}`, keep)
}

// Generate the Query string
func (mr *MapReduce) Query() (query []byte, err error) {
	// Check because either a whole bucket should be added or only inputs
	if mr.index != "" && len(mr.inputs) > 0 {
		err = BadMapReduceInputs
		return
	}
	q := ``
	if mr.index == "" {
		inputs, err := json.Marshal(mr.inputs)
		if err != nil {
			return nil, err
		}
		q = `{"inputs":` + string(inputs) + `, "query":[`
	} else {
		q = `{"inputs":{` + mr.index + `}, "query":[`
	}
	for i, s := range mr.phases {
		if i > 0 {
			q = q + ","
		}
		q = q + s
	}
	q = q + "]}"
	return []byte(q), nil
}

func (mr *MapReduce) Run() (resp [][]byte, err error) {
	query, err := mr.Query()
	if err != nil {
		return nil, err
	}
	req := &pb.RpbMapRedReq{
		Request:     query,
		ContentType: []byte("application/json"),
	}
	err, conn := mr.client.request(req, rpbMapRedReq)
	if err != nil {
		return nil, err
	}
	resp, err = mr.client.mr_response(conn)
	if err != nil {
		return nil, err
	}
	return
}

// Run a MapReduce query
func (c *Client) RunMapReduce(query string) (resp [][]byte, err error) {
	req := &pb.RpbMapRedReq{
		Request:     []byte(query),
		ContentType: []byte("application/json"),
	}
	err, conn := c.request(req, rpbMapRedReq)
	if err != nil {
		return nil, err
	}
	resp, err = c.mr_response(conn)
	if err != nil {
		return nil, err
	}
	return
}

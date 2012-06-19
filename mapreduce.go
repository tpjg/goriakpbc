package riak

// An object to build a MapReduce job similar to how the Ruby client can
// build it by adding different stages.
type MapReduce struct {
	client  *Client
	request string
}

func (c *Client) MapReduce() *MapReduce {
	return &MapReduce{client: c, request: ""}
}

// Run a MapReduce query
func (c *Client) RunMapReduce(mr string) (resp [][]byte, err error) {
	req := &RpbMapRedReq{
		Request:     []byte(mr),
		ContentType: []byte("application/json"),
	}
	err = c.request(req, "RpbMapRedReq")
	if err != nil {
		return nil, err
	}
	resp, err = c.mr_response()
	if err != nil {
		return nil, err
	}
	return
}

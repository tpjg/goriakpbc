package riak

import "github.com/tpjg/goriakpbc/pb"

type Search struct {
	Q       string
	Index   string
	Rows    uint32
	Start   uint32
	Sort    string
	Filter  string
	Df      string
	Op      string
	PreSort string
	Fields  []string
}

func (c *Client) Search(s *Search) ([]map[string]string, error) {
	fl := make([][]byte, len(s.Fields))
	for i, f := range s.Fields {
		fl[i] = []byte(f)
	}

	rows := s.Rows
	if s.Rows == 0 {
		rows = 10
	}

	req := &pb.RpbSearchQueryReq{
		Q:      []byte(s.Q),
		Index:  []byte(s.Index),
		Rows:   &rows,
		Start:  &s.Start,
		Sort:   []byte(s.Sort),
		Filter: []byte(s.Filter),
		Df:     []byte(s.Df),
		Op:     []byte(s.Op),
		Fl:     fl,
	}
	if len(s.PreSort) != 0 {
		req.Presort = []byte(s.PreSort)
	}

	err, conn := c.request(req, rpbSearchQueryReq)
	if err != nil {
		return nil, err
	}

	resp := &pb.RpbSearchQueryResp{}
	err = c.response(conn, resp)
	if err != nil {
		return nil, err
	}
	docs := resp.GetDocs()

	res := make([]map[string]string, len(docs))
	for i, doc := range docs {
		res[i] = make(map[string]string)
		for _, f := range doc.GetFields() {
			res[i][string(f.Key)] = string(f.Value)
		}
	}

	return res, nil
}

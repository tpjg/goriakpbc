package riak

import (
	"github.com/tpjg/goriakpbc/pb"
	"errors"
)

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

type Schema struct {
	Name    string
	Content string
	c Client
}

type SearchIndex struct {
	Name   string
	Schema string
	NVal   uint32
	c Client
}

func (c *Client) Search(s *Search) ([]map[string][]byte, float32, uint32, error) {
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
		return nil, 0.0, 0, err
	}

	resp := &pb.RpbSearchQueryResp{}
	err = c.response(conn, resp)
	if err != nil {
		return nil, 0.0, 0, err
	}
	docs := resp.GetDocs()

	res := make([]map[string][]byte, len(docs))
	for i, doc := range docs {
		res[i] = make(map[string][]byte)
		for _, f := range doc.GetFields() {
			res[i][string(f.Key)] = f.Value
		}
	}

	return res, resp.GetMaxScore(), resp.GetNumFound(), nil
}

func (c *Client) NewSchema(name string, content string) (*Schema, error) {
	if name == "" {
		return nil, errors.New("No schema name specified")
	}
	if content == "" {
		return nil, errors.New("No schema content specified")
	}
	return &Schema{Name: name, Content: content, c: *c}, nil
}

// StoreSchema validate schema and sends it to Riak
func (s *Schema) Store() error {
	if s.Name == "" {
		return errors.New("No schema name specified")
	}
	if s.Content == "" {
		return errors.New("No schema content specified")
	}

	protobuf := &pb.RpbYokozunaSchemaPutReq{
		Schema: &pb.RpbYokozunaSchema {
			Name: []byte(s.Name),
			Content: []byte(s.Content),
		},
	}
	err, conn := s.c.request(protobuf, rpbYokozunaSchemaPutReq)
	if err != nil {
		return err
	}

	resp := &pb.RpbPutResp{}
	err = s.c.response(conn, resp)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) FetchSchema(schemaName string) (*Schema, error) {
	if schemaName == "" {
		return nil, errors.New("No schema name specified")
	}
	protobuf := &pb.RpbYokozunaSchemaGetReq{
		Name: []byte(schemaName),
	}
	err, conn := c.request(protobuf, rpbYokozunaSchemaGetReq)
	if err != nil {
		return nil, err
	}

	resp := &pb.RpbYokozunaSchemaGetResp{}
	err = c.response(conn, resp)
	if err != nil {
		return nil, err
	}

	return &Schema{Name: string(resp.Schema.Name), Content: string(resp.Schema.Content)}, nil
}

func (c *Client) NewSearchIndex(indexName string, schemaName string, nval uint32) (*SearchIndex, error) {
	if indexName == "" {
		return nil, errors.New("No index name specified")
	}
	if schemaName == "" {
		return nil, errors.New("No schema name specified")
	}
	if nval == 0 {
		return nil, errors.New("No nval specified or is not correct")
	}
	return &SearchIndex{Name: indexName, Schema: schemaName, NVal: nval, c: *c}, nil
}

// !Please wait some (5) seconds for Riak stores indexes, before start using it
func (s *SearchIndex) Store() error {
	if s.Name == "" {
		return errors.New("No index name specified")
	}
	if s.Schema == "" {
		return errors.New("No schema name specified")
	}
	if s.NVal == 0 {
		return errors.New("No nval specified or is not correct")
	}

	protobuf := &pb.RpbYokozunaIndexPutReq{
		Index: &pb.RpbYokozunaIndex{
			Name: []byte(s.Name),
			Schema: []byte(s.Schema),
			NVal: &s.NVal,
		},
	}
	err, conn := s.c.request(protobuf, rpbYokozunaIndexPutReq)
	if err != nil {
		return err
	}
	resp := &pb.RpbPutResp{}
	err = s.c.response(conn, resp)
	if err != nil {
		return err
	}
	return nil
}

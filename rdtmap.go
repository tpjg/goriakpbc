package riak

import (
	"fmt"

	"github.com/tpjg/goriakpbc/pb"
)

const (
	COUNTER  = 1
	SET      = 2
	REGISTER = 3
	FLAG     = 4
	MAP      = 5
)

type MapKey struct {
	Key  string
	Type pb.MapField_MapFieldType
}

type RDtMap struct {
	Bucket   *Bucket
	Key      string
	Options  []map[string]uint32
	Values   map[MapKey]interface{}
	Context  []uint8
	ToAdd    []*pb.MapUpdate
	ToRemove []*pb.MapField
}

type RDtFlag struct {
	Value    bool
	Enabled  bool
	Disabled bool
}

func (f *RDtFlag) GetValue() bool {
	return f.Value
}

func (f *RDtFlag) Enable() {
	f.Enabled = true
	f.Disabled = false
}

func (f *RDtFlag) Disable() {
	f.Enabled = false
	f.Disabled = true
}

type RDtRegister struct {
	Value    []byte
	NewValue []byte
}

func (f *RDtRegister) GetValue() []byte {
	return f.Value
}

func (r *RDtRegister) Update(value []byte) {
	r.NewValue = value
}

func (m *RDtMap) Init(mapvalues []*pb.MapEntry) {
	m.Values = make(map[MapKey]interface{})
	if mapvalues == nil {
		return
	}
	for _, e := range mapvalues {
		t := *e.Field.Type
		var ne interface{}
		switch t {
		case pb.MapField_FLAG:
			ne = &RDtFlag{Value: *e.FlagValue}
		case pb.MapField_REGISTER:
			ne = &RDtRegister{Value: e.RegisterValue}
		case pb.MapField_COUNTER:
			ne = &RDtCounter{Value: e.CounterValue, Context: m.Context}
		case pb.MapField_SET:
			ne = &RDtSet{Value: e.SetValue, Context: m.Context}
		case pb.MapField_MAP:
			ne = &RDtMap{Context: m.Context}
			ne.(*RDtMap).Init(e.MapValue)
		}
		m.Values[MapKey{Key: string(e.Field.Name), Type: t}] = ne
	}
}

func (m *RDtMap) Size() int {
	return len(m.Values)
}

func (m *RDtMap) Fetch(key string, t int32) (e interface{}) {
	e, _ = m.Values[MapKey{Key: key, Type: pb.MapField_MapFieldType(t)}]
	return
}

func (m *RDtMap) Add(key string, t int32) (e interface{}) {
	e = m.Fetch(key, t)
	if e != nil {
		return
	}
	switch pb.MapField_MapFieldType(t) {
	case pb.MapField_FLAG:
		e = &RDtFlag{}
	case pb.MapField_REGISTER:
		e = &RDtRegister{}
	case pb.MapField_COUNTER:
		e = &RDtCounter{Context: m.Context}
	case pb.MapField_SET:
		e = &RDtSet{Context: m.Context}
	case pb.MapField_MAP:
		e = &RDtMap{Context: m.Context}
		e.(*RDtMap).Init(nil)
	}
	m.Values[MapKey{Key: key, Type: pb.MapField_MapFieldType(t)}] = e
	return
}

func (m *RDtMap) Print() {
	m.PrintInt(0)
}

func (m *RDtMap) PrintInt(indent int) {
	for i := 0; i < indent; i++ {
		fmt.Printf("\t")
	}
	fmt.Printf("{\n")
	for key, value := range m.Values {
		for i := 0; i < indent; i++ {
			fmt.Printf("\t")
		}
		switch dt := value.(type) {
		case *RDtFlag:
			fmt.Printf("\t%s - %t\n", key, dt.GetValue())
		case *RDtRegister:
			fmt.Printf("\t%s - %s\n", key, dt.GetValue())
		case *RDtCounter:
			fmt.Printf("\t%s - %d\n", key, dt.GetValue())
		case *RDtSet:
			fmt.Printf("\t%s - [\n", key)
			for _, v := range dt.GetValue() {
				for i := 0; i < indent; i++ {
					fmt.Printf("\t")
				}
				fmt.Printf("\t%s\n", v)
			}
			for i := 0; i < indent; i++ {
				fmt.Printf("\t")
			}
			fmt.Printf("\t]\n")
		case *RDtMap:
			fmt.Printf("\t%s - ", key)
			dt.PrintInt(indent + 1)
		}
	}
	for i := 0; i < indent; i++ {
		fmt.Printf("\t")
	}
	fmt.Printf("}\n")
}

func (m *RDtMap) ToOp() *pb.DtOp {
	ops := &pb.DtOp{MapOp: &pb.MapOp{}}
	var field *pb.MapField
	for key, value := range m.Values {
		t := key.Type
		field = &pb.MapField{Name: []byte(key.Key), Type: &t}
		switch dt := value.(type) {
		case *RDtFlag:
			var op pb.MapUpdate_FlagOp
			update := false
			if dt.Enabled {
				op = pb.MapUpdate_ENABLE
				update = true
			} else if dt.Disabled {
				op = pb.MapUpdate_DISABLE
				update = true
			}
			if update {
				ops.MapOp.Updates = append(ops.MapOp.Updates, &pb.MapUpdate{FlagOp: &op, Field: field})
			}
		case *RDtRegister:
			var up *pb.MapUpdate
			if dt.NewValue != nil {
				up = &pb.MapUpdate{RegisterOp: dt.NewValue, Field: field}
			}
			if up != nil {
				ops.MapOp.Updates = append(ops.MapOp.Updates, up)
			}
		case *RDtCounter:
			op := dt.ToOp()
			if op != nil {
				ops.MapOp.Updates = append(ops.MapOp.Updates, &pb.MapUpdate{CounterOp: op.CounterOp, Field: field})
			}
		case *RDtSet:
			op := dt.ToOp()
			if op != nil {
				ops.MapOp.Updates = append(ops.MapOp.Updates, &pb.MapUpdate{SetOp: op.SetOp, Field: field})
			}
		case *RDtMap:
			op := dt.ToOp()
			if op != nil {
				ops.MapOp.Updates = append(ops.MapOp.Updates, &pb.MapUpdate{MapOp: op.MapOp, Field: field})
			}
		}
	}
	return ops
}

func (m *RDtMap) Store() (err error) {
	req := &pb.DtUpdateReq{
		Type:    []byte(m.Bucket.bucket_type),
		Bucket:  []byte(m.Bucket.name),
		Context: m.Context,
		Key:     []byte(m.Key),
		Op:      m.ToOp(),
	}

	// Add the options
	for _, omap := range m.Options {
		for k, v := range omap {
			switch k {
			case "w":
				req.W = &v
			case "dw":
				req.Dw = &v
			case "pw":
				req.Pw = &v
			}
		}
	}

	// Send the request
	err, conn := m.Bucket.client.request(req, dtUpdateReq)
	if err != nil {
		return err
	}
	// Get response, ReturnHead is true, so we can store the vclock
	resp := &pb.DtUpdateResp{}
	err = m.Bucket.client.response(conn, resp)
	if err != nil {
		return err
	}
	return nil
}

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
	RDataTypeObject
	Values   map[MapKey]interface{}
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
			ne = &RDtCounter{Value: e.CounterValue, RDataTypeObject: RDataTypeObject{Context: m.Context}}
		case pb.MapField_SET:
			ne = &RDtSet{Value: e.SetValue, RDataTypeObject: RDataTypeObject{Context: m.Context}}
		case pb.MapField_MAP:
			ne = &RDtMap{RDataTypeObject: RDataTypeObject{Context: m.Context}}
			ne.(*RDtMap).Init(e.MapValue)
		}
		m.Values[MapKey{Key: string(e.Field.Name), Type: t}] = ne
	}
}

func (m *RDtMap) Size() int {
	return len(m.Values)
}

func (m *RDtMap) FetchCounter(key string) (e *RDtCounter) {
	o, _ := m.Values[MapKey{Key: key, Type: pb.MapField_COUNTER}]
	if o == nil {
		return nil
	}
	e = o.(*RDtCounter)
	return
}

func (m *RDtMap) FetchSet(key string) (e *RDtSet) {
	o, _ := m.Values[MapKey{Key: key, Type: pb.MapField_SET}]
	if o == nil {
		return nil
	}
	e = o.(*RDtSet)
	return
}

func (m *RDtMap) FetchRegister(key string) (e *RDtRegister) {
	o, _ := m.Values[MapKey{Key: key, Type: pb.MapField_REGISTER}]
	if o == nil {
		return nil
	}
	e = o.(*RDtRegister)
	return
}

func (m *RDtMap) FetchFlag(key string) (e *RDtFlag) {
	o, _ := m.Values[MapKey{Key: key, Type: pb.MapField_FLAG}]
	if o == nil {
		return nil
	}
	e = o.(*RDtFlag)
	return
}

func (m *RDtMap) FetchMap(key string) (e *RDtMap) {
	o, _ := m.Values[MapKey{Key: key, Type: pb.MapField_MAP}]
	if o == nil {
		return nil
	}
	e = o.(*RDtMap)
	return
}

func (m *RDtMap) AddCounter(key string) (e *RDtCounter) {
	e = m.FetchCounter(key)
	if e != nil {
		return
	}
	e = &RDtCounter{RDataTypeObject: RDataTypeObject{Context: m.Context}}
	m.Values[MapKey{Key: key, Type: pb.MapField_COUNTER}] = e
	return
}

func (m *RDtMap) AddSet(key string) (e *RDtSet) {
	e = m.FetchSet(key)
	if e != nil {
		return
	}
	e = &RDtSet{RDataTypeObject: RDataTypeObject{Context: m.Context}}
	m.Values[MapKey{Key: key, Type: pb.MapField_SET}] = e
	return
}

func (m *RDtMap) AddRegister(key string) (e *RDtRegister) {
	e = m.FetchRegister(key)
	if e != nil {
		return
	}
	e = &RDtRegister{}
	m.Values[MapKey{Key: key, Type: pb.MapField_REGISTER}] = e
	return
}

func (m *RDtMap) AddFlag(key string) (e *RDtFlag) {
	e = m.FetchFlag(key)
	if e != nil {
		return
	}
	e = &RDtFlag{}
	m.Values[MapKey{Key: key, Type: pb.MapField_FLAG}] = e
	return
}

func (m *RDtMap) AddMap(key string) (e *RDtMap) {
	e = m.FetchMap(key)
	if e != nil {
		return
	}
	e = &RDtMap{RDataTypeObject: RDataTypeObject{Context: m.Context}}
	e.Init(nil)
	m.Values[MapKey{Key: key, Type: pb.MapField_MAP}] = e
	return
}

func (m *RDtMap) RemoveCounter(key string) {
	e := m.FetchCounter(key)
	if e == nil {
		return // nothing to do
	}
	t := pb.MapField_COUNTER
	mk := MapKey{Key: key, Type: t}
	delete(m.Values, mk)
	m.ToRemove = append(m.ToRemove, &pb.MapField{Name: []byte(key), Type: &t})
}

func (m *RDtMap) RemoveSet(key string) {
	e := m.FetchSet(key)
	if e == nil {
		return // nothing to do
	}
	t := pb.MapField_SET
	mk := MapKey{Key: key, Type: t}
	delete(m.Values, mk)
	m.ToRemove = append(m.ToRemove, &pb.MapField{Name: []byte(key), Type: &t})
}

func (m *RDtMap) RemoveRegister(key string) {
	e := m.FetchRegister(key)
	if e == nil {
		return // nothing to do
	}
	t := pb.MapField_REGISTER
	mk := MapKey{Key: key, Type: t}
	delete(m.Values, mk)
	m.ToRemove = append(m.ToRemove, &pb.MapField{Name: []byte(key), Type: &t})
}

func (m *RDtMap) RemoveFlag(key string) {
	e := m.FetchFlag(key)
	if e == nil {
		return // nothing to do
	}
	t := pb.MapField_FLAG
	mk := MapKey{Key: key, Type: t}
	delete(m.Values, mk)
	m.ToRemove = append(m.ToRemove, &pb.MapField{Name: []byte(key), Type: &t})
}

func (m *RDtMap) RemoveMap(key string) {
	e := m.FetchMap(key)
	if e == nil {
		return // nothing to do
	}
	t := pb.MapField_MAP
	mk := MapKey{Key: key, Type: t}
	delete(m.Values, mk)
	m.ToRemove = append(m.ToRemove, &pb.MapField{Name: []byte(key), Type: &t})
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
				fmt.Printf("\t\t%s\n", v)
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
	ops.MapOp.Removes = m.ToRemove
	return ops
}

func (m *RDtMap) Store() (err error) {
	op := m.ToOp()
	if op == nil {
		// nothing to do
		return nil
	}
	return m.RDataTypeObject.store(op)
}

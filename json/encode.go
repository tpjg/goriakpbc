package json

// This file is a minor customization of the original encoding/json package for use in goriakpbc

// Copyright 2010 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE.go.txt file.

// Package json implements encoding and decoding of JSON objects as defined in
// RFC 4627.
//
// See "JSON and Go" for an introduction to this package:
// http://golang.org/doc/articles/json_and_go.html

import (
	"bytes"
	"encoding/base64"
	"math"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

// Marshal returns the JSON encoding of v.
//
// Marshal traverses the value v recursively.
// If an encountered value implements the Marshaler interface
// and is not a nil pointer, Marshal calls its MarshalJSON method
// to produce JSON.  The nil pointer exception is not strictly necessary
// but mimics a similar, necessary exception in the behavior of
// UnmarshalJSON.
//
// Otherwise, Marshal uses the following type-dependent default encodings:
//
// Boolean values encode as JSON booleans.
//
// Floating point and integer values encode as JSON numbers.
//
// String values encode as JSON strings, with each invalid UTF-8 sequence
// replaced by the encoding of the Unicode replacement character U+FFFD.
// The angle brackets "<" and ">" are escaped to "\u003c" and "\u003e"
// to keep some browsers from misinterpreting JSON output as HTML.
//
// Array and slice values encode as JSON arrays, except that
// []byte encodes as a base64-encoded string, and a nil slice
// encodes as the null JSON object.
//
// Struct values encode different from the normal encoding/json package!
// --------------------------------------------------------------------------
// Struct values encode as JSON objects. Each exported struct field
// becomes a member of the object unless
//   - the field's tag is a riak.Many, riak.One or riak.Model
//
// The object's default key string is the struct field name but can be
// specified in the struct field's tag value.
//
// The key name will be used if it's a non-empty string consisting of
// only Unicode letters, digits, dollar signs, percent signs, hyphens,
// underscores and slashes.
// --------------------------------------------------------------------------
//
// Map values encode as JSON objects.
// The map's key type must be string; the object keys are used directly
// as map keys.
//
// Pointer values encode as the value pointed to.
// A nil pointer encodes as the null JSON object.
//
// Interface values encode as the value contained in the interface.
// A nil interface value encodes as the null JSON object.
//
// Channel, complex, and function values cannot be encoded in JSON.
// Attempting to encode such a value causes Marshal to return
// an UnsupportedTypeError.
//
// JSON cannot represent cyclic data structures and Marshal does not
// handle them.  Passing cyclic structures to Marshal will result in
// an infinite recursion.
//
func Marshal(v interface{}) ([]byte, error) {
	e := &encodeState{}
	err := e.marshal(v)
	if err != nil {
		return nil, err
	}
	return e.Bytes(), nil
}

// MarshalIndent is like Marshal but applies Indent to format the output.
func MarshalIndent(v interface{}, prefix, indent string) ([]byte, error) {
	b, err := Marshal(v)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	err = Indent(&buf, b, prefix, indent)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// HTMLEscape appends to dst the JSON-encoded src with <, >, and &
// characters inside string literals changed to \u003c, \u003e, \u0026
// so that the JSON will be safe to embed inside HTML <script> tags.
// For historical reasons, web browsers don't honor standard HTML
// escaping within <script> tags, so an alternative JSON encoding must
// be used.
func HTMLEscape(dst *bytes.Buffer, src []byte) {
	// < > & can only appear in string literals,
	// so just scan the string one byte at a time.
	start := 0
	for i, c := range src {
		if c == '<' || c == '>' || c == '&' {
			if start < i {
				dst.Write(src[start:i])
			}
			dst.WriteString(`\u00`)
			dst.WriteByte(hex[c>>4])
			dst.WriteByte(hex[c&0xF])
			start = i + 1
		}
	}
	if start < len(src) {
		dst.Write(src[start:])
	}
}

// Marshaler is the interface implemented by objects that
// can marshal themselves into valid JSON.
type Marshaler interface {
	MarshalJSON() ([]byte, error)
}

// An UnsupportedTypeError is returned by Marshal when attempting
// to encode an unsupported value type.
type UnsupportedTypeError struct {
	Type reflect.Type
}

func (e *UnsupportedTypeError) Error() string {
	return "json: unsupported type: " + e.Type.String()
}

type UnsupportedValueError struct {
	Value reflect.Value
	Str   string
}

func (e *UnsupportedValueError) Error() string {
	return "json: unsupported value: " + e.Str
}

type InvalidUTF8Error struct {
	S string
}

func (e *InvalidUTF8Error) Error() string {
	return "json: invalid UTF-8 in string: " + strconv.Quote(e.S)
}

type MarshalerError struct {
	Type reflect.Type
	Err  error
}

func (e *MarshalerError) Error() string {
	return "json: error calling MarshalJSON for type " + e.Type.String() + ": " + e.Err.Error()
}

var hex = "0123456789abcdef"

// An encodeState encodes JSON into a bytes.Buffer.
type encodeState struct {
	bytes.Buffer // accumulated output
	scratch      [64]byte
}

func (e *encodeState) marshal(v interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
		}
	}()
	e.reflectValue(reflect.ValueOf(v))
	return nil
}

func (e *encodeState) error(err error) {
	panic(err)
}

var byteSliceType = reflect.TypeOf([]byte(nil))

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

func (e *encodeState) reflectValue(v reflect.Value) {
	e.reflectValueQuoted(v, false)
}

// reflectValueQuoted writes the value in v to the output.
// If quoted is true, the serialization is wrapped in a JSON string.
func (e *encodeState) reflectValueQuoted(v reflect.Value, quoted bool) {
	if !v.IsValid() {
		e.WriteString("null")
		return
	}

	m, ok := v.Interface().(Marshaler)
	if !ok {
		// T doesn't match the interface. Check against *T too.
		if v.Kind() != reflect.Ptr && v.CanAddr() {
			m, ok = v.Addr().Interface().(Marshaler)
			if ok {
				v = v.Addr()
			}
		}
	}
	if ok && (v.Kind() != reflect.Ptr || !v.IsNil()) {
		b, err := m.MarshalJSON()
		if err == nil {
			// copy JSON into buffer, checking validity.
			err = compact(&e.Buffer, b, true)
		}
		if err != nil {
			e.error(&MarshalerError{v.Type(), err})
		}
		return
	}

	writeString := (*encodeState).WriteString
	if quoted {
		writeString = (*encodeState).string
	}

	switch v.Kind() {
	case reflect.Bool:
		x := v.Bool()
		if x {
			writeString(e, "true")
		} else {
			writeString(e, "false")
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		b := strconv.AppendInt(e.scratch[:0], v.Int(), 10)
		if quoted {
			writeString(e, string(b))
		} else {
			e.Write(b)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		b := strconv.AppendUint(e.scratch[:0], v.Uint(), 10)
		if quoted {
			writeString(e, string(b))
		} else {
			e.Write(b)
		}
	case reflect.Float32, reflect.Float64:
		f := v.Float()
		if math.IsInf(f, 0) || math.IsNaN(f) {
			e.error(&UnsupportedValueError{v, strconv.FormatFloat(f, 'g', -1, v.Type().Bits())})
		}
		b := strconv.AppendFloat(e.scratch[:0], f, 'g', -1, v.Type().Bits())
		if quoted {
			writeString(e, string(b))
		} else {
			e.Write(b)
		}
	case reflect.String:
		if quoted {
			sb, err := Marshal(v.String())
			if err != nil {
				e.error(err)
			}
			e.string(string(sb))
		} else {
			e.string(v.String())
		}

	case reflect.Struct:
		// Special handling for riak - include a "_type" field with the name of the struct
		e.WriteString(`{"_type":"`)
		e.WriteString(reflect.TypeOf(v.Interface()).Name())
		e.WriteByte('"')
		first := false
		for _, ef := range encodeFields(v.Type()) {
			fieldValue := v.Field(ef.i)
			if ef.omitEmpty && isEmptyValue(fieldValue) {
				continue
			}
			if first {
				first = false
			} else {
				e.WriteByte(',')
			}
			e.string(ef.tag)
			e.WriteByte(':')
			e.reflectValueQuoted(fieldValue, ef.quoted)
		}
		e.WriteByte('}')

	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			e.error(&UnsupportedTypeError{v.Type()})
		}
		if v.IsNil() {
			e.WriteString("null")
			break
		}
		e.WriteByte('{')
		var sv stringValues = v.MapKeys()
		sort.Sort(sv)
		for i, k := range sv {
			if i > 0 {
				e.WriteByte(',')
			}
			e.string(k.String())
			e.WriteByte(':')
			e.reflectValue(v.MapIndex(k))
		}
		e.WriteByte('}')

	case reflect.Slice:
		if v.IsNil() {
			e.WriteString("null")
			break
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			// Byte slices get special treatment; arrays don't.
			s := v.Bytes()
			e.WriteByte('"')
			if len(s) < 1024 {
				// for small buffers, using Encode directly is much faster.
				dst := make([]byte, base64.StdEncoding.EncodedLen(len(s)))
				base64.StdEncoding.Encode(dst, s)
				e.Write(dst)
			} else {
				// for large buffers, avoid unnecessary extra temporary
				// buffer space.
				enc := base64.NewEncoder(base64.StdEncoding, e)
				enc.Write(s)
				enc.Close()
			}
			e.WriteByte('"')
			break
		}
		// Slices can be marshalled as nil, but otherwise are handled
		// as arrays.
		fallthrough
	case reflect.Array:
		e.WriteByte('[')
		n := v.Len()
		for i := 0; i < n; i++ {
			if i > 0 {
				e.WriteByte(',')
			}
			e.reflectValue(v.Index(i))
		}
		e.WriteByte(']')

	case reflect.Interface, reflect.Ptr:
		if v.IsNil() {
			e.WriteString("null")
			return
		}
		e.reflectValue(v.Elem())

	default:
		e.error(&UnsupportedTypeError{v.Type()})
	}
	return
}

func isValidTag(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		switch {
		case strings.ContainsRune("!#$%&()*+-./:<=>?@[]^_{|}~", c):
			// Backslash and quote chars are reserved, but
			// otherwise any punctuation chars are allowed
			// in a tag name.
		default:
			if !unicode.IsLetter(c) && !unicode.IsDigit(c) {
				return false
			}
		}
	}
	return true
}

// stringValues is a slice of reflect.Value holding *reflect.StringValue.
// It implements the methods to sort by string.
type stringValues []reflect.Value

func (sv stringValues) Len() int           { return len(sv) }
func (sv stringValues) Swap(i, j int)      { sv[i], sv[j] = sv[j], sv[i] }
func (sv stringValues) Less(i, j int) bool { return sv.get(i) < sv.get(j) }
func (sv stringValues) get(i int) string   { return sv[i].String() }

func (e *encodeState) string(s string) (int, error) {
	len0 := e.Len()
	e.WriteByte('"')
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if 0x20 <= b && b != '\\' && b != '"' && b != '<' && b != '>' {
				i++
				continue
			}
			if start < i {
				e.WriteString(s[start:i])
			}
			switch b {
			case '\\', '"':
				e.WriteByte('\\')
				e.WriteByte(b)
			case '\n':
				e.WriteByte('\\')
				e.WriteByte('n')
			case '\r':
				e.WriteByte('\\')
				e.WriteByte('r')
			default:
				// This encodes bytes < 0x20 except for \n and \r,
				// as well as < and >. The latter are escaped because they
				// can lead to security holes when user-controlled strings
				// are rendered into JSON and served to some browsers.
				e.WriteString(`\u00`)
				e.WriteByte(hex[b>>4])
				e.WriteByte(hex[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(s[i:])
		if c == utf8.RuneError && size == 1 {
			e.error(&InvalidUTF8Error{s})
		}
		i += size
	}
	if start < len(s) {
		e.WriteString(s[start:])
	}
	e.WriteByte('"')
	return e.Len() - len0, nil
}

// encodeField contains information about how to encode a field of a
// struct.
type encodeField struct {
	i         int // field index in struct
	tag       string
	quoted    bool
	omitEmpty bool
}

var (
	typeCacheLock     sync.RWMutex
	encodeFieldsCache = make(map[reflect.Type][]encodeField)
)

var SkipTypes = make(map[reflect.Type]bool)

// encodeFields returns a slice of encodeField for a given
// struct type.
func encodeFields(t reflect.Type) []encodeField {
	typeCacheLock.RLock()
	fs, ok := encodeFieldsCache[t]
	typeCacheLock.RUnlock()
	if ok {
		return fs
	}

	typeCacheLock.Lock()
	defer typeCacheLock.Unlock()
	fs, ok = encodeFieldsCache[t]
	if ok {
		return fs
	}

	v := reflect.Zero(t)
	n := v.NumField()
	for i := 0; i < n; i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}
		if f.Anonymous {
			// We want to do a better job with these later,
			// so for now pretend they don't exist.
			continue
		}
		// Some special exceptions for riak - skip these fields!
		if SkipTypes[f.Type] {
			continue
		}
		var ef encodeField
		ef.i = i
		ef.tag = f.Name

		// Handling of Tag is different for riak, not looking for a "json:" but for "riak:" instead.
		// (https://github.com/titanous was right after all, need to play nice and use the Go way)
		// Falling back to using the Tag directly until all code is updated (DEPRECATED)
		tv := f.Tag.Get("riak")
		//DEPRECATED: use tag directly if it appears not to have key/values.
		if tv == "" && !strings.Contains(string(f.Tag), `:"`) {
			tv = string(f.Tag)
		}
		if tv != "" {
			if tv == "-" {
				continue
			}
			if isValidTag(tv) {
				ef.tag = tv
			}
			ef.omitEmpty = true
			ef.quoted = false
		}
		fs = append(fs, ef)
	}
	encodeFieldsCache[t] = fs
	return fs
}

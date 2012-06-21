package riak

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

/*
	Make structs work like a Document Model, similar to how the Ruby
	based "ripple" gem works. This is done by parsing the JSON data and mapping
	it to the struct's fields. To enable easy integration with Ruby/ripple
	projects the struct "tag" feature of Go is used to possibly get around the
	naming convention differences between Go and Ruby (Uppercase starting letter
	required for export and typically CamelCase versus underscores).
	Also it stores the model/struct name as _type in Riak.

	For example the following Ruby/Ripple class:
	class Device
	  include Ripple::Document
	  property :ip, String
	  property :description, String
	  property :download_enabled, Boolean
	end

	can be mapped to the following Go class:
	type Device struct {
		Download_enabled bool    "download_enabled"
		Ip               string  "ip"
		Description      string  "description"
		RiakModel        riak.Model
	}

	Note that it is required to have a "RiakModel" field that is a riak.Model
	Also if the field name in Ripple is equal the extra tag is not needed, (e.g.
	if the Ripple class above would have a "property :Ip, String").
*/
type Model struct {
	robject RObject
}

func setval(source reflect.Value, dest reflect.Value) {
	switch dest.Kind() {
	case reflect.Bool:
		dest.SetBool(source.Bool())
	case reflect.String:
		dest.SetString(source.String())
	case reflect.Float32, reflect.Float64:
		dest.SetFloat(source.Float())
	case reflect.Int:
		dest.SetInt(source.Int())
	}
}

/*	
	The Get function retrieves the data from Riak and stores it in the struct
	that is passed as destination. It stores some necessary information in the
	RiakModel field so it can be used later in other (Save) operations.

	Unfortunately you also need to pass the bucketname as it is probably
	different from the struct name.

	Using the "Device" struct as an example:

	dev := &Device{}
	err := client.Get("devices", "12345", dev)
*/
func (c *Client) Get(bucketname string, key string, dest interface{}) (err error) {
	// Check if the passed destination is a pointer to a struct with RiakModel field
	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Ptr || dv.IsNil() {
		return errors.New("Destination is not a pointer (to a struct)")
	}
	dv = dv.Elem()
	dt := reflect.TypeOf(dest)
	dt = dt.Elem()
	if dt.Kind() != reflect.Struct {
		return errors.New("Destination is not a (pointer to a) struct")
	}
	dobj, exist := dt.FieldByName("RiakModel")
	if !exist {
		return errors.New("Destination has no RiakModel field")
	}
	if dobj.Type.Kind() != reflect.Struct {
		return errors.New("Destination RiakModel field is not a Model struct")
	}
	if dobj.Type.Name() != "Model" {
		return errors.New("Destination RiakModel field is not a Model")
	}
	// Fetch the object from Riak.
	bucket := c.Bucket(bucketname)
	if bucket == nil {
		return fmt.Errorf("Can't get bucket for %v", dt.Name())
	}
	obj, err := bucket.Get(key)
	if err != nil {
		return err
	}
	if obj == nil {
		return errors.New("Object not found")
	}
	// Map the data onto the struct, first get it into a map
	var data map[string]interface{}
	err = json.Unmarshal(obj.Data, &data)
	if err != nil {
		return err
	}
	// Double check there is a "_field" type that is the same as the struct
	// name, this is only a warning though.
	if dt.Name() != data["_type"] {
		err = fmt.Errorf("Warning: struct name does not match _type in Riak")
	}
	// For all fields in the struct, find the correct data / mapping
	for i := 0; i < dt.NumField(); i++ {
		ft := dt.Field(i)
		fv := dv.Field(i)
		if data[ft.Name] != nil {
			if ft.Type == reflect.TypeOf(data[ft.Name]) {
				setval(reflect.ValueOf(data[ft.Name]), fv)
			}
		} else if data[string(ft.Tag)] != nil {
			if ft.Type == reflect.TypeOf(data[string(ft.Tag)]) {
				setval(reflect.ValueOf(data[string(ft.Tag)]), fv)
			}
		}
	}
	return
}

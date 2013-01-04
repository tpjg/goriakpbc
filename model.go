package riak

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

/*
Make structs work like a Document Model, similar to how the Ruby based "ripple" 
gem works. This is done by parsing the JSON data and mapping it to the struct's 
fields. To enable easy integration with Ruby/ripple projects the struct "tag" 
feature of Go is used to possibly get around the naming convention differences 
between Go and Ruby (Uppercase starting letter required for export and t
ypically CamelCase versus underscores). Also it stores the model/struct name 
as _type in Riak.

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
    riak.Model
}

Note that it is required to have a riak.Model field.
Also if the field name in Ripple is equal the extra tag is not needed, (e.g.
if the Ripple class above would have a "property :Ip, String").
*/
type Model struct {
	robject *RObject
	parent  Resolver // Pointer to the parent struct (Device in example above)
}

type Resolver interface {
	Resolve(int) error
}

// Error definitions
var (
	ResolveNotImplemented     = errors.New("Resolve not implemented")
	DestinationError          = errors.New("Destination is not a pointer (to a struct)")
	DestinationIsNotModel     = errors.New("Destination has no riak.Model field")
	DestinationIsNotSlice     = errors.New("Must supply a slice to GetSiblings")
	DestinationLengthError    = errors.New("Length of slice does not match number of siblings")
	DestinationNotInitialized = errors.New("Destination struct is not initialized (correctly) using riak.New or riak.Load")
	ModelDoesNotMatch         = errors.New("Warning: struct name does not match _type in Riak")
	ModelNotNew               = errors.New("Destination struct already has an instantiated riak.Model (this struct is probably not new)")
	NoSiblingData             = errors.New("No non-empty sibling data")
)

/*
Return is an error is really a warning, e.g. a common json error, or 
ModelDoesNotMatch.
*/
func IsWarning(err error) bool {
	if err != nil {
		if strings.HasPrefix(err.Error(), "<nil> - json: cannot unmarshal") && strings.Contains(err.Error(), "into Go value of type") {
			return true
		} else if err == ModelDoesNotMatch {
			return true
		}
	} else {
		// In case there is no error reply true anyway since this is probably 
		// what is expected - a check whether it is safe to continue.
		return true
	}
	return false
}

func (*Model) Resolve(count int) (err error) {
	return ResolveNotImplemented
}

// Link to one other model
type One struct {
	model  interface{}
	link   Link
	client *Client
}

// Link to many other models
type Many []One

func (c *Client) setOneLink(source Link, dest reflect.Value) {
	if dest.Kind() == reflect.Struct && dest.Type() == reflect.TypeOf(One{}) {
		one := One{link: source, client: c}
		mv := reflect.ValueOf(one)
		dest.Set(mv)
		return
	}
}

func (c *Client) addOneLink(source Link, dest reflect.Value) {
	if dest.Kind() == reflect.Slice && dest.Type() == reflect.TypeOf(Many{}) {
		one := One{link: source, client: c}
		mv := reflect.ValueOf(one)
		// Add this One link to the slice
		dest.Set(reflect.Append(dest, mv))
	}
}

// Check if the passed destination is a pointer to a struct with riak.Model field
func check_dest(dest interface{}) (dv reflect.Value, dt reflect.Type, rm reflect.Value, err error) {
	dv = reflect.ValueOf(dest)
	if dv.Kind() != reflect.Ptr || dv.IsNil() {
		err = DestinationError
		return
	}
	dv = dv.Elem()
	dt = reflect.TypeOf(dest)
	dt = dt.Elem()
	if dt.Kind() != reflect.Struct {
		err = DestinationError
		return
	}
	for i := 0; i < dt.NumField(); i++ {
		dobj := dt.Field(i)
		if dobj.Type.Kind() == reflect.Struct && dobj.Type == reflect.TypeOf(Model{}) {
			rm = dv.Field(i) // Return the Model field value
			return
		}
	}
	err = DestinationIsNotModel
	return
}

type ModelName struct {
	XXXXModelNameXXXX string "_type"
}

/*
	mapData, maps the decoded JSON data onto the right struct fields, including
	decoding of links.
*/
func (c *Client) mapData(dv reflect.Value, dt reflect.Type, data []byte, links []Link, dest interface{}) (err error) {
	// Double check there is a "_type" field that is the same as the struct
	// name, this is only a warning though.
	var mn ModelName
	err = Unmarshal(data, &mn)
	if err != nil || dt.Name() != mn.XXXXModelNameXXXX {
		err = fmt.Errorf("Warning: struct name does not match _type in Riak (%v)", err)
	}
	// Unmarshal the destination model
	jserr := Unmarshal(data, dest)
	if jserr != nil {
		err = fmt.Errorf("%v - %v", err, jserr) // Add error
	}
	// For all the links in the struct, find the correct mapping
	for i := 0; i < dt.NumField(); i++ {
		ft := dt.Field(i)
		fv := dv.Field(i)
		if ft.Type == reflect.TypeOf(One{}) {
			var name string
			if ft.Tag != "" {
				name = string(ft.Tag)
			} else {
				name = ft.Name
			}
			// Search in Links
			for _, v := range links {
				if v.Tag == name {
					c.setOneLink(v, fv)
				}
			}
		} else if ft.Type == reflect.TypeOf(Many{}) {
			var name string
			if ft.Tag != "" {
				name = string(ft.Tag)
			} else {
				name = ft.Name
			}
			// Search in Links
			for _, v := range links {
				if v.Tag == name {
					c.addOneLink(v, fv)
				}
			}
		}
	}
	return
}

func (m *Model) GetSiblings(dest interface{}) (err error) {
	client, err := m.getClient()
	if err != nil {
		return err
	}
	// Check if a slice is supplied
	v := reflect.ValueOf(dest)
	if v.Kind() == reflect.Ptr && !v.IsNil() {
		v = v.Elem()
	}
	if v.Kind() != reflect.Slice {
		return DestinationIsNotSlice
	}
	// Check the length of the supplied slice against the number of non-empty siblings
	count := 0
	for _, s := range m.robject.Siblings {
		if len(s.Data) != 0 {
			count += 1
		}
	}
	if count == 0 {
		return NoSiblingData
	}
	if v.Len() != count {
		return DestinationLengthError
	}
	// Check the type of the slice elements
	if reflect.TypeOf(v.Index(0).Interface()) != reflect.TypeOf(m.parent).Elem() {
		return fmt.Errorf("Slice elements incorrect, must be %v", reflect.TypeOf(m.parent).Elem())
	}
	count = 0
	// Walk over the slice and map the data for each sibling
	for _, sibling := range m.robject.Siblings {
		if len(sibling.Data) != 0 {
			// Map the data onto the parent struct
			err = client.mapData(v.Index(count), reflect.TypeOf(m.parent).Elem(), sibling.Data, sibling.Links, m.parent)
			// Copy the parent struct to the slice element
			v.Index(count).Set(reflect.ValueOf(m.parent).Elem())
			count += 1
		}
	}
	return
}

/*	
	The Load function retrieves the data from Riak and stores it in the struct
	that is passed as destination. It stores some necessary information in the
	riak.Model field so it can be used later in other (Save) operations.

	Unfortunately you also need to pass the bucketname as it is probably
	different from the struct name.

	Using the "Device" struct as an example:

	dev := &Device{}
	err := client.Load("devices", "12345", dev)
*/
func (c *Client) Load(bucketname string, key string, dest Resolver, options ...map[string]uint32) (err error) {
	// Check destination
	dv, dt, rm, err := check_dest(dest)
	if err != nil {
		return err
	}
	// Fetch the object from Riak.
	bucket, err := c.Bucket(bucketname)
	if bucket == nil || err != nil {
		err = fmt.Errorf("Can't get bucket for %v - %v", dt.Name(), err)
		return
	}
	obj, err := bucket.Get(key, options...)
	if err != nil {
		return err
	}
	if obj == nil {
		return NotFound
	}
	if obj.Conflict() {
		// Count number of non-empty siblings for which a conflict must be resolved
		count := 0
		for _, s := range obj.Siblings {
			if len(s.Data) != 0 {
				count += 1
			}
		}
		// Set the RObject in the destination struct so it can be used for resolving the conflict
		model := &Model{robject: obj, parent: dest}
		mv := reflect.ValueOf(model)
		mv = mv.Elem()
		rm.Set(mv)
		// Resolve the conflict and return the errorcode
		return dest.Resolve(count)
	}
	// Map the data onto the struct.
	err = c.mapData(dv, dt, obj.Data, obj.Links, dest)

	// Set the values in the riak.Model field
	model := &Model{robject: obj, parent: dest}
	mv := reflect.ValueOf(model)
	mv = mv.Elem()
	rm.Set(mv)

	return
}

/*
Create a new Document Model, passing in the bucketname and key. The key can be 
empty in which case Riak will pick a key. The destination must be a pointer to
a struct that has the riak.Model field.
*/
func (c *Client) New(bucketname string, key string, dest Resolver, options ...map[string]uint32) (err error) {
	// Check destination
	_, dt, rm, err := check_dest(dest)
	if err != nil {
		return err
	}
	// Fetch the object from Riak.
	bucket, err := c.Bucket(bucketname)
	if bucket == nil || err != nil {
		err = fmt.Errorf("Can't get bucket for %v - %v", dt.Name(), err)
		return
	}
	// Check if the RObject field within riak.Model is still nill, otherwise
	// this destination (dest) is probably an already fully instantiated 
	// struct.
	model := &Model{}
	mv := reflect.ValueOf(model)
	mv = mv.Elem()
	mv.Set(rm)
	if model.robject != nil {
		return ModelNotNew
	}
	// For the riak.Model field within the struct, set the Client and Bucket 
	// and fields and set the RObject field to nil.
	model.robject = &RObject{Bucket: bucket, Key: key, ContentType: "application/json", Options: options}
	model.parent = dest
	rm.Set(mv)

	return
}

// Creates a link to a given model
func (c *Client) linkToModel(obj *RObject, dest interface{}, tag string) (err error) {
	// Check destination
	_, _, rm, err := check_dest(dest)
	if err != nil {
		return err
	}
	// Get the Model field
	model := &Model{}
	mv := reflect.ValueOf(model)
	mv = mv.Elem()
	mv.Set(rm)
	// Now check if there is an RObject, otherwise probably not correctly instantiated with .New (or Load).
	if model.robject == nil {
		return DestinationNotInitialized
	}
	obj.LinkTo(model.robject, tag)
	return nil
}

// Save a Document Model to Riak under a new key, if empty a Key will be choosen by Riak
func (c *Client) SaveAs(newKey string, dest Resolver) (err error) {
	// Check destination
	dv, dt, rm, err := check_dest(dest)
	if err != nil {
		return err
	}
	// Get the Model field
	model := &Model{}
	mv := reflect.ValueOf(model)
	mv = mv.Elem()
	mv.Set(rm)
	// Now check if there is an RObject, otherwise probably not correctly instantiated with .New (or Load).
	if model.robject == nil {
		return DestinationNotInitialized
	}
	// JSON encode the entire struct
	data, err := Marshal(dest)
	if err != nil {
		return err
	}
	// Now add the Links
	for i := 0; i < dt.NumField(); i++ {
		ft := dt.Field(i)
		fv := dv.Field(i)
		var fieldname string
		if ft.Tag != "" {
			fieldname = string(ft.Tag)
		} else {
			fieldname = ft.Name
		}
		if ft.Type == reflect.TypeOf(One{}) {
			// Save a link, set the One struct first
			lmodel := &One{}
			lmv := reflect.ValueOf(lmodel)
			lmv = lmv.Elem()
			lmv.Set(fv)
			// Now use that to link with the given tag or the name
			c.linkToModel(model.robject, lmodel.model, fieldname)
		}
		if ft.Type == reflect.TypeOf(Many{}) {
			// Save the links, create a Many struct first
			lmodels := &Many{}
			lmv := reflect.ValueOf(lmodels)
			lmv = lmv.Elem()
			lmv.Set(fv)
			// Now walk over those links...
			for _, lmodel := range *lmodels {
				c.linkToModel(model.robject, lmodel.model, fieldname)
			}
		}
	}
	//fmt.Printf("Saving data for %v as %v\n", dt.Name(), string(data))
	model.robject.Data = data
	if newKey != "±___unchanged___±" {
		model.robject.Key = newKey
	}
	// Store the RObject in Riak
	err = model.robject.Store()

	return
}

// Save a Document Model to Riak
func (c *Client) Save(dest Resolver) (err error) {
	return c.SaveAs("±___unchanged___±", dest)
}

// Get the client from a given model
func (m *Model) getClient() (c *Client, err error) {
	if m.robject == nil {
		return nil, DestinationNotInitialized
	}
	if m.robject.Bucket == nil {
		return nil, DestinationNotInitialized
	}
	if m.robject.Bucket.client == nil {
		return nil, DestinationNotInitialized
	}
	return m.robject.Bucket.client, nil
}

// Save a Document Model to Riak under a new key, if empty a Key will be choosen by Riak
func (m *Model) SaveAs(newKey string) (err error) {
	client, err := m.getClient()
	if err != nil {
		return err
	}
	return client.SaveAs(newKey, m.parent)
}

// Save a Document Model to Riak
func (m *Model) Save() (err error) {
	return m.SaveAs("±___unchanged___±")
}

// Get a models Key, e.g. needed when Riak has picked it
func (c *Client) Key(dest interface{}) (key string, err error) {
	// Check destination
	_, _, rm, err := check_dest(dest)
	if err != nil {
		return
	}
	// Get the Model field
	model := &Model{}
	mv := reflect.ValueOf(model)
	mv = mv.Elem()
	mv.Set(rm)
	// Now check if there is an RObject, otherwise probably not correctly instantiated with .New (or Load).
	if model.robject == nil {
		err = errors.New("Destination struct is not instantiated using riak.New or riak.Load")
		return
	}
	return model.robject.Key, nil
}

// Get a models Key, e.g. needed when Riak has picked it
func (m Model) Key() (key string) {
	if m.robject == nil {
		return ""
	}
	return m.robject.Key
}

// Set the Key value, note that this does not save the model, it only changes the data structure
func (c *Client) SetKey(newKey string, dest interface{}) (err error) {
	// Check destination
	_, _, rm, err := check_dest(dest)
	if err != nil {
		return
	}
	// Get the Model field
	model := &Model{}
	mv := reflect.ValueOf(model)
	mv = mv.Elem()
	mv.Set(rm)
	// Now check if there is an RObject, otherwise probably not correctly instantiated with .New (or Load).
	if model.robject == nil {
		return DestinationNotInitialized
	}
	model.robject.Key = newKey
	return
}

// Set the Key value, note that this does not save the model, it only changes the data structure
func (m Model) SetKey(newKey string) (err error) {
	if m.robject == nil {
		return DestinationNotInitialized
	}
	m.robject.Key = newKey
	return
}

func (o One) Link() (link Link) {
	return o.link
}

func (o *One) Set(dest interface{}) (err error) {
	_, _, _, err = check_dest(dest)
	if err == nil {
		o.model = dest
	}
	return
}

func (o *One) Get(dest Resolver) (err error) {
	if o.client == nil {
		return DestinationNotInitialized
	}
	return o.client.Load(o.link.Bucket, o.link.Key, dest)
}

func (m *Many) Add(dest interface{}) (err error) {
	_, _, _, err = check_dest(dest)
	if err == nil {
		*m = append(*m, One{model: dest})
	}
	return err
}

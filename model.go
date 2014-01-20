package riak

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/estebarb/goriakpbc/json"
)

/*
Make structs work like a Document Model, similar to how the Ruby based "ripple"
gem works. This is done by parsing the JSON data and mapping it to the struct's
fields. To enable easy integration with Ruby/ripple projects the struct "tag"
feature of Go is used to possibly get around the naming convention differences
between Go and Ruby (Uppercase starting letter required for export and
typically CamelCase versus underscores). Also it stores the model/struct name
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
		riak.Model       `riak:devices`
		Download_enabled bool    `riak:"download_enabled"`
		Ip               string  `riak:"ip"`
		Description      string  `riak:"description"`
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
		} else if strings.HasPrefix(err.Error(), "<nil> - parsing") && strings.Contains(err.Error(), ": cannot parse") {
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

// Sets up a riak.Model structure and assigns it to the given resolver field.
func setup_model(obj *RObject, dest Resolver, rm reflect.Value) {
	model := Model{robject: obj, parent: dest}
	mv := reflect.ValueOf(model)
	rm.Set(mv)
}

// Check if the passed destination is a pointer to a struct with riak.Model field
// Returns the destination Value and Type (dv, dt) as well as the riak.Model field (rm)
// and the bucketname (bn), which is derived from the tag of the riak.Model field.
func check_dest(dest interface{}) (dv reflect.Value, dt reflect.Type, rm reflect.Value, bn string, err error) {
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
			bn = dt.Field(i).Tag.Get("riak")
			rm = dv.Field(i) // Return the Model field value
			return
		}
	}
	err = DestinationIsNotModel
	return
}

type modelName struct {
	Type string `_type`
}

/*
	mapData, maps the decoded JSON data onto the right struct fields, including
	decoding of links.
*/
func (c *Client) mapData(dv reflect.Value, dt reflect.Type, data []byte, links []Link, dest interface{}) (err error) {
	// Double check there is a "_type" field that is the same as the struct
	// name, this is only a warning though.
	var mn modelName
	err = json.Unmarshal(data, &mn)
	if err != nil || dt.Name() != mn.Type {
		err = fmt.Errorf("Warning: struct name does not match _type in Riak (%v)", err)
	}
	// Unmarshal the destination model
	jserr := json.Unmarshal(data, dest)
	if jserr != nil {
		err = fmt.Errorf("%v - %v", err, jserr) // Add error
	}
	// For all the links in the struct, find the correct mapping
	for i := 0; i < dt.NumField(); i++ {
		ft := dt.Field(i)
		fv := dv.Field(i)
		if ft.Type == reflect.TypeOf(One{}) {
			var name string
			if ft.Tag.Get("riak") != "" {
				name = ft.Tag.Get("riak")
			} else if string(ft.Tag) != "" && !strings.Contains(string(ft.Tag), `:"`) {
				//DEPRECATED: use tag directly if it appears not to have key/values.
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
			if ft.Tag.Get("riak") != "" {
				name = ft.Tag.Get("riak")
			} else if string(ft.Tag) != "" && !strings.Contains(string(ft.Tag), `:"`) {
				//DEPRECATED: use tag directly if it appears not to have key/values.
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
	// Double check the parent and get the Value and Type
	_, dt, _, _, err := check_dest(m.parent)
	if err != nil {
		return err
	}
	// Walk over the slice and map the data for each sibling
	for _, sibling := range m.robject.Siblings {
		if len(sibling.Data) != 0 {
			// Map the data onto a temporary struct
			tmp := reflect.New(dt)
			err = client.mapData(tmp.Elem(), dt, sibling.Data, sibling.Links, tmp.Interface())
			// Copy the temporary struct to the slice element
			v.Index(count).Set(tmp.Elem())
			count += 1
		}
	}
	return
}

/*
The LoadModelFrom function retrieves the data from Riak and stores it in the struct
that is passed as destination. It stores some necessary information in the
riak.Model field so it can be used later in other (Save) operations.

If the bucketname is empty ("") it'll be the default bucket, based on the
riak.Model tag.

Using the "Device" struct as an example:

dev := &Device{}
err := client.Load("devices", "12345", dev)
*/
func (c *Client) LoadModelFrom(bucketname string, key string, dest Resolver, options ...map[string]uint32) (err error) {
	// Check destination
	dv, dt, rm, bn, err := check_dest(dest)
	if err != nil {
		return err
	}
	// Use default bucket name if empty
	if bucketname == "" {
		bucketname = bn
	}
	// Fetch the object from Riak.
	bucket, err := c.Bucket(bucketname)
	if bucket == nil || err != nil {
		err = fmt.Errorf("Can't get bucket for %v - %v", dt.Name(), err)
		return
	}
	obj, err := bucket.Get(key, options...)
	if err != nil {
		if obj != nil {
			// Set the values in the riak.Model field
			setup_model(obj, dest, rm)
		}
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
		setup_model(obj, dest, rm)
		// Resolve the conflict and return the errorcode
		return dest.Resolve(count)
	}
	// Map the data onto the struct.
	err = c.mapData(dv, dt, obj.Data, obj.Links, dest)

	// Set the values in the riak.Model field
	setup_model(obj, dest, rm)

	return
}

// Load data into model. DEPRECATED, use LoadModelFrom instead.
func (c *Client) Load(bucketname string, key string, dest Resolver, options ...map[string]uint32) (err error) {
	return c.LoadModelFrom(bucketname, key, dest, options...)
}

// Load data into the model using the default bucket (from the Model's struct definition)
func (c *Client) LoadModel(key string, dest Resolver, options ...map[string]uint32) (err error) {
	return c.LoadModelFrom("", key, dest, options...)
}

/*
Create a new Document Model, passing in the bucketname and key. The key can be
empty in which case Riak will pick a key. The destination must be a pointer to
a struct that has the riak.Model field.
If the bucketname is empty the default bucketname, based on the riak.Model tag
will be used.
*/
func (c *Client) NewModelIn(bucketname string, key string, dest Resolver, options ...map[string]uint32) (err error) {
	// Check destination
	_, dt, rm, bn, err := check_dest(dest)
	if err != nil {
		return err
	}
	// Use default bucket name if empty
	if bucketname == "" {
		bucketname = bn
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

// Instantiate a new model, setting the necessary fields, like the client.
// If key is not empty that key will be used, otherwise Riak will choose a key.
func (c *Client) NewModel(key string, dest Resolver, options ...map[string]uint32) (err error) {
	return c.NewModelIn("", key, dest, options...)
}

// Create a new model. DEPRECATED, use NewModelIn instead.
func (c *Client) New(bucketname string, key string, dest Resolver, options ...map[string]uint32) (err error) {
	return c.NewModelIn(bucketname, key, dest, options...)
}

// Creates a link to a given model
func (c *Client) linkToModel(dest interface{}) (link Link, err error) {
	// Check destination
	_, _, rm, _, err := check_dest(dest)
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
		err = DestinationNotInitialized
		return
	}
	link = Link{Bucket: model.robject.Bucket.name, Key: model.robject.Key}
	return
}

// Save a Document Model to Riak under a new key, if empty a Key will be choosen by Riak
func (c *Client) SaveAs(newKey string, dest Resolver) (err error) {
	// Check destination
	dv, dt, rm, _, err := check_dest(dest)
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
	data, err := json.Marshal(dest)
	if err != nil {
		return err
	}
	// Clear the old links
	model.robject.Links = []Link{}
	// Now add the Links
	for i := 0; i < dt.NumField(); i++ {
		ft := dt.Field(i)
		fv := dv.Field(i)
		var fieldname string
		if ft.Tag.Get("riak") != "" {
			fieldname = ft.Tag.Get("riak")
		} else if string(ft.Tag) != "" && !strings.Contains(string(ft.Tag), `:"`) {
			//DEPRECATED: use tag directly if it appears not to have key/values.
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
			// If the link is not set, create it now.
			if lmodel.link.Bucket == "" || lmodel.link.Key == "" {
				lmodel.link, _ = c.linkToModel(lmodel.model)
			}
			// Add the link (if not already in the object's links)
			model.robject.AddLink(Link{lmodel.link.Bucket, lmodel.link.Key, fieldname})
		}
		if ft.Type == reflect.TypeOf(Many{}) {
			// Save the links, create a Many struct first
			lmodels := &Many{}
			lmv := reflect.ValueOf(lmodels)
			lmv = lmv.Elem()
			lmv.Set(fv)
			// Now walk over those links...
			for _, lmodel := range *lmodels {
				// If the link is not set, create it now.
				if lmodel.link.Bucket == "" || lmodel.link.Key == "" {
					lmodel.link, _ = c.linkToModel(lmodel.model)
				}
				// Add the link (if not already in the object's links)
				model.robject.AddLink(Link{lmodel.link.Bucket, lmodel.link.Key, fieldname})
			}
		}
	}
	//fmt.Printf("Saving data for %v as %v\n", dt.Name(), string(data))
	model.robject.Data = data
	model.robject.ContentType = "application/json"
	if newKey != "" {
		model.robject.Key = newKey
	}
	// Store the RObject in Riak
	err = model.robject.Store()

	return
}

// Save a Document Model to Riak
func (c *Client) Save(dest Resolver) (err error) {
	return c.SaveAs("", dest)
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
	return m.SaveAs("")
}

// Delete a Document Model
func (m *Model) Delete() (err error) {
	return m.robject.Destroy()
}

// Reload a Document Model
func (m *Model) Reload() (err error) {
	vclock := string(m.robject.Vclock)
	err = m.robject.Reload()
	if err != nil {
		return err
	}
	// Check if there was any change
	if string(m.robject.Vclock) != vclock {
		// Set the content again
		if m.robject.Conflict() {
			// Count number of non-empty siblings for which a conflict must be resolved
			count := 0
			for _, s := range m.robject.Siblings {
				if len(s.Data) != 0 {
					count += 1
				}
			}
			// Resolve the conflict and return the errorcode
			return m.parent.Resolve(count)
		}
		// Map the data onto the struct.
		dv, dt, _, _, err := check_dest(m.parent)
		c, err := m.getClient()
		if err != nil {
			return err
		}
		err = c.mapData(dv, dt, m.robject.Data, m.robject.Links, m.parent)
		if err != nil {
			return err
		}
	}
	return
}

// Return the object Vclock - this allows an application to detect whether Reload()
// loaded a newer version of the object
func (m *Model) Vclock() (vclock []byte) {
	return m.robject.Vclock
}

// Return the object's indexes.  This allows an application to set custom secondary
// indexes on the object for later querying.
func (m *Model) MultiIndexes() map[string][]string {
	if m.robject.MultiIndexes == nil {
		m.robject.MultiIndexes = make(map[string][]string)
	}
	return m.robject.MultiIndexes
}

// Get a models Key, e.g. needed when Riak has picked it
func (c *Client) Key(dest interface{}) (key string, err error) {
	// Check destination
	_, _, rm, _, err := check_dest(dest)
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
	_, _, rm, _, err := check_dest(dest)
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

// Set the link to a given Model (dest)
func (o *One) Set(dest Resolver) (err error) {
	_, _, _, _, err = check_dest(dest)
	if err == nil {
		o.model = dest
		o.link, err = o.client.linkToModel(dest)
	}
	return
}

// Set a link directly
func (o *One) SetLink(a One) {
	o.link = a.link
}

// Test for equality (bucket and key are equal)
func (o *One) Equal(e One) bool {
	return o.link.Bucket == e.link.Bucket && o.link.Key == o.link.Key
}

// Test if the link is empty (not set)
func (o *One) Empty() bool {
	return o.link.Bucket == "" && o.link.Key == ""
}

func (o *One) Get(dest Resolver) (err error) {
	if o.client == nil {
		return DestinationNotInitialized
	}
	return o.client.Load(o.link.Bucket, o.link.Key, dest)
}

// Add a Link to the given Model (dest)
func (m *Many) Add(dest Resolver) (err error) {
	_, _, _, _, err = check_dest(dest)
	if err == nil {
		o := One{model: dest}
		o.link, err = o.client.linkToModel(dest)
		*m = append(*m, o)
	}
	return err
}

// Add a given Link (One) directly
func (m *Many) AddLink(o One) {
	*m = append(*m, o)
}

// Remove a Link to the given Model (dest)
func (m *Many) Remove(dest Resolver) (err error) {
	_, _, _, _, err = check_dest(dest)
	if err == nil {
		o := One{model: dest}
		o.link, err = o.client.linkToModel(dest)
		for i, v := range *m {
			if v.link.Bucket == o.link.Bucket && v.link.Key == o.link.Key {
				// Remove this element from the list
				*m = append((*m)[:i], (*m)[i+1:]...)
				return err
			}
		}
	} else {
		return err
	}
	return NotFound
}

// Remove a given Link (One) directly, e.g. so it can be used when iterating over a riak.Many slice
func (m *Many) RemoveLink(o One) (err error) {
	for i, v := range *m {
		if v.link.Bucket == o.link.Bucket && v.link.Key == o.link.Key {
			// Remove this element from the list
			*m = append((*m)[:i], (*m)[i+1:]...)
			return err
		}
	}
	return NotFound
}

// Return the number of Links
func (m *Many) Len() int {
	return len(*m)
}

// Return if a given Link is in the riak.Many slice
func (m *Many) Contains(o One) bool {
	for _, v := range *m {
		if v.Equal(o) {
			return true
		}
	}
	return false
}

// Instantiate a new model (using the default client), setting the necessary fields, like the client.
// If key is not empty that key will be used, otherwise Riak will choose a key.
func NewModel(key string, dest Resolver, options ...map[string]uint32) (err error) {
	if defaultClient == nil {
		return NoDefaultClientConnection
	}
	return defaultClient.NewModelIn("", key, dest, options...)
}

// Create a new Document Model (using the default client), passing in the bucketname and key.
func NewModelIn(bucketname string, key string, dest Resolver, options ...map[string]uint32) (err error) {
	if defaultClient == nil {
		return NoDefaultClientConnection
	}
	return defaultClient.NewModelIn(bucketname, key, dest, options...)
}

// Load data into the model using the default bucket (from the Model's struct definition)
func LoadModel(key string, dest Resolver, options ...map[string]uint32) (err error) {
	if defaultClient == nil {
		return NoDefaultClientConnection
	}
	return defaultClient.LoadModel(key, dest, options...)
}

// The LoadModelFrom function retrieves the data from Riak using the default client and stores it in the struct
// that is passed as destination.
func LoadModelFrom(bucketname string, key string, dest Resolver, options ...map[string]uint32) (err error) {
	if defaultClient == nil {
		return NoDefaultClientConnection
	}
	return defaultClient.LoadModelFrom(bucketname, key, dest, options...)
}

func init() {
	json.SkipTypes[reflect.TypeOf(Model{})] = true
	json.SkipTypes[reflect.TypeOf(One{})] = true
	json.SkipTypes[reflect.TypeOf(Many{})] = true
}

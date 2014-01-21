package riak

import (
	"errors"
	"github.com/bmizerany/assert"
	"strconv"
	"strings"
	"testing"
	"time"
)

type DocumentModel struct {
	FieldS string  `riak:"string_field"`
	FieldF float64 `riak:"float_field"`
	FieldB bool
	Model  `riak:"testmodeldefault.go"`
}

func TestModel(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create a new "DocumentModel" and save it
	doc := DocumentModel{FieldS: "text", FieldF: 1.2, FieldB: true}
	err := client.New("testmodel.go", "TestModelKey", &doc)
	assert.T(t, err == nil)
	err = doc.Save()
	assert.T(t, err == nil)
	// Check that the JSON is correct
	t.Logf(string(doc.robject.Data))
	assert.T(t, `{"_type":"DocumentModel","string_field":"text","float_field":1.2,"FieldB":true}` == string(doc.robject.Data))

	// Load it from Riak and check that the fields of the DocumentModel struct are set correctly
	doc2 := DocumentModel{}
	err = client.Load("testmodel.go", "TestModelKey", &doc2)
	assert.T(t, err == nil)
	assert.T(t, doc2.FieldS == doc.FieldS)
	assert.T(t, doc2.FieldF == doc.FieldF)
	assert.T(t, doc2.FieldB == doc.FieldB)

	// Cleanup
	err = doc2.Delete()
	assert.T(t, err == nil)

	// Get the key
	key, err := client.Key(&doc2)
	assert.T(t, err == nil)
	assert.T(t, key == "TestModelKey")
	// Set it differently
	err = client.SetKey("newTestModelKey", &doc2)
	assert.T(t, err == nil)
	// And test that it changed by getting it again
	key, err = client.Key(&doc2)
	assert.T(t, err == nil)
	assert.T(t, key == "newTestModelKey")
	// Test setting it
	err = doc2.SetKey("newKeyAgain")
	assert.T(t, err == nil)
	assert.T(t, doc2.Key() == "newKeyAgain")

	// Test Delete(), so test if the cleanup worked
	doc3 := DocumentModel{}
	err = client.Load("testmodel.go", "TestModelKey", &doc3)
	assert.T(t, err == NotFound)

	// Immediately get the same object again, should have a valid Vclock (tombstone)
	doc4 := DocumentModel{}
	err = client.Load("testmodel.go", "TestModelKey", &doc4)
	assert.T(t, doc4.Model.robject != nil)
	assert.T(t, err == NotFound)
}

type DocumentModelWithLinks struct {
	FieldS string
	ALink  One `riak:"tag_as_parent"`
	BLink  One // Will automatically use own name as a tag when linking
	Model
}

func TestModelWithLinks(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create a new "DocumentModel" to use as a parent and save it
	parent := DocumentModel{FieldS: "text", FieldF: 1.2, FieldB: true}
	err := client.New("testmodel.go", "TestModelKey", &parent)
	assert.T(t, err == nil)
	//err = client.Save(&doc)
	err = parent.Save()
	assert.T(t, err == nil)

	// Create a new DocumentModelWithLinks and save it, adding a link to the parent
	doc := DocumentModelWithLinks{FieldS: "textinlinked", ALink: One{model: &parent}}
	// Test emptyness
	assert.T(t, doc.BLink.Empty())
	err = doc.BLink.Set(&parent) // testing One.Set while we're at it
	assert.T(t, err == nil)
	err = client.New("testmodellinks.go", "TestModelKey", &doc)
	assert.T(t, err == nil)
	err = doc.Save()
	assert.T(t, err == nil)

	// Load it from Riak and check that the fields of the struct are set correctly, including the link to the parent
	doc2 := DocumentModelWithLinks{}
	err = client.Load("testmodellinks.go", "TestModelKey", &doc2)
	assert.T(t, err == nil)
	assert.T(t, doc2.FieldS == doc.FieldS)
	assert.T(t, doc2.ALink.model == nil) // Related documents are not loaded automatically, only the link is populated
	assert.T(t, doc2.ALink.link.Tag == "tag_as_parent")
	assert.T(t, doc2.BLink.link.Tag == "BLink")
	t.Logf("Testing DocumentModelWithLinks - One - %v - %v\n", doc2.ALink.model, doc2.ALink.link)
	t.Logf("Testing DocumentModelWithLinks - One - %v - %v\n", doc2.BLink.model, doc2.BLink.link)

	// Load the parent from the link
	parent2 := DocumentModel{}
	err = doc2.ALink.Get(&parent2)
	assert.T(t, err == nil)
	assert.T(t, parent.FieldS == parent2.FieldS)
	assert.T(t, parent.FieldF == parent2.FieldF)
	assert.T(t, parent.FieldB == parent2.FieldB)
	assert.T(t, parent.Key() == parent2.Key())
	// Test equality
	assert.T(t, doc2.ALink.Equal(doc2.BLink))

	// Cleanup
	bucket, _ := client.Bucket("testmodel.go")
	err = bucket.Delete("TestModelKey")
	assert.T(t, err == nil)
	bucket, _ = client.Bucket("testmodellinks.go")
	err = bucket.Delete("TestModelKey")
	assert.T(t, err == nil)
}

type FriendLinks struct {
	Friends Many `riak:"friend"`
	Model
}

func TestModelWithManyLinks(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create two new "DocumentModel"s to use as friends and save it
	f1 := DocumentModel{FieldS: "friend1", FieldF: 1.0, FieldB: true}
	err := client.New("testmodel.go", "f1", &f1)
	assert.T(t, err == nil)
	err = f1.Save()
	assert.T(t, err == nil)
	f2 := DocumentModel{FieldS: "friend2", FieldF: 2.0, FieldB: true}
	err = client.New("testmodel.go", "f2", &f2)
	assert.T(t, err == nil)
	err = f2.Save()
	assert.T(t, err == nil)

	// Create a new "FriendLinks" to and save it
	doc := FriendLinks{Friends: Many{One{model: &f1}}}
	// Testing Many.Add while we're at it.
	err = doc.Friends.Add(&f2)
	assert.T(t, err == nil)
	err = client.New("testmodel.go", "TestMany", &doc)
	assert.T(t, err == nil)
	err = doc.Save()
	t.Logf("Friends json - %v\n", string(doc.robject.Data))

	// Now load a new document and verify it has two links
	var doc2 FriendLinks
	err = client.Load("testmodel.go", "TestMany", &doc2)
	assert.T(t, err == nil)
	assert.T(t, len(doc2.Friends) == 2)
	assert.T(t, doc2.Friends.Len() == 2)
	for i, v := range doc2.Friends {
		var f DocumentModel
		err = v.Get(&f)
		assert.T(t, err == nil)
		t.Logf("TestingModelWithManyLinks - %v - %v - %v\n", i, v, f)
	}

	// Now remove one of the documents.
	err = doc2.Friends.Remove(&f2)
	t.Logf("TestingModelWithManyLinks - removing a link %v", err)
	assert.T(t, err == nil)
	assert.T(t, doc2.Friends.Len() == 1)
	err = doc2.Save()
	assert.T(t, err == nil)
	// Test that f2 was removed
	var doc3 FriendLinks
	err = client.Load("testmodel.go", "TestMany", &doc3)
	assert.T(t, err == nil)
	assert.T(t, len(doc3.Friends) == 1)
	assert.T(t, doc3.Friends.Len() == 1)
	assert.T(t, doc3.Friends[0].link.Key == "f1") // Check if the correct link is remaining
	// Test if a link is in the set, both positive as well as negative, first positive
	assert.T(t, doc3.Friends.Contains(doc3.Friends[0]) == true)
	// Now remove the remaining link using RemoveLink
	err = doc3.Friends.RemoveLink(doc3.Friends[0])
	assert.T(t, err == nil)
	assert.T(t, len(doc3.Friends) == 0)
	assert.T(t, doc3.Friends.Len() == 0)
	assert.T(t, doc3.Friends.Contains(doc2.Friends[0]) == false) // Test negative
}

/*
Example resolve function for DocumentModel. This selects the longest FieldS
from the siblings, the largest FieldF and sets FieldB to true if any of the
siblings have it set to true.
*/
func (d *DocumentModel) Resolve(count int) (err error) {
	siblings := make([]DocumentModel, count)
	err = d.GetSiblings(siblings)
	if err != nil {
		return err
	}
	d.FieldB = false
	for _, s := range siblings {
		if len(s.FieldS) > len(d.FieldS) {
			d.FieldS = s.FieldS
		}
		if s.FieldF > d.FieldF {
			d.FieldF = s.FieldF
		}
		if s.FieldB {
			d.FieldB = true
		}
	}
	return
}

func TestConflictingModel(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create a bucket where siblings are allowed
	bucket, err := client.Bucket("testconflict.go")
	assert.T(t, err == nil)
	err = bucket.SetAllowMult(true)
	assert.T(t, err == nil)

	// Delete earlier work ...
	err = bucket.Delete("TestModelKey")
	assert.T(t, err == nil)

	// Create a new "DocumentModel" and save it
	doc := DocumentModel{FieldS: "text", FieldF: 1.2, FieldB: true}
	err = client.New("testconflict.go", "TestModelKey", &doc)
	assert.T(t, err == nil)
	err = doc.Save()
	assert.T(t, err == nil)

	// Create the same again (with the same key)
	doc2 := DocumentModel{FieldS: "longer_text", FieldF: 1.4, FieldB: false}
	err = client.New("testconflict.go", "TestModelKey", &doc2)
	assert.T(t, err == nil)
	err = doc2.Save()
	assert.T(t, err == nil)

	// Now load it from Riak to test conflicts
	doc3 := DocumentModel{}
	err = client.Load("testconflict.go", "TestModelKey", &doc3)
	t.Logf("Loading model - %v\n", err)
	t.Logf("DocumentModel = %v\n", doc3)
	assert.T(t, err == nil)
	assert.T(t, doc3.FieldS == doc2.FieldS) // doc2 has longer FieldS
	assert.T(t, doc3.FieldF == doc2.FieldF) // doc2 has larger FieldF
	assert.T(t, doc3.FieldB == doc.FieldB)  // doc has FieldB set to true

	// Now add another sibling, re-load it and check
	doc2b := DocumentModel{FieldS: "longer_text-evenlonger", FieldF: 1.8, FieldB: false}
	err = client.New("testconflict.go", "TestModelKey", &doc2b)
	assert.T(t, err == nil)
	err = doc2b.Save()
	assert.T(t, err == nil)
	//Reload
	err = doc3.Reload()
	assert.T(t, err == nil)
	assert.T(t, doc3.FieldS == doc2b.FieldS) // doc2b has longest FieldS
	assert.T(t, doc3.FieldF == doc2b.FieldF) // doc2b has largest FieldF
	assert.T(t, doc3.FieldB == doc.FieldB)   // doc has FieldB set to true

	// Cleanup
	err = bucket.Delete("TestModelKey")
	assert.T(t, err == nil)
}

func TestConflictingModelThatHasNoResolver(t *testing.T) {
	// This should throw an error when it has to Resolve since it didn't
	// override the default
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create a bucket where siblings are allowed
	bucket, err := client.Bucket("testconflict.go")
	assert.T(t, err == nil)
	err = bucket.SetAllowMult(true)
	assert.T(t, err == nil)

	t1 := DMTime{FieldS: "1"}
	err = client.NewModelIn("testconflict.go", "testconflictres", &t1)
	assert.T(t, err == nil)
	err = t1.Save()
	assert.T(t, err == nil)

	// Create with the same key
	t2 := DMTime{FieldS: "2"}
	err = client.NewModelIn("testconflict.go", "testconflictres", &t2)
	assert.T(t, err == nil)
	err = t2.Save()
	assert.T(t, err == nil)

	// Now load to test conflicts, should return error ResolveNotImplemented
	t3 := DMTime{}
	err = client.LoadModelFrom("testconflict.go", "testconflictres", &t3)
	assert.T(t, err == ResolveNotImplemented)
}

type SliceType struct {
	Value []string
	Model
}

func (f *SliceType) Resolve(siblingsCount int) error {
	siblings := make([]SliceType, siblingsCount)

	err := f.GetSiblings(&siblings)
	if err != nil {
		return err
	}

	var haveA, haveB bool

	for _, sib := range siblings {
		if len(sib.Value) == 1 && sib.Value[0] == "A" {
			haveA = true
		} else if len(sib.Value) == 1 && sib.Value[0] == "B" {
			haveB = true
		}
	}

	if haveA && haveB {
		return nil
	}
	return errors.New("Failed to find both \"A\" \"B\" strings!")
}

func TestConflictingModelWithSlices(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create a bucket where siblings are allowed
	bucket, err := client.Bucket("testconflict.go")
	assert.T(t, err == nil)
	err = bucket.SetAllowMult(true)
	assert.T(t, err == nil)
	// Preform clean up, make sure no other conflicts are kicking around.
	err = bucket.Delete("testconflictres")
	assert.T(t, err == nil)

	store := SliceType{Value: []string{"A"}}
	err = client.NewModelIn("testconflict.go", "testconflictres", &store)
	assert.T(t, err == nil)
	err = store.Save()
	assert.T(t, err == nil)

	store = SliceType{Value: []string{"B"}}
	err = client.NewModelIn("testconflict.go", "testconflictres", &store)
	assert.T(t, err == nil)
	err = store.Save()
	assert.T(t, err == nil)

	load := SliceType{}
	err = client.LoadModelFrom("testconflict.go", "testconflictres", &load)
	assert.T(t, err == nil, err)

	err = bucket.Delete("testconflictres")
	assert.T(t, err == nil)
	err = bucket.SetAllowMult(false)
	assert.T(t, err == nil)
}

type DMTime struct {
	FieldS string
	FieldT time.Time
	Model
}

func TestModelTime(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create and save
	doc := DMTime{FieldS: "text", FieldT: time.Now()}
	err := client.New("testmodel.go", "TestTime", &doc)
	assert.T(t, err == nil)
	//err = client.Save(&doc)
	err = doc.Save()
	assert.T(t, err == nil)

	// Load it from Riak and check that the fields of the DocumentModel struct are set correctly
	doc2 := DMTime{}
	err = client.Load("testmodel.go", "TestTime", &doc2)
	assert.T(t, err == nil)
	assert.T(t, doc2.FieldS == doc.FieldS)
	t.Logf("FieldT= %v ? %v\n", doc2.FieldT, doc.FieldT)
	assert.T(t, doc2.FieldT.Equal(doc.FieldT))
}

type SubStruct struct {
	Value string "value"
}

type DMInclude struct {
	Name string    "name"
	Sub  SubStruct "sub"
	Model
}

func TestModelIncludingOtherStruct(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create and save
	doc := DMInclude{Name: "some name", Sub: SubStruct{Value: "some value"}}
	err := client.New("testmodel.go", "TestModelIncludingOtherStruct", &doc)
	assert.T(t, err == nil)
	//err = client.Save(&doc)
	err = doc.Save()
	assert.T(t, err == nil)

	// Load it from Riak and check that the fields of the DocumentModel struct are set correctly
	doc2 := DMInclude{}
	err = client.Load("testmodel.go", "TestModelIncludingOtherStruct", &doc2)
	t.Logf("doc2 json = %v\n", string(doc2.robject.Data))
	assert.T(t, err == nil)
	assert.T(t, string(doc2.robject.Data) == `{"_type":"DMInclude","name":"some name","sub":{"_type":"SubStruct","value":"some value"}}`)
	assert.T(t, doc2.Name == doc.Name)
	t.Logf("Sub struct = %v ? %v\n", doc2.Sub.Value, doc.Sub.Value)
	assert.T(t, doc2.Sub.Value == doc.Sub.Value)
}

func TestModelReload(t *testing.T) {
	// Preparations
	client := setupConnection(t)
	assert.T(t, client != nil)

	// Create a new "DocumentModel" and save it
	doc := DocumentModel{FieldS: "text", FieldF: 1.2, FieldB: true}
	err := client.New("testmodel.go", "TestModelKey", &doc)
	assert.T(t, err == nil)
	err = doc.Save()
	assert.T(t, err == nil)

	doc2 := DocumentModel{FieldS: "text22", FieldF: 1.4, FieldB: true}
	err = client.New("testmodel.go", "TestModelKey", &doc2)
	err = doc2.Save()
	assert.T(t, err == nil)

	vclock := string(doc.Vclock())
	err = (&doc).Reload()
	assert.T(t, err == nil)
	assert.T(t, string(doc.Vclock()) != vclock)
	assert.T(t, string(doc.robject.Vclock) == string(doc2.robject.Vclock))
	assert.T(t, doc.FieldS == doc2.FieldS)
	assert.T(t, doc.FieldF == doc2.FieldF)
	assert.T(t, doc.FieldB == doc2.FieldB)
}

func TestModelNew(t *testing.T) {
	err := ConnectClientPool("127.0.0.1:8087", 5)
	assert.T(t, err == nil)

	doc := DocumentModel{FieldS: "text", FieldF: 1.2, FieldB: true}
	err = NewModel("", &doc)
	assert.T(t, err == nil)
	assert.T(t, doc.Key() == "")
	// Save the doc, now the key should be set
	err = doc.Save()
	assert.T(t, err == nil)
	assert.T(t, doc.Key() != "")
	assert.T(t, string(doc.Vclock()) != "")
	// Verify that the default bucket was used
	assert.T(t, doc.robject.Bucket.Name() == "testmodeldefault.go")
}

func TestClientSaveAndLoad(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	doc := DocumentModel{FieldS: "text", FieldF: 1.2, FieldB: true}
	err := client.NewModel("willbeoverwrittenbySaveAs", &doc)
	assert.T(t, err == nil)

	err = client.SaveAs("clientsavetest", &doc)
	assert.T(t, err == nil)

	err = client.LoadModel("clientsavetest", &doc)
	assert.T(t, err == nil)

	err = doc.Delete()
	assert.T(t, err == nil)
}

type A struct {
	Model
	Err int
}

func (*A) MarshalJSON() ([]byte, error) {
	return []byte{}, errors.New("Deliberate JSON Marshalling error")
}

func TestErrorCatching(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	// First test by supplying something that is not even a pointer to a struct
	err := client.Save(nil)
	assert.T(t, err != nil)

	// Test by supplying a model that is not initialized
	doc := DocumentModel{FieldS: "text", FieldF: 1.2, FieldB: true}
	err = client.SaveAs("clientsavetest", &doc)
	assert.T(t, err == DestinationNotInitialized)

	// Create a model that cannot be marshalled to JSON (see helpers above)
	a := A{Err: 1}
	err = client.NewModelIn("abucket", "newKey", &a)
	assert.T(t, err == nil) // this should still work
	err = client.SaveAs("newKey", &a)
	assert.T(t, err != nil) // but marshalling should fail
	assert.T(t, strings.Contains(err.Error(), "Deliberate"))

	// Same for Loading instead of Saving:
	// First test by supplying something that is not even a pointer to a struct
	err = client.LoadModelFrom("bucketname", "key", nil)
	assert.T(t, err != nil)

	// struct A has no "tag" for the Model field, so the bucket MUST be supplied
	err = client.LoadModel("key", &a)
	assert.T(t, err != nil)
	t.Logf("err = %v\n", err)
	assert.T(t, strings.Contains(err.Error(), "Can't get bucket"))

	// Load a model that doesn't exist
	err = client.LoadModelFrom("bucketnamethatdoesnotexit", "keythatdoesnotexist___", &a)
	assert.T(t, err == NotFound)
}

func TestBrokenModels(t *testing.T) {
	err := ConnectClient("127.0.0.1:8087")
	assert.T(t, err == nil)

	// Create some JSON with a _type field that does not match the class name
	obj, err := NewObjectIn("brokenmodels", "brokenmodel")
	assert.T(t, err == nil)
	assert.T(t, obj != nil)
	obj.ContentType = "application/json"
	obj.Data = []byte(`{"_type":"notthismodel","field":"A"}`)
	err = obj.Store()
	assert.T(t, err == nil)
	// Try to load this into a doc
	doc := DocumentModel{}
	err = LoadModelFrom("brokenmodels", "brokenmodel", &doc)
	assert.T(t, err != nil)
	assert.T(t, strings.Contains(err.Error(), "struct name does not match _type in Riak"))

	// Now change the content so the _type matches, but the fields don't match
	obj.Data = []byte(`{"_type":"DocumentModel","string_field":"string","float_field":"stringnotfloat","FieldB":true}`)
	err = obj.Store()
	assert.T(t, err == nil)
	err = LoadModelFrom("brokenmodels", "brokenmodel", &doc)
	assert.T(t, err != nil)
	assert.T(t, strings.Contains(err.Error(), "cannot unmarshal"))
	assert.T(t, IsWarning(err))

	// Cleanup the test object
	err = obj.Destroy()
	assert.T(t, err == nil)

	// Try to set a key on something that is not a model
	err = defaultClient.SetKey("somekey", nil)
	assert.T(t, err != nil)
	// Try to set a key on a model that is not initialized
	a := A{}
	err = defaultClient.SetKey("somekey", &a)
	assert.T(t, err == DestinationNotInitialized)
	// Try again using the Model direct
	err = a.SetKey("somekey")
	assert.T(t, err == DestinationNotInitialized)
}

func TestNewModelInErrors(t *testing.T) {
	err := ConnectClient("127.0.0.1:8087")
	assert.T(t, err == nil)

	// Try NewModal with something that is not a Model
	err = NewModelIn("", "key", nil)
	assert.T(t, err != nil)
	assert.T(t, strings.Contains(err.Error(), `Destination is not a pointer`))

	// Try NewModelIn without a bucketname (for a model that doesn't specify it in the riak.Model tag)
	a := A{}
	err = NewModelIn("", "key", &a)
	assert.T(t, err != nil)
	assert.T(t, strings.Contains(err.Error(), `Can't get bucket for`))

	// Now try NewModel on a Model that is already initialized
	err = NewModelIn("bucketname", "key", &a)
	assert.T(t, err == nil) // First should work
	err = NewModelIn("bucketname", "key", &a)
	assert.T(t, err == ModelNotNew) // Second should fail with ModelNotNew error
}

func stringInSlice(a string, list []string) bool {
	for _, val := range list {
		if val == a {
			return true
		}
	}
	return false
}

// Testing multiple values for the same secondary index
func TestMultipleMultiIndexesInModel(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	bucket, _ := client.Bucket("client_test.go")
	assert.T(t, bucket != nil)

	// Create object
	doc := DocumentModel{FieldS: "blurb", FieldF: 123, FieldB: true}
	err := client.New("client_test.go", "Bob", &doc)
	assert.T(t, err == nil)
	indexes := doc.MultiIndexes()
	indexes["phone_int"] = []string{"12345", "67890"}
	err = doc.Save()
	assert.T(t, err == nil)

	// Create a second object
	doc2 := DocumentModel{FieldS: "blurb", FieldF: 124, FieldB: true}
	err = client.NewModelIn("client_test.go", "Alice", &doc2)
	assert.T(t, err == nil)
	indexes = doc2.MultiIndexes()
	indexes["phone_int"] = []string{"12345", "99999"}
	err = doc2.Save()
	assert.T(t, err == nil)

	// Fetch the object and check
	err = client.LoadModelFrom("client_test.go", "Bob", &doc)
	assert.T(t, err == nil)
	assert.T(t, stringInSlice(strconv.Itoa(12345), doc.MultiIndexes()["phone_int"]))
	assert.T(t, stringInSlice(strconv.Itoa(67890), doc.MultiIndexes()["phone_int"]))

	// Get a list of keys using the index queries
	// Expecting two keys
	keys, err := bucket.IndexQuery("phone_int", strconv.Itoa(12345))
	if err == nil {
		t.Logf("2i query returned : %v\n", keys)
	} else {
		if err.Error() == "EOF" {
			t.Log("2i queries over protobuf is not supported, maybe running a pre 1.2 version of Riak - skipping 2i tests.")
			return
		} else if err.Error() == "{error,{indexes_not_supported,riak_kv_bitcask_backend}}" {
			t.Log("2i queries not support on bitcask backend - skipping 2i tests.")
			return
		} else if strings.Contains(err.Error(), "indexes_not_supported") {
			t.Logf("2i queries not supported - skipping 2i tests (%v).\n", err)
			return
		}
		t.Logf("2i query returned error : %v\n", err)
	}
	assert.T(t, err == nil)
	assert.T(t, len(keys) == 2)
	assert.T(t, stringInSlice("Alice", keys))
	assert.T(t, stringInSlice("Bob", keys))
	// Get a list of keys using the index queries
	// Expecting "Bob"
	keys, err = bucket.IndexQuery("phone_int", strconv.Itoa(67890))
	assert.T(t, err == nil)
	assert.T(t, len(keys) == 1)
	assert.T(t, keys[0] == "Bob")

	// Expecting "Alice"
	keys, err = bucket.IndexQuery("phone_int", strconv.Itoa(99999))
	assert.T(t, err == nil)
	assert.T(t, len(keys) == 1)
	assert.T(t, keys[0] == "Alice")

	// Cleanup
	err = doc.Delete()
	assert.T(t, err == nil)
	err = doc2.Delete()
	assert.T(t, err == nil)

}

func TestIndexesInModel(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	bucket, _ := client.Bucket("client_test.go")
	assert.T(t, bucket != nil)

	// Create object
	doc := DocumentModel{FieldS: "blurb", FieldF: 123, FieldB: true}
	err := client.New("client_test.go", "indexesSingle", &doc)
	assert.T(t, err == nil)
	indexes := doc.Indexes()
	indexes["test_int"] = "123"
	indexes["and_bin"] = "blurb"
	err = doc.Save()
	assert.T(t, err == nil)

	// Create a second object
	doc2 := DocumentModel{FieldS: "blurb", FieldF: 124, FieldB: true}
	err = client.NewModelIn("client_test.go", "indexes2Single", &doc2)
	assert.T(t, err == nil)
	indexes = doc2.Indexes()
	indexes["test_int"] = "124"
	indexes["and_bin"] = "blurb"
	err = doc2.Save()
	assert.T(t, err == nil)

	// Fetch the object and check
	err = client.LoadModelFrom("client_test.go", "indexesSingle", &doc)
	assert.T(t, err == nil)
	assert.T(t, doc.Indexes()["test_int"] == strconv.Itoa(123))
	assert.T(t, doc.Indexes()["and_bin"] == "blurb")

	// Get a list of keys using the index queries
	keys, err := bucket.IndexQuery("test_int", strconv.Itoa(123))
	if err == nil {
		t.Logf("2i query returned : %v\n", keys)
	} else {
		if err.Error() == "EOF" {
			t.Log("2i queries over protobuf is not supported, maybe running a pre 1.2 version of Riak - skipping 2i tests.")
			return
		} else if err.Error() == "{error,{indexes_not_supported,riak_kv_bitcask_backend}}" {
			t.Log("2i queries not support on bitcask backend - skipping 2i tests.")
			return
		} else if strings.Contains(err.Error(), "indexes_not_supported") {
			t.Logf("2i queries not supported - skipping 2i tests (%v).\n", err)
			return
		}
		t.Logf("2i query returned error : %v\n", err)
	}
	assert.T(t, err == nil)
	assert.T(t, len(keys) == 1)
	assert.T(t, keys[0] == "indexesSingle")
	// Get a list of keys using the index range query
	keys, err = bucket.IndexQueryRange("test_int", strconv.Itoa(120), strconv.Itoa(130))
	if err == nil {
		t.Logf("2i range query returned : %v\n", keys)
	}
	assert.T(t, err == nil)
	assert.T(t, len(keys) == 2)
	assert.T(t, keys[0] == "indexesSingle" || keys[1] == "indexesSingle")
	assert.T(t, keys[0] == "indexes2Single" || keys[1] == "indexes2Single")

	// Cleanup
	err = doc.Delete()
	assert.T(t, err == nil)
	err = doc2.Delete()
	assert.T(t, err == nil)
}

func TestMultiIndexesInModel(t *testing.T) {
	client := setupConnection(t)
	assert.T(t, client != nil)

	bucket, _ := client.Bucket("client_test.go")
	assert.T(t, bucket != nil)

	// Create object
	doc := DocumentModel{FieldS: "blurb", FieldF: 123, FieldB: true}
	err := client.New("client_test.go", "indexes", &doc)
	assert.T(t, err == nil)
	indexes := doc.MultiIndexes()
	indexes["test_int"] = []string{"123"}
	indexes["and_bin"] = []string{"blurb"}
	err = doc.Save()
	assert.T(t, err == nil)

	// Create a second object
	doc2 := DocumentModel{FieldS: "blurb", FieldF: 124, FieldB: true}
	err = client.NewModelIn("client_test.go", "indexes2", &doc2)
	assert.T(t, err == nil)
	indexes = doc2.MultiIndexes()
	indexes["test_int"] = []string{"124"}
	indexes["and_bin"] = []string{"blurb"}
	err = doc2.Save()
	assert.T(t, err == nil)

	// Fetch the object and check
	err = client.LoadModelFrom("client_test.go", "indexes", &doc)
	assert.T(t, err == nil)
	assert.T(t, doc.MultiIndexes()["test_int"][0] == strconv.Itoa(123))
	assert.T(t, doc.MultiIndexes()["and_bin"][0] == "blurb")

	// Get a list of keys using the index queries
	keys, err := bucket.IndexQuery("test_int", strconv.Itoa(123))
	if err == nil {
		t.Logf("2i query returned : %v\n", keys)
	} else {
		if err.Error() == "EOF" {
			t.Log("2i queries over protobuf is not supported, maybe running a pre 1.2 version of Riak - skipping 2i tests.")
			return
		} else if err.Error() == "{error,{indexes_not_supported,riak_kv_bitcask_backend}}" {
			t.Log("2i queries not support on bitcask backend - skipping 2i tests.")
			return
		} else if strings.Contains(err.Error(), "indexes_not_supported") {
			t.Logf("2i queries not supported - skipping 2i tests (%v).\n", err)
			return
		}
		t.Logf("2i query returned error : %v\n", err)
	}
	assert.T(t, err == nil)
	assert.T(t, len(keys) == 1)
	assert.T(t, keys[0] == "indexes")
	// Get a list of keys using the index range query
	keys, err = bucket.IndexQueryRange("test_int", strconv.Itoa(120), strconv.Itoa(130))
	if err == nil {
		t.Logf("2i range query returned : %v\n", keys)
	}
	assert.T(t, err == nil)
	assert.T(t, len(keys) == 2)
	assert.T(t, keys[0] == "indexes" || keys[1] == "indexes")
	assert.T(t, keys[0] == "indexes2" || keys[1] == "indexes2")

	// Cleanup
	err = doc.Delete()
	assert.T(t, err == nil)
	err = doc2.Delete()
	assert.T(t, err == nil)
}

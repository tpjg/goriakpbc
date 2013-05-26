// +build go1.1
package riak

import (
	"github.com/bmizerany/assert"
	"testing"
)

type DocumentModel11 struct {
	FieldS string  `riak:"string_field"`
	FieldF float64 `riak:"float_field"`
	FieldB bool
	Model  `riak:"testmodeldefault.go"`
}

/*
Example resolve function for DocumentModel11. Using Siblings() function that
should allocate the array of Models rather then having the application do
it. Only needs a type assertion (reflect doesn't give us any other option).
*/
func (d *DocumentModel11) Resolve(count int) (err error) {
	s, err := d.Siblings(d)
	if err != nil {
		return err
	}
	siblings := s.([]DocumentModel11) // Assert correct type

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

func TestConflictingModel11(t *testing.T) {
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

	// Create a new "DocumentModel11" and save it
	doc := DocumentModel11{FieldS: "text", FieldF: 1.2, FieldB: true}
	err = client.New("testconflict.go", "TestModelKey", &doc)
	assert.T(t, err == nil)
	err = doc.Save()
	assert.T(t, err == nil)

	// Create the same again (with the same key)
	doc2 := DocumentModel11{FieldS: "longer_text", FieldF: 1.4, FieldB: false}
	err = client.New("testconflict.go", "TestModelKey", &doc2)
	assert.T(t, err == nil)
	err = doc2.Save()
	assert.T(t, err == nil)

	// Now load it from Riak to test conflicts
	doc3 := DocumentModel11{}
	err = client.Load("testconflict.go", "TestModelKey", &doc3)
	t.Logf("Loading model - %v\n", err)
	t.Logf("DocumentModel11 = %v\n", doc3)
	assert.T(t, err == nil)
	assert.T(t, doc3.FieldS == doc2.FieldS) // doc2 has longer FieldS
	assert.T(t, doc3.FieldF == doc2.FieldF) // doc2 has larger FieldF
	assert.T(t, doc3.FieldB == doc.FieldB)  // doc has FieldB set to true

	// Cleanup
	err = bucket.Delete("TestModelKey")
	assert.T(t, err == nil)
}

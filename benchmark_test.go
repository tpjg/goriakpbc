package riak

import (
	"testing"
)

func BenchmarkStoreObject(b *testing.B) {
	client := New("127.0.0.1:8087")
	err := client.Connect()
	if err != nil {
		b.FailNow()
	}

	for i := 0; i < b.N; i++ {
		bucket, err := client.Bucket("client_test.go")
		if err != nil {
			b.FailNow()
		}
		obj := bucket.New("abc", PW1, DW1)
		if obj == nil {
			b.FailNow()
		}
		obj.ContentType = "text/plain"
		obj.Data = []byte("some more data")
		err = obj.Store()
		if err != nil {
			b.Log(err)
			b.Fail()
		}
	}
}

func BenchmarkGetObject(b *testing.B) {
	client := New("127.0.0.1:8087")
	err := client.Connect()
	if err != nil {
		b.FailNow()
	}
	bucket, err := client.Bucket("client_test.go")
	if err != nil {
		b.FailNow()
	}
	obj := bucket.New("abc", PW1, DW1, R1)
	if obj == nil {
		b.FailNow()
	}
	obj.ContentType = "text/plain"
	obj.Data = []byte("some more data")
	err = obj.Store()
	if err != nil {
		b.FailNow()
	}

	for i := 0; i < b.N; i++ {
		obj, err = bucket.Get("abc", R1, PR1)
		if err != nil || obj.Data == nil {
			b.Log(err)
			b.Fail()
		}
	}
}

func BenchmarkSaveModel(b *testing.B) {
	client := New("127.0.0.1:8087")
	err := client.Connect()
	if err != nil {
		b.FailNow()
	}

	for i := 0; i < b.N; i++ {
		// Create a new "DocumentModel" and save it
		doc := DMInclude{Name: "some name", Sub: SubStruct{Value: "some value"}}
		err := client.New("testmodel.go", "BenchModelKey", &doc)
		if err != nil {
			b.Log(err)
			b.FailNow()
		}
		err = doc.Save()
		if err != nil {
			b.Log(err)
			b.Fail()
		}
	}
}

func BenchmarkLoadModel(b *testing.B) {
	client := New("127.0.0.1:8087")
	err := client.Connect()
	if err != nil {
		b.FailNow()
	}
	// Create a new "DocumentModel" and save it
	doc := DMInclude{Name: "some name", Sub: SubStruct{Value: "some value"}}
	err = client.New("testmodel.go", "BenchModelKey", &doc)
	if err != nil {
		b.Log(err)
		b.FailNow()
	}
	err = doc.Save()
	if err != nil {
		b.Log(err)
		b.Fail()
	}

	for i := 0; i < b.N; i++ {
		err = client.Load("testmodel.go", "BenchModelKey", &doc, R1)
		if err != nil {
			b.Log(err)
			b.Fail()
		}
	}
}

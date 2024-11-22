package chroma_test

import (
	"github.com/Adedunmol/chroma"
	"reflect"
	"testing"
)

func TestParseDelete(t *testing.T) {
	oplog := []byte(`{
		"op": "d",
		"ns": "test.student",
		"o":  {
			"_id": "635b79e231d82a8ab1de863b"
		}
	}`)

	data, err := chroma.ParseJSONMap(oplog)
	if err != nil {
		t.Fatal(err)
	}

	got := chroma.NewDelete()
	err = got.Parse(data)
	if err != nil {
		t.Fatal(err)
	}

	want := chroma.Delete{
		Database:  "test",
		Table:     "student",
		Condition: chroma.KeyValue{Key: "_id", Value: "635b79e231d82a8ab1de863b"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v want %#v", got, want)
	}
}

func TestStringDelete(t *testing.T) {
	oplog := []byte(`{
		"op": "d",
		"ns": "test.student",
		"o":  {
			"_id": "635b79e231d82a8ab1de863b"
		}
	}`)

	data, err := chroma.ParseJSONMap(oplog)
	if err != nil {
		t.Fatal(err)
	}

	delete := chroma.NewDelete()
	err = delete.Parse(data)
	if err != nil {
		t.Fatal(err)
	}

	got := delete.String()

	want := "DELETE FROM student WHERE _id = 635b79e231d82a8ab1de863b"

	if len(got) != len(want) {
		t.Errorf("got %d, want %d", len(got), len(want))
	}
}

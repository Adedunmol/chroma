package chroma_test

import (
	chroma "github.com/Adedunmol/chroma"
	"reflect"
	"testing"
)

func TestParseInsert(t *testing.T) {
	oplog := []byte(`{
		"op": "i",
		"ns": "test.student",
		"o":  {
			"_id": "635b79e231d82a8ab1de863b",
			"name": "John Doe",
			"roll_no": 51,
			"is_graduated": false,
			"date_of_birth": "2000-01-30"
		}
	}`)

	got, err := chroma.Parse(oplog)
	if err != nil {
		t.Fatal(err)
	}

	wanted := chroma.Insert{
		Database: "test",
		Table:    "student",
		Columns: []chroma.KeyValue{
			{Key: "_id", Value: "635b79e231d82a8ab1de863b"},
			{Key: "name", Value: "John Doe"},
			{Key: "roll_no", Value: float64(51)},
			{Key: "is_graduated", Value: false},
			{Key: "date_of_birth", Value: "2000-01-30"},
		},
	}

	if !reflect.DeepEqual(got, wanted) {
		t.Errorf("got %#v want %#v", got, wanted)
	}
}

package main_test

import (
	chroma "github.com/Adedunmol/chroma"
	"reflect"
	"testing"
)

func TestSeparateOperations(t *testing.T) {
	oplog := []byte(`
	[
		{
    	"op": "i",
    	"ns": "test.student",
    	"o": {
      		"_id": "635b79e231d82a8ab1de863b",
      		"name": "Selena Miller",
      		"roll_no": 51,
      		"is_graduated": false,
      		"date_of_birth": "2000-01-30"
    		}
  		},
  		{
		"op": "i",
    	"ns": "test.student",
    	"o": {
      		"_id": "14798c213f273a7ca2cf5174",
      		"name": "George Smith",
      		"roll_no": 21,
      		"is_graduated": true,
      		"date_of_birth": "2001-03-23"
    	}
  	}
	]
`)

	oplogsMap, err := chroma.ParseJSONArray(oplog)
	if err != nil {
		t.Errorf("got unexpected error: %v", err)
	}

	got := chroma.SeparateOperations(oplogsMap)

	want := []chroma.Handler{
		&chroma.Insert{
			Database: "test",
			Table:    "student",
			Columns: []chroma.KeyValue{
				{"_id", "635b79e231d82a8ab1de863b"},
				{"name", "Selena Miller"},
				{"roll_no", float64(51)},
				{"is_graduated", false},
				{"date_of_birth", "2000-01-30"},
			}},
		&chroma.Insert{
			Database: "test",
			Table:    "student",
			Columns: []chroma.KeyValue{
				{"_id", "14798c213f273a7ca2cf5174"},
				{"name", "George Smith"},
				{"roll_no", float64(21)},
				{"is_graduated", true},
				{"date_of_birth", "2001-0-23"},
			}},
	}

	if len(got) != len(want) {
		t.Errorf("got %v, want %v", len(got), len(want))
	}

	if !reflect.DeepEqual(&got[0], &want[0]) {
		t.Errorf("got %#v, want %#v", &got[0], &want[0])
	}
}

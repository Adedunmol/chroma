package chroma_test

import (
	"github.com/Adedunmol/chroma"
	"reflect"
	"testing"
)

func TestUpdate(t *testing.T) {
	oplog := []byte(`{
		"op": "u",
		"ns": "test.student",
		"o":  {
			"$v": 2,
			"diff": {
				"u": {
					"is_graduated": true
				}
			}
		},
		"o2": {
			"_id": "635b79e231d82a8ab1de863b"
		}
	}`)

	update := chroma.NewUpdate()
	data, err := chroma.ParseJSONMap(oplog)
	if err != nil {
		t.Fatal(err)
	}

	got, err := update.Parse(data)
	if err != nil {
		t.Fatal(err)
	}

	want := chroma.Update{
		Op:        "u",
		Database:  "test",
		Table:     "student",
		Columns:   []chroma.KeyValue{{Key: "is_graduated", Value: true}},
		Condition: chroma.KeyValue{Key: "_id", Value: "635b79e231d82a8ab1de863b"},
	}

	if !reflect.DeepEqual(*got, want) {
		t.Errorf("got %#v want %#v", *got, want)
	}
}

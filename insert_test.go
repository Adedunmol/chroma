package chroma_test

import (
	"reflect"
	"testing"
)

func TestParseInsert(t *testing.T) {
	oplog = []byte(`{
		"op": "i",
		"ns": "test.student",
		"o":  {
			"_id": "635b79e231d82a8ab1de863b",
			"name": "John Doe",
			"roll_no": 51,
			"is_graduated": false,
			"date_of_birth": "2000-01-30"
		},
	}`)

	got, err := Parse(oplog)
	if err != nil {
	}

	wanted := Insert{}

	if !reflect.DeepEqual(got, wanted) {
		t.Errorf("got %#v want %#v", got, wanted)
	}
}
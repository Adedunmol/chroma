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

	data, err := chroma.ParseJSONMap(oplog)
	if err != nil {
		t.Fatal(err)
	}

	insert := chroma.NewInsert()
	got, err := insert.Parse(data)
	if err != nil {
		t.Fatal(err)
	}

	want := chroma.Insert{
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

	if !reflect.DeepEqual(*got, want) {
		t.Errorf("got %#v want %#v", *got, want)
	}
}

func TestStringInsert(t *testing.T) {
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

	data, err := chroma.ParseJSONMap(oplog)
	if err != nil {
		t.Fatal(err)
	}

	insert := chroma.NewInsert()
	result, err := insert.Parse(data)
	if err != nil {
		t.Fatal(err)
	}

	got := result.String()

	want := "INSERT INTO student (_id, name, roll_no, is_graduated, date_of_birth) VALUES (635b79e231d82a8ab1de863b, John Doe, 51, false, 2000-01-30)"

	if len(got) != len(want) {
		t.Errorf("got %d, want %d", len(got), len(want))
	}
}

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

	got := chroma.NewInsert()
	err = got.Parse(data)
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

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v want %#v", got, want)
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
	err = insert.Parse(data)
	if err != nil {
		t.Fatal(err)
	}

	got := insert.String()

	want := "INSERT INTO student (_id, name, roll_no, is_graduated, date_of_birth) VALUES (635b79e231d82a8ab1de863b, John Doe, 51, false, 2000-01-30)"

	if len(got) != len(want) {
		t.Errorf("got %d, want %d", len(got), len(want))
	}
}

func TestCreateTable(t *testing.T) {
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
	err = insert.Parse(data)
	if err != nil {
		t.Fatal(err)
	}

	got, err := insert.CreateTable()
	if err != nil {
		t.Fatal(err)
	}

	want := `CREATE TABLE IF NOT EXISTS student (
    	date_of_birth VARCHAR(255),
		_id VARCHAR(255) PRIMARY KEY,
		name VARCHAR(255),
		roll_no FLOAT,
		is_graduated BOOLEAN
	);`

	if len(got) != len(want) {
		t.Errorf("got: %d want: %d", len(got), len(want))
	}
}

func TestCreateSchema(t *testing.T) {
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
	err = insert.Parse(data)
	if err != nil {
		t.Fatal(err)
	}

	got := insert.CreateSchema()
	if err != nil {
		t.Fatal(err)
	}

	want := "CREATE SCHEMA IF NOT EXISTS test;"

	if got != want {
		t.Errorf("got: %s want: %s", got, want)
	}
}

package main_test

import (
	chroma "github.com/Adedunmol/chroma"
	"reflect"
	"strings"
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

	//table := chroma.Table{
	//	Name: "student",
	//	Schema: map[string]bool{
	//		"_id":           true,
	//		"name":          true,
	//		"roll_no":       true,
	//		"is_graduated":  true,
	//		"date_of_birth": true,
	//	},
	//}

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
		t.Errorf("got: %#v\n want: %#v", got, want)
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

	if !strings.Contains(got, "CREATE SCHEMA") {
		t.Errorf("expected output to contain: %s", "CREATE SCHEMA")
	}
	if !strings.Contains(got, "CREATE TABLE") {
		t.Errorf("expected output to contain: %s", "CREATE TABLE")
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

	if !strings.Contains(got, "CREATE TABLE IF NOT EXISTS student") {
		t.Errorf("expected output to contain: %s", "CREATE TABLE IF NOT EXISTS student")
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

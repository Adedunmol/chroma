package main_test

import (
	"errors"
	chroma "github.com/Adedunmol/chroma"
	"reflect"
	"testing"
)

func TestParseJSON(t *testing.T) {

	t.Run("check parsing of insert", func(t *testing.T) {
		oplog := []byte(`{
		"op": "i",
		"ns": "test.student",
		"o": {
			"_id": "635b79e231d82a8ab1de863b",
			"name": "John Doe",
			"roll_no": 51,
			"is_graduated": false,
			"date_of_birth": "2000-01-30"
		}
	}`)

		got, err := chroma.ParseJSON(oplog)
		if err != nil {
		}

		want := chroma.Oplog{
			Op:        "insert",
			Namespace: "test.student",
			Object: map[string]interface{}{
				"_id":           "635b79e231d82a8ab1de863b",
				"name":          "John Doe",
				"roll_no":       float64(51),
				"is_graduated":  false,
				"date_of_birth": "2000-01-30",
			},
		}

		assertEqual(t, got.Op, want.Op)
		assertEqual(t, got.Namespace, want.Namespace)
		assertObjectEqual(t, got.Object, want.Object)
	})

	t.Run("check parsing of update", func(t *testing.T) {
		oplog := []byte(`{
		"op": "u",
		"ns": "test.student",
		"o": {
			"_id": "635b79e231d82a8ab1de863b",
			"name": "John Doe",
			"roll_no": 51,
			"is_graduated": false,
			"date_of_birth": "2000-01-30"
		}
		}`)

		got, err := chroma.ParseJSON(oplog)
		if err != nil {
		}

		want := chroma.Oplog{
			Op:        "update",
			Namespace: "test.student",
			Object: map[string]interface{}{
				"_id":           "635b79e231d82a8ab1de863b",
				"name":          "John Doe",
				"roll_no":       float64(51),
				"is_graduated":  false,
				"date_of_birth": "2000-01-30",
			},
		}

		assertEqual(t, got.Op, want.Op)
		assertEqual(t, got.Namespace, want.Namespace)
		assertObjectEqual(t, got.Object, want.Object)
	})

	t.Run("check parsing of unknown operation", func(t *testing.T) {
		oplog := []byte(`{
		"op": "g",
		"ns": "test.student",
		"o": {
			"_id": "635b79e231d82a8ab1de863b",
			"name": "John Doe",
			"roll_no": 51,
			"is_graduated": false,
			"date_of_birth": "2000-01-30"
		}
		}`)

		_, err := chroma.ParseJSON(oplog)
		if err == nil {
			t.Errorf("expected an error")
		}

		if !errors.Is(err, chroma.UnknownOp) {
			t.Errorf("got unexpected error: %v", err)
		}
	})
}

func TestParseJSONMap(t *testing.T) {

	t.Run("check parsing of insert", func(t *testing.T) {
		oplog := []byte(`{
		"op": "i",
		"ns": "test.student",
		"o": {
			"_id": "635b79e231d82a8ab1de863b",
			"name": "John Doe",
			"roll_no": 51,
			"is_graduated": false,
			"date_of_birth": "2000-01-30"
		}
	}`)
		got, err := chroma.ParseJSONMap(oplog)
		if err != nil {
		}

		want := map[string]interface{}{
			"op": "insert",
			"ns": "test.student",
			"o": map[string]interface{}{
				"_id":           "635b79e231d82a8ab1de863b",
				"name":          "John Doe",
				"roll_no":       float64(51),
				"is_graduated":  false,
				"date_of_birth": "2000-01-30",
			},
		}

		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("check parsing of update", func(t *testing.T) {
		oplog := []byte(`{
			"op": "u",
			"ns": "test.student",
			"o":  {
				"$v": 2,
				"diff": {
					"d": {
						"roll_no": false
					}
				}
			},
			"o2": {
				"_id": "635b79e231d82a8ab1de863b"
			}
		}`)

		got, err := chroma.ParseJSONMap(oplog)
		if err != nil {
		}

		want := map[string]interface{}{
			"op": "update",
			"ns": "test.student",
			"o": map[string]interface{}{
				"$v": float64(2),
				"diff": map[string]interface{}{
					"d": map[string]interface{}{
						"roll_no": false,
					},
				},
			},
			"o2": map[string]interface{}{
				"_id": "635b79e231d82a8ab1de863b",
			},
		}

		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("check parsing of unknown operation", func(t *testing.T) {
		oplog := []byte(`{
		"op": "g",
		"ns": "test.student",
		"o": {
			"_id": "635b79e231d82a8ab1de863b",
			"name": "John Doe",
			"roll_no": 51,
			"is_graduated": false,
			"date_of_birth": "2000-01-30"
		}
		}`)

		_, err := chroma.ParseJSONMap(oplog)
		if err == nil {
			t.Errorf("expected an error")
		}

		if !errors.Is(err, chroma.UnknownOp) {
			t.Errorf("got unexpected error: %v", err)
		}
	})
}

func TestParseArray(t *testing.T) {

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

	got, err := chroma.ParseJSONArray(oplog)
	if err != nil {
		t.Errorf("got unexpected error: %v", err)
	}

	want := []map[string]interface{}{
		{
			"op": "insert",
			"ns": "test.student",
			"o": map[string]interface{}{
				"_id":           "635b79e231d82a8ab1de863b",
				"name":          "Selena Miller",
				"roll_no":       float64(51),
				"is_graduated":  false,
				"date_of_birth": "2000-01-30",
			},
		},
		{
			"op": "insert",
			"ns": "test.student",
			"o": map[string]interface{}{
				"_id":           "14798c213f273a7ca2cf5174",
				"name":          "George Smith",
				"roll_no":       float64(21),
				"is_graduated":  true,
				"date_of_birth": "2001-03-23",
			},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func assertEqual(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func assertObjectEqual(t *testing.T, got, want map[string]interface{}) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

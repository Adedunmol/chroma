package chroma_test

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
			"op": "update",
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

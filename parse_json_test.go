package chroma_test

import (
	chroma "github.com/Adedunmol/chroma"
	"reflect"
	"testing"
)

func TestParseJSON(t *testing.T) {
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

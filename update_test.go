package main_test

import (
	chroma "github.com/Adedunmol/chroma"
	"reflect"
	"testing"
)

func TestUpdate(t *testing.T) {
	t.Run("test update", func(t *testing.T) {

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

		got := chroma.NewUpdate()
		data, err := chroma.ParseJSONMap(oplog)
		if err != nil {
			t.Fatal(err)
		}

		err = got.Parse(data)
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

		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v want %#v", got, want)
		}
	})

	t.Run("test removing of a field", func(t *testing.T) {

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

		got := chroma.NewUpdate()
		data, err := chroma.ParseJSONMap(oplog)
		if err != nil {
			t.Fatal(err)
		}

		err = got.Parse(data)
		if err != nil {
			t.Fatal(err)
		}

		want := chroma.Update{
			Op:        "d",
			Database:  "test",
			Table:     "student",
			Columns:   []chroma.KeyValue{{Key: "roll_no", Value: false}},
			Condition: chroma.KeyValue{Key: "_id", Value: "635b79e231d82a8ab1de863b"},
		}

		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v want %#v", got, want)
		}
	})
}

func TestUpdateString(t *testing.T) {
	t.Run("test update", func(t *testing.T) {

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

		err = update.Parse(data)
		if err != nil {
			t.Fatal(err)
		}

		got := update.String()

		want := "UPDATE student SET is_graduated = true WHERE _id = 635b79e231d82a8ab1de863b"

		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v want %#v", got, want)
		}
	})

	t.Run("test removing a field", func(t *testing.T) {

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

		update := chroma.NewUpdate()
		data, err := chroma.ParseJSONMap(oplog)
		if err != nil {
			t.Fatal(err)
		}

		err = update.Parse(data)
		if err != nil {
			t.Fatal(err)
		}

		got := update.String()

		want := "UPDATE student SET roll_no = NULL WHERE _id = 635b79e231d82a8ab1de863b"

		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v want %#v", got, want)
		}
	})
}

package chroma_test

import (
	"testing"
)

func TestUpdate(t *testing.T) {
	//oplog := []byte(`{
	//	"op": "i",
	//	"ns": "test.student",
	//	"o":  {
	//		"$v": 2,
	//		"diff": {
	//			"d": {
	//				"roll_no": false
	//			}
	//		}
	//	},
	//	"o2": {
	//		"_id": "635b79e231d82a8ab1de863b"
	//	}
	//}`)
	//
	//insert := chroma.NewInsert()
	//got, err := insert.Parse(oplog)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//want := chroma.Insert{
	//	Database: "test",
	//	Table:    "student",
	//	Columns: []chroma.KeyValue{
	//		{Key: "_id", Value: "635b79e231d82a8ab1de863b"},
	//		{Key: "name", Value: "John Doe"},
	//		{Key: "roll_no", Value: float64(51)},
	//		{Key: "is_graduated", Value: false},
	//		{Key: "date_of_birth", Value: "2000-01-30"},
	//	},
	//}
	//
	//if !reflect.DeepEqual(*got, want) {
	//	t.Errorf("got %#v want %#v", *got, want)
	//}
}

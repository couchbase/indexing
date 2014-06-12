package common

import (
	"reflect"
	"testing"
)

func TestComponentStat(t *testing.T) {
	ref := ComponentStat{"componentName": "indexer", "count": 10.0}
	out := ComponentStat{"componentName": "indexer"}
	if data, err := ref.Encode(); err != nil {
		t.Fatal(err)
	} else {
		if err := out.Decode(data); err != nil {
			t.Fatal(err)
		} else if reflect.DeepEqual(ref, out) == false {
			t.Fatal("mistmatch in component stats")
		}
	}
}

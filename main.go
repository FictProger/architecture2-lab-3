package main

import (
	"fmt"
	"github.com/FictProger/architecture2-lab-3/datastore"
)

func main() {
	db, err := datastore.NewDb("./", "current-data", false)
	if err != nil {
		return
	}
	for i := 1; i < 50000; i++ {
		db.Put(fmt.Sprintf("very_long_key_%d", i), "2222222222")
	}
	val, err := db.Get("very_long_key_4")
	print(val, err)
	//db.Put("key2", "val2")
	//db.Put("key3", "val3")
	//dbToMerge, err := datastore.NewDb("./segments", "segment_8674665223082153551")
	//db.Merge(dbToMerge)
	//for {
	//
	//}
}

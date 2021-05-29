package datastore

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"
)

func TestDb_Put(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir, "current-data", false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := [][]string {
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	outFile, err := os.Open(filepath.Join(dir, "current-data"))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if size1 * 2 != outInfo.Size() {
			t.Errorf("Unexpected size (%d vs %d)", size1, outInfo.Size())
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir, "current-data", false)
		if err != nil {
			t.Fatal(err)
		}

		for _, pair := range pairs {
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	t.Run("segments tests", func(t *testing.T) {
		db, err = NewDb(dir, "current-data", false)
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i < 40000; i++ {
			db.Put(fmt.Sprintf("very_long_key_%d", i), "2222222222")
		}

		if _, err := os.Stat(path.Join(db.dir, "/segments", "segment_1")); os.IsNotExist(err) {
			t.Fatal(err)
		}

		val, err := db.Get("very_long_key_4")
		if err != nil{
			t.Fatal(err)
		}
		if val == "" {
			t.Errorf("Bad value returned after reading from segment value very_long_key_4 must be in db")
		}
	})

	t.Run("merge test", func(t *testing.T) {
		db, err = NewDb(dir, "current-data", false)
		dbSegment, err := NewDb(dir, "segment", true)
		if err != nil {
			t.Fatal(err)
		}

		db.Put("key1", "val1")
		db.Put("key2", "val2")
		db.Put("key3", "val3")

		dbSegment.Put("key4", "val4")
		dbSegment.Put("key5", "val5")

		db.Merge(dbSegment)

		db.Close()

		db, err = NewDb(dir, "current-data", false)
		val, err := db.Get("key4")
		if err != nil {
			t.Fatal(err)
		}

		if val == "" {
			t.Errorf("Bad value returned after reading from db value \"key4\" must be in db")
		}

	})

}


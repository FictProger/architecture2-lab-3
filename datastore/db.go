package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)
const MAX_SIZE = 1 * 1024 * 1024

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type Db struct {
	out *os.File
	outPath string
	dir string
	outOffset int64
	index hashIndex
	lastSegmentNum int64
	segmentsDb []*Db
}

func NewDb(dir, outFileName string, forMerge bool) (*Db, error) {

	outputPath := filepath.Join(dir, outFileName)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	db := &Db{
		outPath: outputPath,
		out:     f,
		index:   make(hashIndex),
		dir:     dir,
	}


	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}

	if !forMerge {
		if _, err := os.Stat(path.Join(dir, "/segments")); os.IsNotExist(err) {
			err := os.Mkdir(path.Join(dir, "/segments"), os.ModePerm)
			if err != nil {
				return nil, err
			}
		}
		err = db.SetLastSegmentNumber()
		if err != nil {
			return nil, err
		}
		err = db.recoverSegments()
		if err != nil && err != io.EOF {
			return nil, err
		}
		go db.MergeRoutine()
	}

	return db, nil
}

const bufSize = 8192

func (db *Db) recover() error {
	input, err := os.Open(db.outPath)
	if err != nil {
		return err
	}
	defer input.Close()

	var buf [bufSize]byte
	in := bufio.NewReaderSize(input, bufSize)
	for err == nil {
		var (
			header, data []byte
			n int
		)
		header, err = in.Peek(bufSize)
		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)

		if size < bufSize {
			data = buf[:size]
		} else {
			data = make([]byte, size)
		}
		n, err = in.Read(data)

		if err == nil {
			if n != int(size) {
				return fmt.Errorf("corrupted file")
			}

			var e entry
			e.Decode(data)
			db.index[e.key] = db.outOffset
			db.outOffset += int64(n)
		}
	}
	return err
}

func (db *Db) recoverSegments() error {
	files, err := ioutil.ReadDir(path.Join(db.dir, "./segments"))
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		segmentDb, _ := NewDb(path.Join(db.dir, "./segments"), f.Name(), true)
		db.segmentsDb = append(db.segmentsDb, segmentDb)
	}
	return err
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	position, ok := db.index[key]
	if !ok {
		for _, segment := range db.segmentsDb {
			val, err := segment.Get(key)
			if err == nil {
				return val, nil
			}
		}
		return "", ErrNotFound
	}

	file, err := os.Open(db.outPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		for _, segment := range db.segmentsDb {
			val, err := segment.Get(key)
			if err == nil {
				return val, nil
			}
		}
		return "", err
	}
	return value, nil
}

func (db *Db) Put(key, value string) error {
	e := entry{
		key:   key,
		value: value,
	}
	n, err := db.out.Write(e.Encode())
	if err == nil {
		db.index[key] = db.outOffset
		db.outOffset += int64(n)
	}
	if stat, _ := db.out.Stat(); stat.Size() > MAX_SIZE {
		db.out.Close()
		os.Rename(db.outPath, path.Join(db.dir, fmt.Sprintf("/segments/segment_%d", db.lastSegmentNum)))
		os.Remove(db.outPath)
		f, err := os.OpenFile(db.outPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
		if err != nil { return err }
		segmentDb, err := NewDb(path.Join(db.dir, "./segments"), fmt.Sprintf("/segment_%d", db.lastSegmentNum), true)
		db.index = make(hashIndex)
		db.recover()
		db.segmentsDb = append(db.segmentsDb, segmentDb)
		db.out = f
		db.lastSegmentNum++
	}
	return err
}

func (db *Db) Merge (dbToMerge *Db) error {
	for key, _ := range dbToMerge.index {
		_, err := db.Get(key)
		if err != nil {
			val, err := dbToMerge.Get(key)
			if err != nil {
				if err == io.EOF {
					err := os.Remove(dbToMerge.outPath)
					if err != nil {
						return err
					}
				}
				return err
			}
			err = db.Put(key, val)
			if err != nil {
				return err
			}
		}
	}
	err := os.Remove(dbToMerge.outPath)
	if err != nil {
		return err
	}
	return nil
}

func (db *Db) MergeRoutine() error {
	for {
		files, err := ioutil.ReadDir(path.Join(db.dir, "./segments"))
		if err != nil {
			log.Fatal(err)
		}
		if len(files) < 2 {
			return nil
		}
		dbToMerge, err := NewDb(path.Join(db.dir, "./segments"), files[len(files) - 1].Name(), true)
		if err != nil {
			return err
		}
		_secondDb,err := NewDb(path.Join(db.dir, "./segments"), files[len(files) - 2].Name(), true)
		err = dbToMerge.Merge(_secondDb)
		if err != nil {
			return err
		}
		time.Sleep(time.Duration(20)*time.Second)
	}

	return nil
}

func (db *Db) SetLastSegmentNumber() error {
	max := int64(1)
	files, err := ioutil.ReadDir(path.Join(db.dir, "./segments"))
	if err != nil {
		return err
	}
	for _, f := range files {
		s := strings.Split(f.Name(), "_")
		if number, _ := strconv.ParseInt(s[1], 10, 64); number > max {
			max = number
		}
	}
	db.lastSegmentNum = max
	return nil
}
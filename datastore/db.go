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
	"sync"
)

const (
	MAX_SIZE = 1 * 1024 * 1024
	RECOVER_BUF_SIZE = 8192

	OS_OPEN_FLAG = os.O_APPEND|os.O_WRONLY|os.O_CREATE
	OS_OPEN_PERM = 0o600
)

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type Db struct {
	sync.Mutex

	out *os.File
	outPath string
	dir string
	segPath string
	outOffset int64
	index hashIndex
	lastSegmentNum int64
	segmentsDb []*Db
}

func NewDb(dir, outFileName string, forMerge bool) (*Db, error) {
	outputPath := filepath.Join(dir, outFileName)
	f, err := os.OpenFile(outputPath, OS_OPEN_FLAG, OS_OPEN_PERM)
	if err != nil {
		return nil, err
	}

	db := &Db{
		outPath: outputPath,
		out:     f,
		index:   make(hashIndex),
		dir:     dir,
		segPath: path.Join(dir, "/segments"),
	}

	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}

	if !forMerge {
		if _, err := os.Stat(db.segPath); os.IsNotExist(err) {
			if err := os.Mkdir(db.segPath, os.ModePerm); err != nil {
				return nil, err
			}
		}

		if err := db.SetLastSegmentNumber(); err != nil {
			return nil, err
		}

		if err := db.recoverSegments(); err != nil && err != io.EOF {
			return nil, err
		}

		go db.MergeRoutine()
	}

	return db, nil
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	db.Lock()
	defer db.Unlock()

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

	if _, err := file.Seek(position, 0); err != nil {
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
	db.Lock()
	defer db.Unlock()

	e := entry{ key, value }

	n, err := db.out.Write(e.Encode())
	if err == nil {
		db.index[key] = db.outOffset
		db.outOffset += int64(n)
	}

	if stat, _ := db.out.Stat(); stat.Size() > MAX_SIZE {
		db.out.Close()

		outNewName := path.Join(db.segPath, fmt.Sprintf("/segment_%d", db.lastSegmentNum))
		os.Rename(db.outPath, outNewName) //TODO check rename
		os.Remove(db.outPath)

		f, err := os.OpenFile(db.outPath, OS_OPEN_FLAG, OS_OPEN_PERM)
		if err != nil { 
			return err 
		}

		segmentDb, err := NewDb(db.segPath, fmt.Sprintf("/segment_%d", db.lastSegmentNum), true)
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
		if _, err := db.Get(key); err != nil {
			val, err := dbToMerge.Get(key)
			if err != nil {
				if err == io.EOF {
					if err := os.Remove(dbToMerge.outPath); err != nil {
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

	if err := os.Remove(dbToMerge.outPath); err != nil {
		return err
	}

	return nil
}

func (db *Db) MergeRoutine() error {
	for {
		files, err := ioutil.ReadDir(db.segPath)
		if err != nil {
			log.Fatal(err)
		}
		if len(files) < 2 {
			return nil
		}

		dbToMerge, err := NewDb(db.segPath, files[len(files) - 1].Name(), true)
		if err != nil {
			return err
		}

		_secondDb, err := NewDb(db.segPath, files[len(files) - 2].Name(), true)
		if err := dbToMerge.Merge(_secondDb); err != nil {
			return err
		}

		time.Sleep(time.Duration(20)*time.Second)
	}

	return nil
}

func (db *Db) SetLastSegmentNumber() error {
	files, err := ioutil.ReadDir(db.segPath)
	if err != nil {
		return err
	}
	
	max := int64(1)
	for _, f := range files {
		s := strings.Split(f.Name(), "_")
		if number, _ := strconv.ParseInt(s[1], 10, 64); number > max {
			max = number
		}
	}

	db.lastSegmentNum = max

	return nil
}

func (db *Db) recover() error {
	file, err := os.Open(db.outPath)
	if err != nil {
		return err
	}
	defer file.Close()

	var buf [RECOVER_BUF_SIZE]byte
	reader := bufio.NewReaderSize(file, RECOVER_BUF_SIZE)
	for {
		var (
			header, data []byte
			n int
		)

		header, err = reader.Peek(RECOVER_BUF_SIZE)
		isError := err != nil && err != io.EOF
		isEnd := err == io.EOF && len(header) == 0
		if isError || isEnd {
			return err
		}

		size := binary.LittleEndian.Uint32(header)
		if size < RECOVER_BUF_SIZE {
			data = buf[:size]
		} else {
			data = make([]byte, size)
		}

		n, err = reader.Read(data)
		if err != nil {
			return err
		} else if n != int(size) {
			return fmt.Errorf("corrupted file")
		}

		var e entry
		e.Decode(data)
		db.index[e.key] = db.outOffset
		db.outOffset += int64(n)
	}

	return nil
}

func (db *Db) recoverSegments() error {
	files, err := ioutil.ReadDir(db.segPath)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		segmentDb, _ := NewDb(db.segPath, f.Name(), true)
		db.segmentsDb = append(db.segmentsDb, segmentDb)
	}

	return err
}

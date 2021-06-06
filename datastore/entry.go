package datastore

import (
	"bufio"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
)

type entry struct {
	key, value string
}

func (e *entry) Encode() []byte {
	kl := len(e.key)
	vl := len(e.value)
	hash := sha1.Sum([]byte(e.value))
	hl := len(hash)

	size := kl + vl + hl + 12
	res := make([]byte, size)

	binary.LittleEndian.PutUint32(res, uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	copy(res[8:], e.key)
	copy(res[kl+8:], string(hash[:]))
	binary.LittleEndian.PutUint32(res[kl+hl+8:], uint32(vl))
	copy(res[kl+hl+12:], e.value)

	return res
}

func (e *entry) Decode(input []byte) {
	kl := binary.LittleEndian.Uint32(input[4:])
	keyBuf := make([]byte, kl)
	copy(keyBuf, input[8:kl+8])
	e.key = string(keyBuf)

	hl := len(sha1.Sum([]byte{}))

	vl := binary.LittleEndian.Uint32(input[kl+8+uint32(hl):])
	valBuf := make([]byte, vl)
	copy(valBuf, input[kl+12+uint32(hl):kl+12+uint32(hl)+vl])
	e.value = string(valBuf)
}

func readValue(in *bufio.Reader) (string, error) {
	header, err := in.Peek(8)
	if err != nil {
		return "", err
	}
	keySize := int(binary.LittleEndian.Uint32(header[4:]))
	_, err = in.Discard(keySize + 8)
	if err != nil {
		return "", err
	}
	
	hl := len(sha1.Sum([]byte{}))
	hash := make([]byte, hl)
	n, err := in.Read(hash)
	if err != nil {
		return "", err
	}

	header, err = in.Peek(4)
	if err != nil {
		return "", err
	}
	valSize := int(binary.LittleEndian.Uint32(header))
	_, err = in.Discard(4)
	if err != nil {
		return "", err
	}

	data := make([]byte, valSize)
	n, err = in.Read(data)
	if err != nil {
		return "", err
	}
	if n != valSize {
		return "", fmt.Errorf("can't read value bytes (read %d, expected %d)", n, valSize)
	}

	dataHash := sha1.Sum(data)
	if string(dataHash[:]) != string(hash) {
		return "", fmt.Errorf("wrong hash")
	}

	return string(data), nil
}

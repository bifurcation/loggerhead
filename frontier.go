package loggerhead

import (
	"crypto"
	_ "crypto/sha256"
	"encoding/binary"
	"fmt"
)

var (
	hash      = crypto.SHA256
	endian    = binary.BigEndian
	entrySize = 8 + hash.Size()
)

func leafHash(d []byte) []byte {
	h := hash.New()
	h.Write([]byte{0x00})
	h.Write(d)
	return h.Sum(nil)
}

func pairHash(d1, d2 []byte) []byte {
	h := hash.New()
	h.Write([]byte{0x01})
	h.Write(d1)
	h.Write(d2)
	return h.Sum(nil)
}

type frontierEntry struct {
	SubtreeSize uint64
	Value       []byte
}

type frontier []frontierEntry

func (f *frontier) Compact() {
	if len(*f) < 2 {
		return
	}

	n := len(*f)
	for n > 1 {
		last := (*f)[n-1]
		nextToLast := (*f)[n-2]

		if last.SubtreeSize != nextToLast.SubtreeSize {
			break
		}

		n -= 1
		(*f)[n-1] = frontierEntry{
			SubtreeSize: last.SubtreeSize + nextToLast.SubtreeSize,
			Value:       pairHash(nextToLast.Value, last.Value),
		}
	}

	(*f) = (*f)[:n]
}

func (f *frontier) Add(v []byte) {
	leaf := leafHash(v)
	*f = append(*f, frontierEntry{1, leaf})
	f.Compact()
}

func (f frontier) Size() uint64 {
	size := uint64(0)
	for _, entry := range f {
		size += entry.SubtreeSize
	}
	return size
}

func (f frontier) Head() []byte {
	if len(f) == 1 {
		return f[0].Value
	}

	n := len(f) - 2
	curr := pairHash(f[n].Value, f[n+1].Value)
	for n > 0 {
		n -= 1
		curr = pairHash(f[n].Value, curr)
	}

	return curr
}

func (f frontier) Marshal() []byte {
	buf := make([]byte, len(f)*entrySize)

	for i, entry := range f {
		start := entrySize * i
		end := start + entrySize
		endian.PutUint64(buf[start:], entry.SubtreeSize)
		copy(buf[start+8:end], entry.Value)
	}

	return buf
}

func (f *frontier) Unmarshal(buf []byte) error {
	if len(buf)%entrySize != 0 {
		return fmt.Errorf("Malformed frontier: Incorrect size")
	}

	*f = make([]frontierEntry, len(buf)/entrySize)
	for i := range *f {
		start := entrySize * i
		end := start + entrySize
		(*f)[i].SubtreeSize = endian.Uint64(buf[start:])
		(*f)[i].Value = make([]byte, hash.Size())
		copy((*f)[i].Value, buf[start+8:end])
	}

	return nil
}

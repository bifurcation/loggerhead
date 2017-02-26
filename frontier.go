package loggerhead

import (
	"crypto"
	_ "crypto/sha256"
	"sort"
)

// Merkle tree primitives
const hash = crypto.SHA256

var hashCounter = 0

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

	hashCounter += 1
	return h.Sum(nil)
}

type frontierEntry struct {
	SubtreeSize uint64
	Value       []byte
}

type frontier struct {
	entries []frontierEntry
}

func (f frontier) Len() int {
	return len(f.entries)
}

func (f frontier) Less(i, j int) bool {
	return f.entries[i].SubtreeSize > f.entries[j].SubtreeSize
}

func (f *frontier) Swap(i, j int) {
	temp := f.entries[i]
	f.entries[i] = f.entries[j]
	f.entries[j] = temp
}

func (f *frontier) Sort() {
	sort.Sort(f)
}

func (f *frontier) Compact() {
	n := len(f.entries)

	if n < 2 {
		return
	}

	for n > 1 {
		last := f.entries[n-1]
		nextToLast := f.entries[n-2]

		if last.SubtreeSize != nextToLast.SubtreeSize {
			break
		}

		n -= 1
		f.entries[n-1] = frontierEntry{
			SubtreeSize: last.SubtreeSize + nextToLast.SubtreeSize,
			Value:       pairHash(nextToLast.Value, last.Value),
		}
	}

	f.entries = f.entries[:n]
}

func (f *frontier) Add(v []byte) {
	leaf := leafHash(v)
	f.entries = append(f.entries, frontierEntry{1, leaf})
	f.Compact()
}

func (f frontier) Size() uint64 {
	size := uint64(0)
	for _, entry := range f.entries {
		size += entry.SubtreeSize
	}
	return size
}

func (f frontier) Head() []byte {
	if len(f.entries) == 1 {
		return f.entries[0].Value
	}

	n := len(f.entries) - 2
	curr := pairHash(f.entries[n].Value, f.entries[n+1].Value)
	for n > 0 {
		n -= 1
		curr = pairHash(f.entries[n].Value, curr)
	}

	return curr
}

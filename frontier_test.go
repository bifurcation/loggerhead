package loggerhead

import (
	"bytes"
	"testing"
)

const (
	testSamples = 10
)

func round2(n int) int {
	k := 1
	for k < n {
		k <<= 1
	}
	return k >> 1
}

func merkleTreeHead(d [][]byte) []byte {
	if len(d) == 1 {
		return d[0]
	}

	k := round2(len(d))
	l := merkleTreeHead(d[:k])
	r := merkleTreeHead(d[k:])
	return pairHash(l, r)
}

func TestFrontier(t *testing.T) {
	f := frontier{}
	d := [][]byte{}

	for i := uint64(0); i < testSamples; i += 1 {
		v := []byte{byte(i)}
		d = append(d, leafHash(v))
		f.Add(v)

		if s := f.Size(); s != i+1 {
			t.Fatalf("Incorrect size [%d] [%d] != [%d]", i, s, i+1)
		}

		h := merkleTreeHead(d)
		if fh := f.Head(); !bytes.Equal(h, fh) {
			t.Fatalf("Incorrect head [%d] [%x] != [%x]", i, fh, h)
		}
	}
}

func TestFrontierMarshalUnmarshal(t *testing.T) {
	f := frontier{}
	for i := uint64(0); i < testSamples; i += 1 {
		v := []byte{byte(i)}
		f.Add(v)
	}

	buf := f.Marshal()

	f2 := frontier{}
	err := f2.Unmarshal(buf)
	if err != nil {
		t.Fatalf("Failed marshal/unmarshal round trip: %v", err)
	}

	if len(f) != len(f2) {
		t.Fatalf("Unmarshaled frontier has different size [%d] [%d]", len(f), len(f2))
	}

	for i := range f2 {
		if f[i].SubtreeSize != f2[i].SubtreeSize {
			t.Fatalf("Unmarshaled frontier has different subtree size [%d] [%d]", f[i].SubtreeSize, f2[i].SubtreeSize)
		}

		if !bytes.Equal(f[i].Value, f2[i].Value) {
			t.Fatalf("Unmarshaled frontier has different value [%x] [%x]", f[i].Value, f2[i].Value)
		}
	}
}

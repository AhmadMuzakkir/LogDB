package logdb

import (
	"bytes"
	"crypto/rand"
	"os"
	"testing"
)

func TestDatabase(t *testing.T) {
	path := "./test"

	os.Remove(path)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	key := []byte("key1")
	value := []byte("bob")

	err = db.Set(key, value)
	if err != nil {
		t.Fatal(err)
	}

	valueGot, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(valueGot, value) {
		t.Errorf("want value %s, got %s", value, valueGot)
	}

	// Override the value and test again.

	value = []byte("alice")

	err = db.Set(key, value)
	if err != nil {
		t.Fatal(err)
	}

	valueGot, err = db.Get([]byte("key1"))
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(valueGot, value) {
		t.Errorf("want value %v, got %v", value, valueGot)
	}

	// Test not exists

	valueGot, err = db.Get([]byte("not exist"))
	if err != ErrNotExists {
		t.Fatalf("want err %v, got %v", ErrNotExists, err)
	}
	if valueGot != nil {
		t.Fatalf("want value nil, got %x", valueGot)
	}

	// Test Delete

	if err := db.Delete([]byte("key1")); err != nil {
		t.Fatal(err)
	}

	valueGot, err = db.Get([]byte("key1"))
	if err != ErrNotExists {
		t.Fatalf("want err %v, got %v", ErrNotExists, err)
	}
	if valueGot != nil {
		t.Fatalf("want value nil, got %v", valueGot)
	}
}

func TestIndex(t *testing.T) {
	path := "./test"

	os.Remove(path)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	var key [16]byte
	var val [32]byte

	// Generate a list of random entry.
	var list [][2][]byte
	for i := 0; i < 100; i++ {
		_, err := rand.Read(key[:])
		if err != nil {
			t.Fatal(err)
		}

		_, err = rand.Read(val[:])
		if err != nil {
			t.Fatal(err)
		}

		list = append(list, [2][]byte{
			key[:], val[:],
		})

		err = db.Set(key[:], val[:])
		if err != nil {
			t.Fatal(err)
		}
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}

	for i := range list {
		key := list[i][0]

		gotVal, err := db.Get(key)
		if err != nil {
			t.Fatalf("read key %x: %s", key, err)
		}

		wantVal := list[i][1]

		if !bytes.Equal(gotVal, wantVal) {
			t.Fatalf("want value %x, got %x", wantVal, gotVal)
		}
	}
}

package logdb

import (
	"io"
	"io/ioutil"
)

type entry struct {
	value []byte
	key   []byte
}

func decodeEntry(r io.Reader, h header, getVal bool) (*entry, int, error) {
	var e entry

	e.key = make([]byte, h.klen)
	n, err := io.ReadFull(r, e.key[:])
	if err != nil {
		return nil, n, err
	}
	bytesRead := n

	// Discard the value if the entry is deleted or the getVal is false.
	if h.deleted() || !getVal {
		n, err := io.CopyN(ioutil.Discard, r, int64(h.vlen))

		if err != nil {
			return nil, bytesRead, err
		}
		bytesRead += int(n)
	} else {
		e.value = make([]byte, h.vlen)
		_, err = io.ReadFull(r, e.value[:])
		if err != nil {
			return nil, bytesRead, err
		}
		bytesRead += n
	}

	return &e, bytesRead, nil
}

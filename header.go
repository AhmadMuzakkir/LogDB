package logdb

import (
	"encoding/binary"
	"io"
)

const MaxHeaderSize = 11
const MinHeaderSize = 3

type header struct {
	// The right most one bit is for tombstone.
	// tombstone: 1 = deleted, 0 = not deleted.
	meta byte
	klen uint32
	vlen uint32

	// Offset to the entry.
	// Internal use only. Not stored on disk.
	offset int64
}

func (h header) encode(buf []byte) int {
	buf[0] = h.meta
	index := 1

	index += binary.PutUvarint(buf[index:], uint64(h.klen))
	index += binary.PutUvarint(buf[index:], uint64(h.vlen))

	return index
}

func (h *header) decodeFrom(r io.ByteReader) error {
	var err error

	h.meta, err = r.ReadByte()
	if err != nil {
		return err
	}

	klen, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}

	h.klen = uint32(klen)

	vlen, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}

	h.vlen = uint32(vlen)

	return nil
}

func (h header) deleted() bool {
	return h.meta&1 == 1
}

package snapio

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"

	"github.com/tinylib/msgp/msgp"
	"github.com/viciious/go-tarantool"
)

type SpaceData struct {
	Space  uint
	Tuples [][]interface{}
}

func WriteV12Snapshot(fd io.Writer, data []*SpaceData) error {
	header := `SNAP
0.12
Version: 2.2.1-3-g878e2a42c
Instance: d31ad582-66a6-4b18-96f7-278a7a33ad20
VClock: {1: 10001}

`

	w := bufio.NewWriter(fd)
	defer w.Flush()

	_, err := w.WriteString(header)
	if err != nil {
		return err
	}

	var lsn uint
	for _, s := range data {
		space := s.Space
		if space == 0 {
			space = 10024
		}
		for _, t := range s.Tuples {
			var arr []byte

			arr = msgp.AppendMapHeader(arr, 1)
			arr = msgp.AppendUint(arr, tarantool.KeyLSN)
			arr = msgp.AppendUint(arr, uint(lsn+1))

			arr = msgp.AppendMapHeader(arr, 2)
			arr = msgp.AppendUint(arr, tarantool.KeySpaceNo)
			arr = msgp.AppendUint(arr, uint(space))
			arr = msgp.AppendUint(arr, tarantool.KeyTuple)
			arr, _ = msgp.AppendIntf(arr, t)

			var lenbuf []byte
			lenbuf = msgp.AppendUint32(lenbuf, uint32(len(arr)))

			if err = binary.Write(w, binary.BigEndian, uint32(XRowFixedHeaderMagic)); err != nil {
				return err
			}
			if _, err = w.Write(lenbuf); err != nil {
				return err
			}
			if _, err = w.Write(bytes.Repeat([]byte{'\x00'}, XRowFixedHeaderSize-len(lenbuf)-4)); err != nil {
				return err
			}
			if _, err = w.Write(arr); err != nil {
				return err
			}
		}
	}

	if err = binary.Write(w, binary.BigEndian, uint32(XRowFixedHeaderEof)); err != nil {
		return err
	}

	return nil
}

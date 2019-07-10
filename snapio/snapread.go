package snapio

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"

	"github.com/tinylib/msgp/msgp"
	"github.com/viciious/go-tarantool"
)

func ReadSnapshot(rs io.Reader, tuplecb func(space uint, tuple []interface{})) error {
	in := bufio.NewReaderSize(rs, 100*1024)

	header := make([]byte, 4)
	headerLen, err := in.Read(header)
	if err != nil {
		return err
	}
	if headerLen < 4 {
		return errors.New("Truncated snapshot")
	}

	if string(header[:4]) != "SNAP" {
		return errors.New("Missing SNAP header")
	}

	prevNL := false
	for headerLen := 0; ; headerLen++ {
		if headerLen == 1024 {
			return errors.New("Malformed snapshot")
		}

		b, err := in.ReadByte()
		if err != nil {
			return err
		}

		nl := (b == 0xa)
		if nl && prevNL {
			break
		}
		prevNL = nl
	}

	var fixh [XRowFixedHeaderSize]byte
	xrow := make([]byte, 0, 1024)

	for {
		var n int
		var ulen uint

		if n, err = io.ReadFull(in, fixh[:]); err == io.EOF {
			return nil
		}

		if n == 4 && binary.BigEndian.Uint32(fixh[0:4]) == XRowFixedHeaderEof {
			return nil
		}

		if err != nil {
			return err
		}

		if binary.BigEndian.Uint32(fixh[0:4]) != XRowFixedHeaderMagic {
			return errors.New("Bad xrow magic")
		}

		buf := fixh[4:]
		if ulen, buf, err = msgp.ReadUintBytes(buf); err != nil {
			return err
		}

		len := int(ulen)
		if len > cap(xrow) {
			xrow = make([]byte, 0, len+1024)
		}

		if _, err = io.ReadFull(in, xrow[:len]); err != nil {
			return err
		}

		var ml uint32

		buf = xrow[:len]
		if buf, err = msgp.Skip(buf); err != nil {
			return err
		}

		if ml, buf, err = msgp.ReadMapHeaderBytes(buf); err != nil {
			return err
		}

		var space uint
		var tuple []interface{}

		for ; ml > 0; ml-- {
			var cd uint
			if cd, buf, err = msgp.ReadUintBytes(buf); err != nil {
				return err
			}

			switch cd {
			case tarantool.KeySpaceNo:
				if space, buf, err = msgp.ReadUintBytes(buf); err != nil {
					return err
				}
			case tarantool.KeyTuple:
				var tinf interface{}
				if tinf, buf, err = msgp.ReadIntfBytes(buf); err != nil {
					return err
				}
				tuple = tinf.([]interface{})
			}
		}

		if space == 0 || tuple == nil {
			continue
		}

		tuplecb(space, tuple)
	}

	return nil
}

package snapio

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/tinylib/msgp/msgp"
	"github.com/viciious/go-tarantool"
)

func ReadSnapshotPacked(rs io.Reader, tuplecb func(space uint, tuple []byte) error) error {
	var err error
	var version int

	in := bufio.NewReaderSize(rs, 16*1024*1024)

	for ln := 0; ; ln++ {
		if ln > 0 {
			nl, err := in.Peek(1)
			if err != nil {
				return err
			}
			if nl[0] == 0xa {
				in.ReadByte()
				break
			}
		}

		lineb, _, err := in.ReadLine()
		if err != nil {
			return err
		}

		line := string(lineb)
		switch ln {
		case 0:
			if line != "SNAP" {
				return errors.New("Missing SNAP header")
			}
		case 1:
			if line == "0.12" {
				version = 12
			} else if line == "0.13" {
				version = 13
			} else {
				return fmt.Errorf("Unknown snapshot version: %s", line)
			}
		}
	}

	var fixh [XRowFixedHeaderSize]byte
	var xrow, zrow []byte
	var zr *zstd.Decoder

	if version != 12 {
		if zr, err = zstd.NewReader(nil); err != nil {
			return err
		}
	}

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

		compressed := false
		if zr != nil {
			compressed = binary.BigEndian.Uint32(fixh[0:4]) == ZRowFixedHeaderMagic
		}

		if !compressed && binary.BigEndian.Uint32(fixh[0:4]) != XRowFixedHeaderMagic {
			return fmt.Errorf("Bad xrow magic %0X", fixh[0:4])
		}

		buf := fixh[4:]
		if ulen, buf, err = msgp.ReadUintBytes(buf); err != nil {
			return err
		}

		rlen := int(ulen)
		if rlen <= in.Buffered() {
			if buf, err = in.Peek(rlen); err != nil {
				return err
			}
			if _, err = in.Discard(rlen); err != nil {
				return err
			}
		} else {
			if rlen > cap(zrow) {
				zrow = make([]byte, 0, rlen+1024)
			}
			if _, err = io.ReadFull(in, zrow[:rlen]); err != nil {
				return err
			}
			buf = zrow[:rlen]
		}

		if compressed {
			if xrow, err = zr.DecodeAll(buf, xrow); err != nil {
				return err
			}
			buf = xrow
			xrow = xrow[:0]
		}

		for len(buf) > 0 {
			// meta map: timestamp, lsn, etc
			if buf, err = msgp.Skip(buf); err != nil {
				return err
			}

			var ml uint32
			if ml, buf, err = msgp.ReadMapHeaderBytes(buf); err != nil {
				return err
			}

			var space uint
			var tuple []byte

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
					var curbuf = buf
					if buf, err = msgp.Skip(buf); err != nil {
						return err
					}
					tuple = curbuf[:len(curbuf)-len(buf)]
				default:
					if buf, err = msgp.Skip(buf); err != nil {
						return err
					}
				}
			}

			if space == 0 || tuple == nil {
				continue
			}

			if err = tuplecb(space, tuple); err != nil {
				return err
			}
		}
	}

	return nil
}

func ReadSnapshot(rs io.Reader, tuplecb func(space uint, tuple []interface{}) error) error {
	return ReadSnapshotPacked(rs, func(space uint, buf []byte) error {
		var err error
		var tinf interface{}
		if tinf, _, err = msgp.ReadIntfBytes(buf); err != nil {
			return err
		}
		return tuplecb(space, tinf.([]interface{}))
	})
}

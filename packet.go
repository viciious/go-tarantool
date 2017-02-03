package tarantool

import (
	"bytes"
	"errors"
	"io"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Packet struct {
	Code      uint32
	Error     error
	Data      [][]interface{}
	request   interface{}
	requestID uint32
	buf       *bytes.Buffer // read buffer. For delayer unpack
}

func readMessage(r io.Reader) ([]byte, error) {
	var err error
	header := make([]byte, PacketLengthBytes)

	if _, err = io.ReadAtLeast(r, header, PacketLengthBytes); err != nil {
		return nil, err
	}

	if header[0] != 0xce {
		return nil, errors.New("Wrong reponse header")
	}

	bodyLength := (int(header[1]) << 24) +
		(int(header[2]) << 16) +
		(int(header[3]) << 8) +
		int(header[4])

	if bodyLength == 0 {
		return nil, errors.New("Packet should not be 0 length")
	}

	body := make([]byte, bodyLength)
	_, err = io.ReadAtLeast(r, body, bodyLength)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func readMessageToBuffer(r io.Reader, buffer []byte) ([]byte, error) {
	var err error
	if _, err = io.ReadAtLeast(r, buffer[:PacketLengthBytes], PacketLengthBytes); err != nil {
		return nil, err
	}

	if buffer[0] != 0xce {
		return nil, errors.New("Wrong reponse header")
	}

	bodyLength := (int(buffer[1]) << 24) +
		(int(buffer[2]) << 16) +
		(int(buffer[3]) << 8) +
		int(buffer[4])

	if bodyLength == 0 {
		return nil, errors.New("Packet should not be 0 length")
	}

	var body []byte
	if bodyLength <= len(buffer)-PacketLengthBytes {
		body = buffer[PacketLengthBytes : bodyLength+PacketLengthBytes]
	} else {
		body = make([]byte, bodyLength)
	}
	_, err = io.ReadAtLeast(r, body, bodyLength)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func msgpackDecodeBody(d *msgpack.Decoder) ([][]interface{}, error) {
	n, err := d.DecodeSliceLen()
	if err != nil {
		return nil, err
	}

	if n == -1 {
		return nil, nil
	}

	s := make([][]interface{}, n)
	for i := 0; i < n; i++ {
		v, err := d.DecodeSlice()
		if err != nil {
			return nil, err
		}
		s[i] = v
	}

	return s, nil
}

func (pack *Packet) decodeHeader(r *bytes.Buffer) (err error) {
	var l int
	d := msgpack.NewDecoder(r)
	if l, err = d.DecodeMapLen(); err != nil {
		return
	}
	for ; l > 0; l-- {
		var cd int
		if cd, err = d.DecodeInt(); err != nil {
			return
		}
		switch cd {
		case KeySync:
			if pack.requestID, err = d.DecodeUint32(); err != nil {
				return
			}
		case KeyCode:
			if pack.Code, err = d.DecodeUint32(); err != nil {
				return
			}
		case KeySchemaID:
			if _, err = d.DecodeUint32(); err != nil {
				return
			}
		default:
			if _, err = d.DecodeInterface(); err != nil {
				return
			}
		}
	}
	return nil
}

func (pack *Packet) decodeBody(r *bytes.Buffer) (err error) {
	if r.Len() > 2 {

		var l int
		d := msgpack.NewDecoder(r)
		if l, err = d.DecodeMapLen(); err != nil {
			return
		}
		for ; l > 0; l-- {
			var cd int
			if cd, err = d.DecodeInt(); err != nil {
				return
			}
			switch cd {
			case KeyData:
				value, err := d.DecodeInterface()
				if err != nil {
					return err
				}
				v := value.([]interface{})

				pack.Data = make([]([]interface{}), len(v))
				for i := 0; i < len(v); i++ {
					pack.Data[i] = v[i].([]interface{})
				}
			case KeyError:
				errorMessage, err := d.DecodeString()
				if err != nil {
					return err
				}
				pack.Error = NewQueryError(errorMessage)
			default:
				switch pack.Code {
					case SelectRequest:
						q := &Select{}
						if err = q.Unpack(d); err != nil {
							return
						}
						pack.request = q
					case AuthRequest:
						q := &Auth{}
						if err = q.Unpack(d); err != nil {
							return
						}
						pack.request = q
					default:
						if _, err = d.DecodeInterface(); err != nil {
							return
						}
				}
			}
		}
	}
	return
}

func decodePacket(r *bytes.Buffer) (*Packet, error) {
	pack := &Packet{}
	err := pack.decodeHeader(r)
	if err != nil {
		return nil, err
	}

	err = pack.decodeBody(r)
	if err != nil {
		return nil, err
	}
	return pack, nil
}

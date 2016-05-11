package tarantool

import (
	"bytes"
	"errors"
	"io"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Response struct {
	Code       uint32
	Error      error
	Data       []interface{}
	requestID  uint32
	poolRecord *PoolRecord // read buffer. For delayer unpack
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
		return nil, errors.New("Response should not be 0 length")
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
		return nil, errors.New("Response should not be 0 length")
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

func (resp *Response) decodeHeader(r *bytes.Buffer) (err error) {
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
			if resp.requestID, err = d.DecodeUint32(); err != nil {
				return
			}
		case KeyCode:
			if resp.Code, err = d.DecodeUint32(); err != nil {
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

func (resp *Response) decodeBody(r *bytes.Buffer) (err error) {
	if r.Len() > 2 {
		var body map[int]interface{}
		d := msgpack.NewDecoder(r)
		if err = d.Decode(&body); err != nil {
			return err
		}
		if body[KeyData] != nil {
			resp.Data = body[KeyData].([]interface{})
		}
		if body[KeyError] != nil {
			resp.Error = errors.New(body[KeyError].(string))
		}
	}
	return
}

func decodeResponse(r *bytes.Buffer) (*Response, error) {
	resp := &Response{}
	err := resp.decodeHeader(r)
	if err != nil {
		return nil, err
	}

	err = resp.decodeBody(r)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func read(r io.Reader) (*Response, error) {
	body, err := readMessage(r)
	if err != nil {
		return nil, err
	}

	response, err := decodeResponse(bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	return response, nil
}

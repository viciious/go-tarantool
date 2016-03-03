package tnt

import (
	"bytes"
	"errors"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Response struct {
	Code      uint32
	Error     error
	Data      []Tuple
	requestID uint32
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
			// resp.Data = body[KeyData].([]interface{})
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

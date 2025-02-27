package tarantool

import (
	"errors"
	"fmt"

	"github.com/tinylib/msgp/msgp"
)

type resultUnmarshalMode int

const (
	ResultDefaultMode resultUnmarshalMode = iota
	ResultAsRawData
	ResultAsDataWithFallback

	ResultAsData = ResultDefaultMode
)

type Result struct {
	ErrorCode uint
	Error     error

	// Data is a parsed array of tuples.
	// Keep in mind that by default if original data structure it's unmarhsalled from
	// has a different type it's forcefully wrapped to become array of tuples. This might be
	// the case for call17 or eval commands. You may overwrite this behavior by specifying
	// desired unmarshal mode.
	Data    [][]interface{}
	RawData interface{}

	unmarshalMode resultUnmarshalMode
}

func (r *Result) GetCommandID() uint {
	if r.Error != nil {
		return r.ErrorCode | ErrorFlag
	}
	return r.ErrorCode
}

// MarshalMsg implements msgp.Marshaler
func (r *Result) MarshalMsg(b []byte) (o []byte, err error) {
	o = b
	if r.Error != nil {
		o = msgp.AppendMapHeader(o, 1)
		o = msgp.AppendUint(o, KeyError)
		o = msgp.AppendString(o, r.Error.Error())
	} else {
		o = msgp.AppendMapHeader(o, 1)
		o = msgp.AppendUint(o, KeyData)
		switch {
		case r.Data != nil:
			if o, err = msgp.AppendIntf(o, r.Data); err != nil {
				return nil, err
			}
		case r.RawData != nil:
			if o, err = msgp.AppendIntf(o, r.RawData); err != nil {
				return nil, err
			}
		default:
			o = msgp.AppendArrayHeader(o, 0)
		}
	}

	return o, nil
}

// UnmarshalMsg implements msgp.Unmarshaler
func (r *Result) UnmarshalMsg(data []byte) (buf []byte, err error) {
	var l uint32
	var errorMessage string

	buf = data

	// Tarantool >= 1.7.7 sends periodic heartbeat messages without body
	if len(buf) == 0 && r.ErrorCode == OKCommand {
		return buf, nil
	}
	l, buf, err = msgp.ReadMapHeaderBytes(buf)

	if err != nil {
		return
	}

	for ; l > 0; l-- {
		var cd uint

		if cd, buf, err = msgp.ReadUintBytes(buf); err != nil {
			return
		}

		switch cd {
		case KeyData:
			switch r.unmarshalMode {
			case ResultAsDataWithFallback:
				obuf := buf
				r.Data, buf, err = r.UnmarshalTuplesArray(buf, false)
				if err != nil && errors.As(err, &msgp.TypeError{}) {
					r.RawData, buf, err = msgp.ReadIntfBytes(obuf)
				}
			case ResultAsRawData:
				r.RawData, buf, err = msgp.ReadIntfBytes(buf)
			default:
				r.Data, buf, err = r.UnmarshalTuplesArray(buf, true)
			}

			if err != nil {
				return
			}
		case KeyError:
			errorMessage, buf, err = msgp.ReadStringBytes(buf)
			if err != nil {
				return
			}
			r.Error = NewQueryError(r.ErrorCode, errorMessage)
		default:
			if buf, err = msgp.Skip(buf); err != nil {
				return
			}
		}
	}

	return
}

func (*Result) UnmarshalTuplesArray(buf []byte, force bool) ([][]interface{}, []byte, error) {
	var (
		dl, tl uint32
		i, j   uint32
		val    interface{}
		err    error
	)

	if dl, buf, err = msgp.ReadArrayHeaderBytes(buf); err != nil {
		return nil, nil, err
	}

	data := make([][]interface{}, dl)
	for i = 0; i < dl; i++ {
		obuf := buf
		if tl, buf, err = msgp.ReadArrayHeaderBytes(buf); err != nil {
			buf = obuf
			if _, ok := err.(msgp.TypeError); ok && force {
				if val, buf, err = msgp.ReadIntfBytes(buf); err != nil {
					return nil, nil, err
				}
				data[i] = []interface{}{val}
				continue
			}
			return nil, nil, err
		}

		data[i] = make([]interface{}, tl)
		for j = 0; j < tl; j++ {
			if data[i][j], buf, err = msgp.ReadIntfBytes(buf); err != nil {
				return nil, nil, err
			}
		}
	}

	return data, buf, nil
}

func (r *Result) String() string {
	switch {
	case r == nil:
		return "Result <nil>"
	case r.Error != nil:
		return fmt.Sprintf("Result ErrCode:%v, Err: %v", r.ErrorCode, r.Error)
	case r.Data != nil:
		return fmt.Sprintf("Result Data:%#v", r.Data)
	case r.RawData != nil:
		return fmt.Sprintf("Result RawData:%#v", r.RawData)
	default:
		return ""
	}
}

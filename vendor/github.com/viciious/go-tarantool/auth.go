package tarantool

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"

	"github.com/tinylib/msgp/msgp"
)

type Auth struct {
	User         string
	Password     string
	GreetingAuth []byte
}

var _ Query = (*Auth)(nil)

const authHash = "chap-sha1"
const scrambleSize = sha1.Size // == 20

// copy-paste from go-tarantool
func scramble(encodedSalt []byte, pass string) (scramble []byte, err error) {
	/* ==================================================================
	    According to: http://tarantool.org/doc/dev_guide/box-protocol.html

	    salt = base64_decode(encoded_salt);
	    step_1 = sha1(password);
	    step_2 = sha1(step_1);
	    step_3 = sha1(salt, step_2);
	    scramble = xor(step_1, step_3);
	    return scramble;

	===================================================================== */

	salt, err := base64.StdEncoding.DecodeString(string(encodedSalt))
	if err != nil {
		return
	}
	step1 := sha1.Sum([]byte(pass))
	step2 := sha1.Sum(step1[0:])
	hash := sha1.New() // may be create it once per connection ?
	hash.Write(salt[0:scrambleSize])
	hash.Write(step2[0:])
	step3 := hash.Sum(nil)

	return xor(step1[0:], step3[0:], scrambleSize), nil
}

func xor(left, right []byte, size int) []byte {
	result := make([]byte, size)
	for i := 0; i < size; i++ {
		result[i] = left[i] ^ right[i]
	}
	return result
}

func (auth *Auth) GetCommandID() uint {
	return AuthCommand
}

// MarshalMsg implements msgp.Marshaler
func (auth *Auth) MarshalMsg(b []byte) (o []byte, err error) {
	scr, err := scramble(auth.GreetingAuth, auth.Password)
	if err != nil {
		return nil, fmt.Errorf("auth: scrambling failure: %s", err.Error())
	}

	o = b
	o = msgp.AppendMapHeader(o, 2)
	o = msgp.AppendUint(o, KeyUserName)
	o = msgp.AppendString(o, auth.User)

	o = msgp.AppendUint(o, KeyTuple)
	o = msgp.AppendArrayHeader(o, 2)
	o = msgp.AppendString(o, authHash)
	o = msgp.AppendBytes(o, scr)

	return o, nil
}

// UnmarshalMsg implements msgp.Unmarshaler
func (auth *Auth) UnmarshalMsg(data []byte) (buf []byte, err error) {
	var i, l uint32
	var k uint

	buf = data
	if i, buf, err = msgp.ReadMapHeaderBytes(buf); err != nil {
		return
	}

	for ; i > 0; i-- {
		if k, buf, err = msgp.ReadUintBytes(buf); err != nil {
			return
		}

		switch k {
		case KeyUserName:
			if auth.User, buf, err = msgp.ReadStringBytes(buf); err != nil {
				return
			}
		case KeyTuple:
			if l, buf, err = msgp.ReadArrayHeaderBytes(buf); err != nil {
				return
			}
			if l == 2 {
				var obuf []byte

				if buf, err = msgp.Skip(buf); err != nil {
					return
				}

				obuf = buf
				if auth.GreetingAuth, buf, err = msgp.ReadBytesBytes(buf, nil); err != nil {
					if _, ok := err.(msgp.TypeError); ok {
						buf = obuf
						var greetingStr string
						if greetingStr, buf, err = msgp.ReadStringBytes(buf); err != nil {
							return
						}
						auth.GreetingAuth = []byte(greetingStr)
					}
				}
			}
		default:
			return buf, fmt.Errorf("Auth.Unpack: Expected KeyUserName or KeyTuple")
		}
	}
	return
}

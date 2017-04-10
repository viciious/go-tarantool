package tarantool

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"fmt"

	"gopkg.in/vmihailenco/msgpack.v2"
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

func (auth *Auth) Pack(data *packData, bodyBuffer *bytes.Buffer) (byte, error) {
	scr, err := scramble(auth.GreetingAuth, auth.Password)
	if err != nil {
		return BadRequest, fmt.Errorf("auth: scrambling failure: %s", err.Error())
	}

	encoder := msgpack.NewEncoder(bodyBuffer)

	encoder.EncodeMapLen(2) // User, Password
	encoder.EncodeUint64(KeyUserName)
	encoder.EncodeString(auth.User)

	encoder.EncodeUint64(KeyTuple)
	encoder.EncodeArrayLen(2)
	encoder.EncodeString(authHash)
	encoder.EncodeBytes(scr)

	return AuthRequest, nil
}

func (auth *Auth) Unpack(r *bytes.Buffer) (err error) {
	var i, l int
	var k uint64

	decoder := msgpack.NewDecoder(r)

	if i, err = decoder.DecodeMapLen(); err != nil {
		return
	}

	for ; i > 0; i-- {
		if k, err = decoder.DecodeUint64(); err != nil {
			return
		}

		switch k {
		case KeyUserName:
			if auth.User, err = decoder.DecodeString(); err != nil {
				return
			}
		case KeyTuple:
			if l, err = decoder.DecodeSliceLen(); err != nil {
				return
			}
			if l == 2 {
				if _, err = decoder.DecodeString(); err != nil {
					return
				}
				if auth.GreetingAuth, err = decoder.DecodeBytes(); err != nil {
					return
				}
			}
		default:
			return fmt.Errorf("Auth.Unpack: Expected KeyUserName or KeyTuple")
		}
	}

	return nil
}

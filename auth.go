package tnt

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
)

type Auth struct {
	User         string
	Password     string
	GreetingAuth []byte
}

var _ Query = (*Auth)(nil)

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
	scrambleSize := sha1.Size // == 20

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

func (auth *Auth) Pack(requestID uint64, defaultSpace string) ([]byte, error) {
	_, err := scramble(auth.GreetingAuth, auth.Password)
	if err != nil {
		return nil, fmt.Errorf("auth: scrambling failure: %s", err.Error())
	}

	return nil, nil
}

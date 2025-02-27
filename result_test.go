package tarantool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResultMarshaling(t *testing.T) {
	// The result of a call17 to:
	// function a()
	//     return "a"
	// end
	tntBodyBytes := []byte{
		0x81,                     // MP_MAP
		0x30,                     // key IPROTO_DATA
		0xdd, 0x0, 0x0, 0x0, 0x1, // MP_ARRAY
		0xa1, 0x61, // string value "a"
	}

	expectedDefaultMarshalBytes := []byte{
		0x81,       // MP_MAP
		0x30,       // key IPROTO_DATA
		0x91,       // MP_ARRAY
		0x91,       // MP_ARRAY
		0xa1, 0x61, // string value "a"
	}

	expectedFallbackMarshalBytes := []byte{
		0x81,       // MP_MAP
		0x30,       // key IPROTO_DATA
		0x91,       // MP_ARRAY
		0xa1, 0x61, // string value "a"
	}

	var result Result

	buf, err := result.UnmarshalMsg(tntBodyBytes)
	require.NoError(t, err, "error unmarshaling result")
	require.Empty(t, buf, "unmarshaling result buffer is not empty")
	require.Equal(t, result.Data, [][]interface{}{{"a"}})
	require.Empty(t, result.RawData)

	defaultMarshalRes, err := result.MarshalMsg(nil)
	require.NoError(t, err, "error marshaling by default marshaller")
	require.Equal(
		t,
		expectedDefaultMarshalBytes,
		defaultMarshalRes,
	)

	result = Result{unmarshalMode: ResultAsDataWithFallback}

	buf, err = result.UnmarshalMsg(tntBodyBytes)
	require.NoError(t, err, "error unmarshaling result")
	require.Empty(t, buf, "unmarshaling result buffer is not empty")
	require.Empty(t, result.Data)
	require.Equal(t, result.RawData, []interface{}{"a"})

	fallbackMarshalRes, err := result.MarshalMsg(nil)
	require.NoError(t, err, "error marshaling by bytes marshaller")
	require.Equal(t, fallbackMarshalRes, expectedFallbackMarshalBytes)
}

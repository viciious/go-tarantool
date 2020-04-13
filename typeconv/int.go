package typeconv

func IntfToInt(number interface{}) (int, bool) {
	switch value := number.(type) {
	default:
		return 0, false
	case int:
		return value, true
	case uint:
		return int(value), true
	case int8:
		return int(value), true
	case uint8:
		return int(value), true
	case int16:
		return int(value), true
	case uint16:
		return int(value), true
	case int32:
		return int(value), true
	case uint32:
		return int(value), true
	case int64:
		return int(value), true
	case uint64:
		return int(value), true
	}
}

func IntfToUint(number interface{}) (uint, bool) {
	switch value := number.(type) {
	default:
		return 0, false
	case int:
		return uint(value), true
	case uint:
		return value, true
	case int8:
		return uint(value), true
	case uint8:
		return uint(value), true
	case int16:
		return uint(value), true
	case uint16:
		return uint(value), true
	case int32:
		return uint(value), true
	case uint32:
		return uint(value), true
	case int64:
		return uint(value), true
	case uint64:
		return uint(value), true
	}
}

func IntfToInt32(number interface{}) (int32, bool) {
	if conv, ok := IntfToInt(number); ok {
		return int32(conv), true
	}
	return 0, false
}

func IntfToUint32(number interface{}) (uint32, bool) {
	if conv, ok := IntfToUint(number); ok {
		return uint32(conv), true
	}
	return 0, false
}

func IntfToInt64(number interface{}) (int64, bool) {
	switch value := number.(type) {
	default:
		return 0, false
	case int:
		return int64(value), true
	case uint:
		return int64(value), true
	case int8:
		return int64(value), true
	case uint8:
		return int64(value), true
	case int16:
		return int64(value), true
	case uint16:
		return int64(value), true
	case int32:
		return int64(value), true
	case uint32:
		return int64(value), true
	case int64:
		return value, true
	case uint64:
		return int64(value), true
	}
}

func IntfToUint64(number interface{}) (uint64, bool) {
	switch value := number.(type) {
	default:
		return 0, false
	case int:
		return uint64(value), true
	case uint:
		return uint64(value), true
	case int8:
		return uint64(value), true
	case uint8:
		return uint64(value), true
	case int16:
		return uint64(value), true
	case uint16:
		return uint64(value), true
	case int32:
		return uint64(value), true
	case uint32:
		return uint64(value), true
	case int64:
		return uint64(value), true
	case uint64:
		return value, true
	}
}

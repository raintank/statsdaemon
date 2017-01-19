package out

import "strconv"

func WriteFloat64(buf []byte, key []byte, val float64, now int64) []byte {
	buf = append(buf, key...)
	buf = append(buf, ' ')
	buf = strconv.AppendFloat(buf, val, 'f', -1, 64)
	buf = append(buf, ' ')
	buf = strconv.AppendInt(buf, now, 10)
	return append(buf, '\n')
}

func WriteInt64(buf []byte, key []byte, val, now int64) []byte {
	buf = append(buf, key...)
	buf = append(buf, ' ')
	buf = strconv.AppendInt(buf, val, 10)
	buf = append(buf, ' ')
	buf = strconv.AppendInt(buf, now, 10)
	return append(buf, '\n')
}

// Package resp implements the REdis serialization protocol (RESP)
// parser
package resp

import (
	"errors"
	"strconv"
)

func ParseCommand(data []byte) ([]string, int, error) {
	if len(data) == 0 || data[0] != '*' {
		return nil, 0, errors.New("invalid input, expected array")
	}
	pos := 1 // ignoring '*'

	numArgs, newPos, err := ReadLine(data, pos)
	if err != nil {
		return nil, 0, err
	}

	pos = newPos
	n, err := strconv.Atoi(numArgs)
	if err != nil {
		return nil, 0, err
	}

	args := make([]string, 0, n)

	for i := 0; i < n; i++ {
		if pos >= len(data) || data[pos] != '$' {
			return nil, 0, errors.New("expected bulk string")
		}
		pos++ // to skip '$'

		lenStr, newPos, err := ReadLine(data, pos)
		if err != nil {
			return nil, 0, err
		}
		pos = newPos

		strLen, err := strconv.Atoi(lenStr)
		if err != nil {
			return nil, 0, err
		}

		if pos+strLen > len(data) {
			return nil, 0, errors.New("not enough bytes for bulk string")
		}

		args = append(args, string(data[pos:pos+strLen]))
		pos += strLen

		if pos+2 > len(data) || data[pos] != '\r' || data[pos+1] != '\n' {
			return nil, 0, errors.New("expected CRLF after bulk string")
		}
		pos += 2
	}
	return args, pos, nil
}

func ReadLine(data []byte, pos int) (string, int, error) {
	for i := pos; i < len(data)-1; i++ {
		if data[i] == '\r' && data[i+1] == '\n' {
			return string(data[pos:i]), i + 2, nil
		}
	}
	return "", 0, errors.New("no CRLF found")
}

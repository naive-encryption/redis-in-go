package rdbparser

import (
	"encoding/binary"
	"fmt"
	"os"
)

type RDBEntry struct {
	Key      string
	Value    string
	ExpireAt int64
}

func ReadRDBFile(path string) (map[string]RDBEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err // TODO:  implement creation of an empty db
	}
	result := make(map[string]RDBEntry)
	pos := 9 // header is 9 bytes

	var pendingExpiry int64

	for pos < len(data) {
		op := data[pos]
		pos++

		switch op {
		case 0xFF: // eof
			return result, nil

		case 0xFE:
			_, newPos := readLength(data, pos)
			pos = newPos

		case 0xFB: // two length-encoded ints
			_, newPos := readLength(data, pos)
			pos = newPos
			_, newPos = readLength(data, pos)
			pos = newPos

		case 0xFA: // AUX field - key string + value string
			_, newPos := readString(data, pos)
			pos = newPos
			_, newPos = readString(data, pos)
			pos = newPos

		case 0xFD: // expiry in seconds
			secs := binary.LittleEndian.Uint32(data[pos : pos+4])
			pendingExpiry = int64(secs) * 1000
			pos += 4

		case 0xFC: // expiry in milliseconds
			ms := binary.LittleEndian.Uint64(data[pos : pos+8])
			pendingExpiry = int64(ms)
			pos += 8

		default:
			valueType := op
			key, newPos := readString(data, pos)
			pos = newPos

			switch valueType {
			case 0x00: // string value
				val, newPos := readString(data, pos)
				pos = newPos
				result[key] = RDBEntry{Key: key, Value: val, ExpireAt: pendingExpiry}
			default:
				return result, fmt.Errorf("Unsuported value type 0x%02X", valueType)
			}

			pendingExpiry = 0
		}
	}
	return result, nil
}

func readLength(data []byte, pos int) (int, int) {
	b := data[pos]
	pos++

	switch b >> 6 { // top 2 bits
	case 0b00:
		return int(b & 0x3F), pos
	case 0b01: // 14 bit
		next := data[pos]
		pos++
		return (int(b&0x3F) << 8) | int(next), pos
	case 0b10:
		val := binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4
		return int(val), pos
	default:
		return int(b), pos
	}
}

func readString(data []byte, pos int) (string, int) {
	b := data[pos]

	if b>>6 == 0b11 {
		pos++
		switch b & 0x3F {
		case 0: // 8 bit int
			val := int8(data[pos])
			pos++
			return fmt.Sprintf("%d", val), pos
		case 1: // 16 bit int
			val := int16(binary.LittleEndian.Uint16(data[pos : pos+2]))
			pos += 2
			return fmt.Sprintf("%d", val), pos
		case 2: // 32 bit int
			val := int32(binary.LittleEndian.Uint32(data[pos : pos+4]))
			pos += 4
			return fmt.Sprintf("%d", val), pos
		default:
			panic("LZF compressed strings not supported")
		}
	}

	length, newPos := readLength(data, pos)
	pos = newPos
	str := string(data[pos : pos+length])
	pos += length
	return str, pos
}

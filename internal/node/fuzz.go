// +build gofuzz

package node

import "encoding/binary"

func Fuzz(data []byte) int {
	// ensure it's large enough and that capacity is correct
	if len(data) < nodeHeaderSize {
		data = append(data, make([]byte, nodeHeaderSize-len(data))...)
	}
	binary.BigEndian.PutUint32(data[0:4], uint32(len(data)))

	// limit entry count to 16k
	data[8] = 0
	data[9] = 0

	_, err := Load(data)
	if err != nil {
		return 0
	}
	return 1
}

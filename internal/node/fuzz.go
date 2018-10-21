// +build gofuzz

package node

func Fuzz(data []byte) int {
	// ensure it's large enough and that capacity is correct
	if len(data) < nodeHeaderSize {
		data = append(data, make([]byte, nodeHeaderSize-len(data))...)
	}

	n, err := Load(data)
	if err != nil {
		return 0
	}

	// walk all the entries
	n.iter(func(ent entry, buf []byte) bool {
		ent.readKey(buf)
		return true
	})

	return 1
}

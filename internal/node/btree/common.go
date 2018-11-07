package btree

// compare is like bytes.Compare but for uint32s.
func compare(a, b uint32) int {
	if a == b {
		return 0
	} else if a < b {
		return -1
	}
	return 1
}

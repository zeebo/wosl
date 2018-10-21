// +build !release

package debug

import _ "unsafe"

//go:linkname throw runtime.throw
func throw(string)

func Assert(info string, fn func() bool) {
	if !fn() {
		throw("assertion failed: " + info)
	}
}

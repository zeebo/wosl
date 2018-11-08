package node

import (
	"github.com/zeebo/wosl/internal/node/btree"
	"github.com/zeebo/wosl/internal/node/entry"
)

// Iterator walks over the entries in a node.
type Iterator struct {
	buf  []byte
	iter btree.Iterator
}

func (i *Iterator) Next() bool     { return i.iter.Next() }
func (i *Iterator) Entry() entry.T { return i.iter.Entry() }
func (i *Iterator) Key() []byte    { return i.Entry().ReadKey(i.buf) }
func (i *Iterator) Value() []byte  { return i.Entry().ReadValue(i.buf) }

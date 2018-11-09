package node

import "github.com/zeebo/wosl/internal/node/btree"

func Dump(n *T) { btree.Dump(&n.entries, n.buf[n.base:]) }

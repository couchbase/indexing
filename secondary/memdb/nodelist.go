package memdb

import (
	"bytes"
	"github.com/couchbase/indexing/secondary/memdb/skiplist"
)

type NodeList struct {
	head           *skiplist.Node
	exposeItemCopy bool
}

func NewNodeList(head *skiplist.Node, exposeItemCopy bool) *NodeList {
	return &NodeList{
		head:           head,
		exposeItemCopy: exposeItemCopy,
	}
}

func (l *NodeList) Keys() (keys [][]byte) {
	var key []byte
	var itm *Item

	node := l.head
	for node != nil {
		itm = (*Item)(node.Item())
		if l.exposeItemCopy {
			// Exposed to GSI slice mutation path, return copy
			key = itm.BytesCopy()
		} else {
			key = itm.Bytes()
		}

		keys = append(keys, key)
		node = node.GetLink()
	}

	return
}

func (l *NodeList) Remove(key []byte) *skiplist.Node {
	var prev *skiplist.Node
	node := l.head
	for node != nil {
		nodeKey := (*Item)(node.Item()).Bytes()
		if bytes.Equal(nodeKey, key) {
			if prev == nil {
				l.head = node.GetLink()
				return node
			} else {
				prev.SetLink(node.GetLink())
				return node
			}
		}
		prev = node
		node = node.GetLink()
	}

	return nil
}

func (l *NodeList) Add(node *skiplist.Node) {
	node.SetLink(l.head)
	l.head = node
}

func (l *NodeList) Head() *skiplist.Node {
	return l.head
}

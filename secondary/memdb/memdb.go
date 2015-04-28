package memdb

type Item interface {
	Less(than Item) bool
}

type ItemIterator func(i Item) bool

type MemDB interface {
	Get(Item) Item
	InsertNoReplace(Item)
	Delete(Item) Item
	AscendGreaterOrEqual(Item, ItemIterator)
	Clone() MemDB
	Len() int
}

func New() MemDB {
	return NewLLRB()
}

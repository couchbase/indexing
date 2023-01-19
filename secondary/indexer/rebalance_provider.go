package indexer

type RebalanceProvider interface {
	Cancel()
	RestoreAndUnlockShards()
}

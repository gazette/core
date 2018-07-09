package cmd

import (
	"crypto"
)

// When shard_compose finds the same key in multiple different input sources, it has to select a single value
// to take the place of the n incoming values. Implement this interface to choose a different behavior for how
// multiple keys will be merged.
type Merge func(key []byte, values [][]byte) []byte

// Default Merge function
func First(key []byte, values [][]byte) []byte {
	return values[0]
}

// shard_split partitions a shard into multiple shards with smaller keyspaces. In order to do this partitioning, you must
// specify how you want shards to choose where to put each key. This function takes in a key and the index
// of the original shard you're processing and outputs an index for the new shard that it will be assigned to.
type Split func(originalPartition int, key []byte) int

// Example of a Split function that splits keys in a shard in half arbitrarily
func Halve(originalPartition int, key []byte) int {
	h := crypto.MD5.New().Sum(key)
	return originalPartition + int(h[len(h)-1])%2
}

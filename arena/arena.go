package arena

// arena is a free list that provides quick access to pre-allocated byte
// slices, greatly reducing memory churn and effectively disabling GC for these
// allocations. After the arena is created, a slice of bytes can be requested by
// calling Pop(). The caller is responsible for calling Push(), which puts the
// blocks back in the queue for later usage. The bytes given by Pop() are *not*
// zeroed, so the caller should only read positions that it knows to have been
// overwitten. That can be done by shortening the slice at the right place,
// based on the count of bytes returned by Write() and similar functions.
type Arena chan []byte

func NewArena(blockSize int, numBlocks int) Arena {
	blocks := make(Arena, numBlocks)
	for i := 0; i < numBlocks; i++ {
		blocks <- make([]byte, blockSize)
	}
	return blocks
}

func (a Arena) Pop() (x []byte) {
	return <-a
}

func (a Arena) Push(x []byte) {
	x = x[:cap(x)]
	a <- x
}

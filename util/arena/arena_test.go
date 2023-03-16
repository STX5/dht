package arena

import (
	"testing"
)

func BenchmarkArena(b *testing.B) {
	b.StopTimer()
	a := NewArena(1024, 1000)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		a.Push(a.Pop())
	}
}

package tarantool

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVectorClockClone(t *testing.T) {
	require := require.New(t)
	vc := NewVectorClock(1, 2, 3, 4, 5)
	clone := vc.Clone()
	require.Equal(vc, clone)
}

func TestVectorClockFollow(t *testing.T) {
	require := require.New(t)
	vc := NewVectorClock(1, 2, 3, 4, 5)
	require.Equal(VectorClock{0, 1, 2, 3, 4, 5}, vc)
	vc.Follow(0, 1)
	require.Equal(VectorClock{1, 1, 2, 3, 4, 5}, vc)
	vc.Follow(1, 2)
	require.Equal(VectorClock{1, 2, 2, 3, 4, 5}, vc)
	vc.Follow(2, 3)
	require.Equal(VectorClock{1, 2, 3, 3, 4, 5}, vc)
	vc.Follow(7, 42)
	require.Equal(VectorClock{1, 2, 3, 3, 4, 5, 0, 42}, vc)
}

// makes sense only with -race flag
func TestVectorClockRace(t *testing.T) {
	vc := NewVectorClock(1, 2, 3, 4, 5)
	var wg sync.WaitGroup

	wg.Add(1)
	go func(vc VectorClock) {
		defer wg.Done()
		for id := uint32(1); id < 10; id++ {
			for i := 0; i < 10; i++ {
				vc.Follow(id, rand.Uint64())
				time.Sleep(10 * time.Millisecond)
			}
		}
	}(vc)

	wg.Add(1)
	go func(vc VectorClock) {
		defer wg.Done()
		for i := 1; i < 100; i++ {
			clone := vc.Clone()
			_ = clone
			time.Sleep(10 * time.Millisecond)
		}
	}(vc)

	wg.Wait()
}

// Copyright IBM Corp. 2014, 2025
// SPDX-License-Identifier: MPL-2.0

package lru

import (
	"sync"
	"testing"
)

func Benchmark2Q_Rand(b *testing.B) {
	l, err := New2Q[int64, int64](8192)
	if err != nil {
		b.Fatalf("err: %v", err)
	}

	trace := make([]int64, b.N*2)
	for i := 0; i < b.N*2; i++ {
		trace[i] = getRand(b) % 32768
	}

	b.ResetTimer()

	var hit, miss int
	for i := 0; i < 2*b.N; i++ {
		if i%2 == 0 {
			l.Add(trace[i], trace[i])
		} else {
			if _, ok := l.Get(trace[i]); ok {
				hit++
			} else {
				miss++
			}
		}
	}
	b.Logf("hit: %d miss: %d ratio: %f", hit, miss, float64(hit)/float64(hit+miss))
}

func Benchmark2Q_Freq(b *testing.B) {
	l, err := New2Q[int64, int64](8192)
	if err != nil {
		b.Fatalf("err: %v", err)
	}

	trace := make([]int64, b.N*2)
	for i := 0; i < b.N*2; i++ {
		if i%2 == 0 {
			trace[i] = getRand(b) % 16384
		} else {
			trace[i] = getRand(b) % 32768
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		l.Add(trace[i], trace[i])
	}
	var hit, miss int
	for i := 0; i < b.N; i++ {
		if _, ok := l.Get(trace[i]); ok {
			hit++
		} else {
			miss++
		}
	}
	b.Logf("hit: %d miss: %d ratio: %f", hit, miss, float64(hit)/float64(hit+miss))
}

func Test2Q_RandomOps(t *testing.T) {
	size := 128
	l, err := New2Q[int64, int64](128)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	n := 200000
	for i := 0; i < n; i++ {
		key := getRand(t) % 512
		r := getRand(t)
		switch r % 3 {
		case 0:
			l.Add(key, key)
		case 1:
			l.Get(key)
		case 2:
			l.Remove(key)
		}

		if l.recent.Len()+l.frequent.Len() > size {
			t.Fatalf("bad: recent: %d freq: %d",
				l.recent.Len(), l.frequent.Len())
		}
	}
}

func Test2Q_Get_RecentToFrequent(t *testing.T) {
	l, err := New2Q[int, int](128)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Touch all the entries, should be in t1
	for i := 0; i < 128; i++ {
		l.Add(i, i)
	}
	if n := l.recent.Len(); n != 128 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}

	// Get should upgrade to t2
	for i := 0; i < 128; i++ {
		if _, ok := l.Get(i); !ok {
			t.Fatalf("missing: %d", i)
		}
	}
	if n := l.recent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 128 {
		t.Fatalf("bad: %d", n)
	}

	// Get be from t2
	for i := 0; i < 128; i++ {
		if _, ok := l.Get(i); !ok {
			t.Fatalf("missing: %d", i)
		}
	}
	if n := l.recent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 128 {
		t.Fatalf("bad: %d", n)
	}
}

func Test2Q_Add_RecentToFrequent(t *testing.T) {
	l, err := New2Q[int, int](128)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Add initially to recent
	l.Add(1, 1)
	if n := l.recent.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}

	// Add should upgrade to frequent
	l.Add(1, 1)
	if n := l.recent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}

	// Add should remain in frequent
	l.Add(1, 1)
	if n := l.recent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
}

func Test2Q_Add_RecentEvict(t *testing.T) {
	l, err := New2Q[int, int](4)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Add 1,2,3,4,5 -> Evict 1
	l.Add(1, 1)
	l.Add(2, 2)
	l.Add(3, 3)
	l.Add(4, 4)
	l.Add(5, 5)
	if n := l.recent.Len(); n != 4 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.recentEvict.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}

	// Pull in the recently evicted
	l.Add(1, 1)
	if n := l.recent.Len(); n != 3 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.recentEvict.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}

	// Add 6, should cause another recent evict
	l.Add(6, 6)
	if n := l.recent.Len(); n != 3 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.recentEvict.Len(); n != 2 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
}

func Test2Q_Resize(t *testing.T) {
	l, err := New2Q[int, int](100)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Touch all the entries, should be in t1
	for i := 0; i < 100; i++ {
		l.Add(i, i)
	}

	evicted := l.Resize(50)
	if evicted != 50 {
		t.Fatalf("bad: %d", evicted)
	}

	if n := l.recent.Len(); n != 50 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}

	l, err = New2Q[int, int](100)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	for i := 0; i < 100; i++ {
		l.Add(i, i)
	}

	for i := 0; i < 50; i++ {
		l.Add(i, i)
	}

	evicted = l.Resize(50)
	if evicted != 50 {
		t.Fatalf("bad: %d", evicted)
	}

	if n := l.recent.Len(); n != 12 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 38 {
		t.Fatalf("bad: %d", n)
	}

	l, err = New2Q[int, int](100)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	for i := 0; i < 100; i++ {
		l.Add(i, i)
		l.Add(i, i)
	}

	evicted = l.Resize(50)
	if evicted != 50 {
		t.Fatalf("bad: %d", evicted)
	}

	if n := l.recent.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.frequent.Len(); n != 50 {
		t.Fatalf("bad: %d", n)
	}
}

func Test2Q(t *testing.T) {
	l, err := New2Q[int, int](128)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := 0; i < 256; i++ {
		l.Add(i, i)
	}
	if l.Len() != 128 {
		t.Fatalf("bad len: %v", l.Len())
	}
	if l.Cap() != 128 {
		t.Fatalf("expect %d, but %d", 128, l.Cap())
	}

	for i, k := range l.Keys() {
		if v, ok := l.Get(k); !ok || v != k || v != i+128 {
			t.Fatalf("bad key: %v", k)
		}
	}
	for i, v := range l.Values() {
		if v != i+128 {
			t.Fatalf("bad key: %v", v)
		}
	}
	for i := 0; i < 128; i++ {
		if _, ok := l.Get(i); ok {
			t.Fatalf("should be evicted")
		}
	}
	for i := 128; i < 256; i++ {
		if _, ok := l.Get(i); !ok {
			t.Fatalf("should not be evicted")
		}
	}
	for i := 128; i < 192; i++ {
		l.Remove(i)
		if _, ok := l.Get(i); ok {
			t.Fatalf("should be deleted")
		}
	}

	l.Purge()
	if l.Len() != 0 {
		t.Fatalf("bad len: %v", l.Len())
	}
	if _, ok := l.Get(200); ok {
		t.Fatalf("should contain nothing")
	}
}

// Test that Contains doesn't update recent-ness
func Test2Q_Contains(t *testing.T) {
	l, err := New2Q[int, int](2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	l.Add(1, 1)
	l.Add(2, 2)
	if !l.Contains(1) {
		t.Errorf("1 should be contained")
	}

	l.Add(3, 3)
	if l.Contains(1) {
		t.Errorf("Contains should not have updated recent-ness of 1")
	}
}

// Test that Peek doesn't update recent-ness
func Test2Q_Peek(t *testing.T) {
	l, err := New2Q[int, int](2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	l.Add(1, 1)
	l.Add(2, 2)
	if v, ok := l.Peek(1); !ok || v != 1 {
		t.Errorf("1 should be set to 1: %v, %v", v, ok)
	}

	l.Add(3, 3)
	if l.Contains(1) {
		t.Errorf("should not have updated recent-ness of 1")
	}
}

func Test2Q_EvictRecent(t *testing.T) {
	evictCounter := 0
	onEvicted := func(k int, v int) {
		evictCounter++
	}
	l, err := New2QWithEvict[int, int](4, onEvicted)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Add 1,2,3,4,5 -> 1 is evicted from the recent segment and enters
	// the ghost segment.
	l.Add(1, 1)
	l.Add(2, 2)
	l.Add(3, 3)
	l.Add(4, 4)
	l.Add(5, 5)

	if evictCounter != 1 {
		t.Fatalf("bad evict count: %v", evictCounter)
	}
	if n := l.recentEvict.Len(); n != 1 {
		t.Fatalf("bad ghost len: %d", n)
	}

	// Explicitly removing a ghost key is user initiated and must not fire
	// the callback.
	l.Remove(1)
	if evictCounter != 1 {
		t.Fatalf("bad evict count: %v", evictCounter)
	}
}

func Test2Q_EvictFrequent(t *testing.T) {
	evictCounter := 0
	var evictedKey, evictedVal int
	onEvicted := func(k int, v int) {
		evictCounter++
		evictedKey, evictedVal = k, v
	}
	// A recent ratio of 0 promotes every entry straight to the frequent
	// segment; keep a positive ghost ratio so the ghost LRU is valid.
	l, err := New2QParamsWithEvict[int, int](2, 0.0, 0.5, onEvicted)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// recentSize is 0, so every entry is promoted straight to frequent.
	l.Add(1, 1)
	l.Add(1, 1)
	l.Add(2, 2)
	l.Add(2, 2)

	l.Add(3, 3)
	if evictCounter != 1 {
		t.Fatalf("bad evict count: %v", evictCounter)
	}
	if evictedKey != 1 || evictedVal != 1 {
		t.Fatalf("bad evicted pair: (%v, %v)", evictedKey, evictedVal)
	}
	if l.Len() != 2 {
		t.Fatalf("bad len: %v", l.Len())
	}
}

func Test2Q_EvictGhost(t *testing.T) {
	var ghostKeys []int
	realEvictCounter := 0
	ghostEvictCounter := 0
	onEvicted := func(k int, v int) {
		if v == 0 {
			ghostEvictCounter++
			ghostKeys = append(ghostKeys, k)
			return
		}
		realEvictCounter++
	}
	l, err := New2QParamsWithEvict[int, int](4, 0.25, 0.5, onEvicted)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// The ghost segment holds at most 2 keys. Adding distinct keys cycles
	// entries through it; once full, every further eviction pushes a ghost
	// key out, reported with the zero value.
	for i := 1; i <= 16; i++ {
		l.Add(i, i)
	}

	if l.Len() != 4 {
		t.Fatalf("bad len: %v", l.Len())
	}
	if n := l.recentEvict.Len(); n != 2 {
		t.Fatalf("bad ghost len: %d", n)
	}
	if ghostEvictCounter != len(ghostKeys) || ghostEvictCounter == 0 {
		t.Fatalf("bad ghost evict count: %v, keys: %v", ghostEvictCounter, ghostKeys)
	}
	if realEvictCounter == 0 {
		t.Fatalf("expected value-bearing evictions")
	}
}

func Test2Q_EvictedCountAfterFill(t *testing.T) {
	const size = 128
	const distinct = 512
	evictCounter := 0
	valueEvictCounter := 0
	ghostEvictCounter := 0
	reported := make(map[int]int)
	onEvicted := func(k int, v int) {
		evictCounter++
		reported[k]++
		if v == 0 {
			ghostEvictCounter++
		} else {
			valueEvictCounter++
			if v != k {
				t.Fatalf("bad evicted value: %v for key %v", v, k)
			}
		}
	}
	l, err := New2QWithEvict[int, int](size, onEvicted)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Add many distinct random keys from a large key space so that, with
	// overwhelming probability, no key is ever re-added after its eviction.
	// Each non-resident key is therefore reported either once (value-bearing
	// eviction, still in the ghost segment) or twice (followed by a
	// zero-valued ghost eviction), so the total count equals the number of
	// value-bearing evictions plus ghost evictions and never exceeds twice
	// the number of non-resident keys.
	seen := make(map[int]struct{}, distinct)
	for len(seen) < distinct {
		key := int(getRand(t) % 1_000_000_000)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		l.Add(key, key)
	}

	if l.Len() != size {
		t.Fatalf("bad len: %v", l.Len())
	}
	if evictCounter != valueEvictCounter+ghostEvictCounter {
		t.Fatalf("bad evict count: %v", evictCounter)
	}
	// Every distinct key leaves the value-bearing segments at most once.
	if valueEvictCounter != distinct-size {
		t.Fatalf("bad value evict count: %v, expect %v", valueEvictCounter, distinct-size)
	}
	// Ghost evictions cannot outnumber value-bearing evictions and every
	// reported key must be a key we actually inserted.
	if ghostEvictCounter > valueEvictCounter {
		t.Fatalf("too many ghost evictions: %v", ghostEvictCounter)
	}
	for key := range reported {
		if _, ok := seen[key]; !ok {
			t.Fatalf("callback fired for unknown key: %v", key)
		}
	}
}

func Test2Q_EvictionCallbackReentrant(t *testing.T) {
	evictCounter := 0
	var l *TwoQueueCache[int, int]
	onEvicted := func(k int, v int) {
		evictCounter++
		// Re-entering the cache from the callback must not deadlock since
		// the callback runs outside of the cache lock. These read-only
		// re-entrant calls acquire the same lock and would self-deadlock if
		// the callback were invoked while holding it.
		if _, ok := l.Peek(k); ok {
			t.Errorf("evicted key %v should no longer be resident", k)
		}
		l.Contains(k)
	}

	var err error
	l, err = New2QWithEvict[int, int](4, onEvicted)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Fill and overflow the cache to drive evictions through the callback.
	for i := 0; i < 32; i++ {
		l.Add(i, i)
	}
	if evictCounter == 0 {
		t.Fatal("expected eviction callback to fire")
	}
	if l.Len() != 4 {
		t.Fatalf("bad len: %v", l.Len())
	}
}

func Test2Q_EvictionCallbackConcurrent(t *testing.T) {
	const size = 64
	var l *TwoQueueCache[int, int]
	onEvicted := func(k int, v int) {
		// Re-entering through the read path while other goroutines mutate
		// exercises the lock-outside-callback guarantee under -race.
		l.Peek(k)
		l.Contains(k)
	}

	var err error
	l, err = New2QWithEvict[int, int](size, onEvicted)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(off int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				key := off*2000 + i
				l.Add(key, key)
				if key%7 == 0 {
					l.Remove(key)
				}
				l.Get(key)
			}
		}(g)
	}
	wg.Wait()

	if l.Len() > size {
		t.Fatalf("cache exceeded capacity: %v", l.Len())
	}
}

func Test2Q_EvictOnPurgeAndResize(t *testing.T) {
	evictCounter := 0
	onEvicted := func(k int, v int) {
		evictCounter++
	}

	l, err := New2QWithEvict[int, int](8, onEvicted)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	for i := 0; i < 8; i++ {
		l.Add(i, i)
	}

	l.Resize(4)
	if evictCounter != 4 {
		t.Fatalf("bad evict count after resize: %v", evictCounter)
	}

	l.Purge()
	if evictCounter != 8 {
		t.Fatalf("bad evict count after purge: %v", evictCounter)
	}
}

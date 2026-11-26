// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: MPL-2.0

package simplelru

import (
	"reflect"
	"testing"
)

func TestWeightedLRUWeigherValidation(t *testing.T) {
	if _, err := NewWeightedLRU[int, int](0, func(k, v int) int64 { return 1 }, nil); err == nil {
		t.Fatal("expected error for non-positive capacity")
	}
	if _, err := NewWeightedLRU[int, int](-1, func(k, v int) int64 { return 1 }, nil); err == nil {
		t.Fatal("expected error for negative capacity")
	}
	if _, err := NewWeightedLRU[int, int](10, nil, nil); err == nil {
		t.Fatal("expected error for nil weigher")
	}
}

func TestWeightedLRUEvictsByWeight(t *testing.T) {
	var evicted []int
	onEvicted := func(k int, v int) {
		evicted = append(evicted, k)
	}
	// capacity 10, entry cost equals its key
	l, err := NewWeightedLRU[int, int](10, func(k, v int) int64 { return int64(k) }, onEvicted)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// 4 + 6 = 10, exactly at capacity
	if l.Add(4, 4) || l.Add(6, 6) {
		t.Fatal("unexpected eviction")
	}
	if l.Weight() != 10 || l.Len() != 2 {
		t.Fatalf("bad weight/len: %v/%v", l.Weight(), l.Len())
	}

	// adding cost 3 evicts the oldest (4) to make room: 6 + 3 = 9
	if !l.Add(3, 3) {
		t.Fatal("expected eviction")
	}
	if !reflect.DeepEqual(evicted, []int{4}) {
		t.Fatalf("bad evicted: %v", evicted)
	}
	if l.Weight() != 9 || l.Len() != 2 {
		t.Fatalf("bad weight/len: %v/%v", l.Weight(), l.Len())
	}

	// a single heavy entry can evict multiple lighter ones: 8 needs 6 and 3 gone
	if !l.Add(8, 8) {
		t.Fatal("expected eviction")
	}
	if !reflect.DeepEqual(evicted, []int{4, 6, 3}) {
		t.Fatalf("bad evicted: %v", evicted)
	}
	if l.Weight() != 8 || l.Len() != 1 {
		t.Fatalf("bad weight/len: %v/%v", l.Weight(), l.Len())
	}
}

func TestWeightedLRUUpdateAdjustsWeight(t *testing.T) {
	l, err := NewWeightedLRU[int, int](10, func(k, v int) int64 { return int64(v) }, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	l.Add(1, 4)
	l.Add(2, 4)
	if l.Weight() != 8 {
		t.Fatalf("bad weight: %v", l.Weight())
	}

	// grow an existing entry: 4 -> 6, total 10, no eviction
	if l.Add(1, 6) {
		t.Fatal("unexpected eviction")
	}
	if l.Weight() != 10 || l.Len() != 2 {
		t.Fatalf("bad weight/len: %v/%v", l.Weight(), l.Len())
	}

	// shrink an existing entry: 6 -> 1, total 5
	if l.Add(1, 1) {
		t.Fatal("unexpected eviction")
	}
	if l.Weight() != 5 {
		t.Fatalf("bad weight: %v", l.Weight())
	}

	// grow past capacity: 1 -> 7, total 11, evicts oldest (key 2)
	if !l.Add(1, 7) {
		t.Fatal("expected eviction")
	}
	if l.Contains(2) {
		t.Fatal("expected key 2 to be evicted")
	}
	if l.Weight() != 7 || l.Len() != 1 {
		t.Fatalf("bad weight/len: %v/%v", l.Weight(), l.Len())
	}
}

func TestWeightedLRURejectsOverweight(t *testing.T) {
	var evicted []int
	l, err := NewWeightedLRU[int, int](10, func(k, v int) int64 { return int64(v) }, func(k, v int) {
		evicted = append(evicted, k)
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// entry heavier than capacity is not admitted
	if l.Add(1, 11) {
		t.Fatal("unexpected eviction")
	}
	if l.Len() != 0 || l.Weight() != 0 {
		t.Fatalf("bad weight/len: %v/%v", l.Weight(), l.Len())
	}

	// overweight update evicts the previously cached entry
	l.Add(1, 5)
	if !l.Add(1, 11) {
		t.Fatal("expected eviction of replaced entry")
	}
	if l.Contains(1) || l.Weight() != 0 {
		t.Fatalf("bad state: weight %v", l.Weight())
	}
	if !reflect.DeepEqual(evicted, []int{1}) {
		t.Fatalf("bad evicted: %v", evicted)
	}
}

func TestWeightedLRUClampsWeight(t *testing.T) {
	// a buggy weigher reporting zero or negative costs must not allow
	// unbounded growth or corrupt the total
	l, err := NewWeightedLRU[int, int](3, func(k, v int) int64 { return int64(v) }, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := 0; i < 10; i++ {
		l.Add(i, -i) // weights 0, -1, -2, ... all clamp to 1
	}
	if l.Len() != 3 || l.Weight() != 3 {
		t.Fatalf("bad weight/len: %v/%v", l.Weight(), l.Len())
	}
}

func TestWeightedLRURecency(t *testing.T) {
	l, err := NewWeightedLRU[int, int](10, func(k, v int) int64 { return int64(v) }, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	l.Add(1, 5)
	l.Add(2, 5)
	// touch key 1 so key 2 becomes the eviction victim
	if _, ok := l.Get(1); !ok {
		t.Fatal("expected key 1")
	}
	if l.Weight() != 10 {
		t.Fatalf("get must not change weight: %v", l.Weight())
	}

	l.Add(3, 5)
	if l.Contains(2) {
		t.Fatal("expected key 2 to be evicted")
	}
	if !l.Contains(1) || !l.Contains(3) {
		t.Fatal("expected keys 1 and 3")
	}

	// Peek and Contains must not update recency
	if _, ok := l.Peek(1); !ok {
		t.Fatal("expected key 1")
	}
	if !l.Contains(1) {
		t.Fatal("expected key 1")
	}
	l.Add(4, 5)
	if l.Contains(1) {
		t.Fatal("expected key 1 evicted, peek/contains must not update recency")
	}
	if !l.Contains(3) {
		t.Fatal("expected key 3 to survive")
	}
}

func TestWeightedLRURemoveAndPurge(t *testing.T) {
	evictCounter := 0
	l, err := NewWeightedLRU[int, int](10, func(k, v int) int64 { return int64(v) }, func(k, v int) {
		evictCounter++
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	l.Add(1, 4)
	l.Add(2, 4)
	if !l.Remove(1) {
		t.Fatal("expected key 1")
	}
	if l.Weight() != 4 {
		t.Fatalf("bad weight: %v", l.Weight())
	}

	k, _, ok := l.RemoveOldest()
	if !ok || k != 2 {
		t.Fatalf("bad oldest: %v", k)
	}
	if l.Weight() != 0 {
		t.Fatalf("bad weight: %v", l.Weight())
	}

	l.Add(1, 4)
	l.Add(2, 4)
	l.Purge()
	if l.Len() != 0 || l.Weight() != 0 {
		t.Fatalf("bad weight/len after purge: %v/%v", l.Weight(), l.Len())
	}
	if evictCounter != 4 {
		t.Fatalf("bad evict count: %v", evictCounter)
	}
}

func TestWeightedLRUResize(t *testing.T) {
	l, err := NewWeightedLRU[int, int](10, func(k, v int) int64 { return int64(v) }, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	l.Add(1, 4)
	l.Add(2, 4)
	if evicted := l.Resize(5); evicted != 1 {
		t.Fatalf("bad evicted: %v", evicted)
	}
	if l.Weight() != 4 || l.Cap() != 5 {
		t.Fatalf("bad weight/cap: %v/%v", l.Weight(), l.Cap())
	}
	if l.Contains(1) {
		t.Fatal("expected oldest entry evicted")
	}

	// growing the capacity admits more weight again
	l.Resize(10)
	if l.Add(3, 6) {
		t.Fatal("unexpected eviction")
	}
	if l.Weight() != 10 {
		t.Fatalf("bad weight: %v", l.Weight())
	}
}

func TestWeightedLRUKeysValuesOldest(t *testing.T) {
	l, err := NewWeightedLRU[int, int](10, func(k, v int) int64 { return 1 }, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	l.Add(1, 1)
	l.Add(2, 2)
	l.Add(3, 3)

	if keys := l.Keys(); !reflect.DeepEqual(keys, []int{1, 2, 3}) {
		t.Fatalf("bad keys: %v", keys)
	}
	if values := l.Values(); !reflect.DeepEqual(values, []int{1, 2, 3}) {
		t.Fatalf("bad values: %v", values)
	}
	if k, v, ok := l.GetOldest(); !ok || k != 1 || v != 1 {
		t.Fatalf("bad oldest: %v/%v", k, v)
	}
}

// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: MPL-2.0

package lru

import (
	"sync"
	"testing"

	"github.com/hashicorp/golang-lru/v2/simplelru"
)

func TestWeightedCacheEvictCallback(t *testing.T) {
	var mu sync.Mutex
	var evicted []int
	onEvicted := func(k int, v int) {
		mu.Lock()
		evicted = append(evicted, k)
		mu.Unlock()
	}

	c, err := NewWeightedWithEvict[int, int](10, func(k, v int) int64 { return int64(v) }, onEvicted)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	c.Add(1, 4)
	c.Add(2, 4)
	// one Add evicts both entries to fit weight 9
	if !c.Add(3, 9) {
		t.Fatal("expected eviction")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(evicted) != 2 || evicted[0] != 1 || evicted[1] != 2 {
		t.Fatalf("bad evicted: %v", evicted)
	}
	if c.Weight() != 9 || c.Len() != 1 || c.Cap() != 10 {
		t.Fatalf("bad state: %v/%v/%v", c.Weight(), c.Len(), c.Cap())
	}
}

func TestWeightedCacheOperations(t *testing.T) {
	c, err := NewWeighted[string, []byte](16, func(k string, v []byte) int64 {
		return int64(len(k) + len(v))
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if ok, evicted := c.ContainsOrAdd("a", []byte("12345")); ok || evicted {
		t.Fatal("unexpected contains/evicted")
	}
	if ok, _ := c.ContainsOrAdd("a", []byte("12345")); !ok {
		t.Fatal("expected contains")
	}
	if prev, ok, _ := c.PeekOrAdd("a", []byte("x")); !ok || len(prev) != 5 {
		t.Fatalf("bad peek-or-add: %v", prev)
	}
	if _, ok := c.Peek("a"); !ok {
		t.Fatal("expected peek hit")
	}
	if !c.Contains("a") {
		t.Fatal("expected contains")
	}

	c.Add("b", []byte("12345678901")) // 1 + 11 = 12, total 18 > 16, evicts "a"
	if c.Contains("a") {
		t.Fatal("expected a evicted")
	}
	if c.Weight() != 12 {
		t.Fatalf("bad weight: %v", c.Weight())
	}

	if k, _, ok := c.GetOldest(); !ok || k != "b" {
		t.Fatalf("bad oldest: %v", k)
	}
	if k, _, ok := c.RemoveOldest(); !ok || k != "b" {
		t.Fatalf("bad remove oldest: %v", k)
	}
	if c.Weight() != 0 {
		t.Fatalf("bad weight: %v", c.Weight())
	}

	c.Add("x", nil)
	c.Add("y", nil)
	if keys := c.Keys(); len(keys) != 2 || keys[0] != "x" || keys[1] != "y" {
		t.Fatalf("bad keys: %v", keys)
	}
	if values := c.Values(); len(values) != 2 {
		t.Fatalf("bad values: %v", values)
	}
	if evicted := c.Resize(1); evicted != 1 {
		t.Fatalf("bad evicted: %v", evicted)
	}
	if !c.Remove("y") {
		t.Fatal("expected remove")
	}
	c.Purge()
	if c.Len() != 0 || c.Weight() != 0 {
		t.Fatalf("bad state after purge: %v/%v", c.Len(), c.Weight())
	}
}

func TestWeightedCacheConcurrent(t *testing.T) {
	c, err := NewWeighted[int, int](1000, func(k, v int) int64 { return 1 })
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				c.Add(base+i, i)
				c.Get(base + i)
			}
		}(g * 500)
	}
	wg.Wait()

	if c.Weight() != int64(c.Len()) {
		t.Fatalf("weight/len mismatch: %v/%v", c.Weight(), c.Len())
	}
	if c.Weight() > c.Cap() {
		t.Fatalf("weight %v exceeds capacity %v", c.Weight(), c.Cap())
	}
}

func TestWeightedCacheUsesSimpleLRUWeigher(t *testing.T) {
	var weigher simplelru.Weigher[int, int] = func(k, v int) int64 { return int64(v) }
	if _, err := NewWeighted[int, int](0, weigher); err == nil {
		t.Fatal("expected error for non-positive capacity")
	}
}

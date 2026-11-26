// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: MPL-2.0

package simplelru

import (
	"errors"

	"github.com/hashicorp/golang-lru/v2/internal"
)

// Weigher computes the cost ("weight") of a cache entry. It is fixed at
// construction time, so callers adding entries cannot misreport costs: an
// over-reported weight would keep entries out of the cache, while an
// under-reported weight would let the cache oversell its capacity.
//
// The returned value is sanitized before use: weights below 1 are treated
// as 1, so every entry accounts for at least one unit of capacity and the
// total weight can never be corrupted by a buggy weigher.
type Weigher[K comparable, V any] func(key K, value V) int64

// WeightedLRU implements a non-thread safe LRU cache bounded by total
// weight instead of entry count. Entries are evicted, least recently used
// first, whenever the total weight exceeds the configured capacity.
//
// The total weight is maintained only on mutation paths (Add, Remove,
// RemoveOldest, Resize, Purge). Lookups never touch it, so cache hits on
// hot keys do not serialize on shared weight accounting.
type WeightedLRU[K comparable, V any] struct {
	capacity  int64
	weight    int64
	weigher   Weigher[K, V]
	evictList *internal.LruList[K, V]
	items     map[K]*internal.Entry[K, V]
	onEvict   EvictCallback[K, V]
}

// NewWeightedLRU constructs a weighted LRU with the given weight capacity.
func NewWeightedLRU[K comparable, V any](capacity int64, weigher Weigher[K, V], onEvict EvictCallback[K, V]) (*WeightedLRU[K, V], error) {
	if capacity <= 0 {
		return nil, errors.New("must provide a positive weight capacity")
	}
	if weigher == nil {
		return nil, errors.New("must provide a weigher")
	}

	c := &WeightedLRU[K, V]{
		capacity:  capacity,
		weigher:   weigher,
		evictList: internal.NewList[K, V](),
		items:     make(map[K]*internal.Entry[K, V]),
		onEvict:   onEvict,
	}
	return c, nil
}

// weigh returns the sanitized weight of an entry.
func (c *WeightedLRU[K, V]) weigh(key K, value V) int64 {
	if w := c.weigher(key, value); w > 1 {
		return w
	}
	return 1
}

// Purge is used to completely clear the cache.
func (c *WeightedLRU[K, V]) Purge() {
	for k, v := range c.items {
		if c.onEvict != nil {
			c.onEvict(k, v.Value)
		}
		delete(c.items, k)
	}
	c.evictList.Init()
	c.weight = 0
}

// Add adds a value to the cache, evicting least recently used entries
// until the total weight fits within the capacity. Returns true if an
// eviction occurred.
//
// An entry whose weight exceeds the capacity is never admitted; if it
// replaces an existing key, the previous entry is evicted.
func (c *WeightedLRU[K, V]) Add(key K, value V) (evicted bool) {
	w := c.weigh(key, value)
	if w > c.capacity {
		if ent, ok := c.items[key]; ok {
			c.removeElement(ent)
			return true
		}
		return false
	}

	// Check for existing item
	if ent, ok := c.items[key]; ok {
		c.weight += w - ent.Weight
		c.evictList.MoveToFront(ent)
		ent.Value = value
		ent.Weight = w
	} else {
		// Add new item
		ent := c.evictList.PushFront(key, value)
		ent.Weight = w
		c.items[key] = ent
		c.weight += w
	}

	// Evict oldest entries until the total weight fits
	for c.weight > c.capacity {
		c.removeOldest()
		evicted = true
	}
	return evicted
}

// Get looks up a key's value from the cache.
func (c *WeightedLRU[K, V]) Get(key K) (value V, ok bool) {
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		return ent.Value, true
	}
	return
}

// Contains checks if a key is in the cache, without updating the recent-ness
// or deleting it for being stale.
func (c *WeightedLRU[K, V]) Contains(key K) (ok bool) {
	_, ok = c.items[key]
	return ok
}

// Peek returns the key value (or undefined if not found) without updating
// the "recently used"-ness of the key.
func (c *WeightedLRU[K, V]) Peek(key K) (value V, ok bool) {
	var ent *internal.Entry[K, V]
	if ent, ok = c.items[key]; ok {
		return ent.Value, true
	}
	return
}

// Remove removes the provided key from the cache, returning if the
// key was contained.
func (c *WeightedLRU[K, V]) Remove(key K) (present bool) {
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
		return true
	}
	return false
}

// RemoveOldest removes the oldest item from the cache.
func (c *WeightedLRU[K, V]) RemoveOldest() (key K, value V, ok bool) {
	if ent := c.evictList.Back(); ent != nil {
		c.removeElement(ent)
		return ent.Key, ent.Value, true
	}
	return
}

// GetOldest returns the oldest entry
func (c *WeightedLRU[K, V]) GetOldest() (key K, value V, ok bool) {
	if ent := c.evictList.Back(); ent != nil {
		return ent.Key, ent.Value, true
	}
	return
}

// Keys returns a slice of the keys in the cache, from oldest to newest.
func (c *WeightedLRU[K, V]) Keys() []K {
	keys := make([]K, c.evictList.Length())
	i := 0
	for ent := c.evictList.Back(); ent != nil; ent = ent.PrevEntry() {
		keys[i] = ent.Key
		i++
	}
	return keys
}

// Values returns a slice of the values in the cache, from oldest to newest.
func (c *WeightedLRU[K, V]) Values() []V {
	values := make([]V, c.evictList.Length())
	i := 0
	for ent := c.evictList.Back(); ent != nil; ent = ent.PrevEntry() {
		values[i] = ent.Value
		i++
	}
	return values
}

// Len returns the number of items in the cache.
func (c *WeightedLRU[K, V]) Len() int {
	return c.evictList.Length()
}

// Weight returns the total weight of the items in the cache.
func (c *WeightedLRU[K, V]) Weight() int64 {
	return c.weight
}

// Cap returns the weight capacity of the cache.
func (c *WeightedLRU[K, V]) Cap() int64 {
	return c.capacity
}

// Resize changes the cache weight capacity, evicting least recently used
// entries as needed, and returns the number of entries evicted.
func (c *WeightedLRU[K, V]) Resize(capacity int64) (evicted int) {
	for c.weight > capacity && c.evictList.Length() > 0 {
		c.removeOldest()
		evicted++
	}
	c.capacity = capacity
	return evicted
}

// removeOldest removes the oldest item from the cache.
func (c *WeightedLRU[K, V]) removeOldest() {
	if ent := c.evictList.Back(); ent != nil {
		c.removeElement(ent)
	}
}

// removeElement is used to remove a given list element from the cache
func (c *WeightedLRU[K, V]) removeElement(e *internal.Entry[K, V]) {
	c.evictList.Remove(e)
	delete(c.items, e.Key)
	c.weight -= e.Weight
	if c.onEvict != nil {
		c.onEvict(e.Key, e.Value)
	}
}

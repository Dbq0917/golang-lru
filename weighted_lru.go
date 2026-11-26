// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: MPL-2.0

package lru

import (
	"sync"

	"github.com/hashicorp/golang-lru/v2/simplelru"
)

// WeightedCache is a thread-safe LRU cache bounded by total weight instead
// of entry count. Entry costs are computed by a Weigher fixed at
// construction time, and the total weight is only maintained on mutation
// paths, so lookups on hot keys never serialize on weight accounting.
type WeightedCache[K comparable, V any] struct {
	lru         *simplelru.WeightedLRU[K, V]
	evictedKeys []K
	evictedVals []V
	onEvictedCB func(k K, v V)
	lock        sync.RWMutex
}

// NewWeighted creates a weighted LRU cache with the given weight capacity.
// The weigher computes the cost of each entry and cannot be overridden by
// callers adding entries.
func NewWeighted[K comparable, V any](capacity int64, weigher simplelru.Weigher[K, V]) (*WeightedCache[K, V], error) {
	return NewWeightedWithEvict[K, V](capacity, weigher, nil)
}

// NewWeightedWithEvict constructs a weighted cache with the given eviction
// callback.
func NewWeightedWithEvict[K comparable, V any](capacity int64, weigher simplelru.Weigher[K, V], onEvicted func(key K, value V)) (c *WeightedCache[K, V], err error) {
	c = &WeightedCache[K, V]{
		onEvictedCB: onEvicted,
	}
	if onEvicted != nil {
		c.initEvictBuffers()
		onEvicted = c.onEvicted
	}
	c.lru, err = simplelru.NewWeightedLRU(capacity, weigher, onEvicted)
	return
}

func (c *WeightedCache[K, V]) initEvictBuffers() {
	c.evictedKeys = make([]K, 0, DefaultEvictedBufferSize)
	c.evictedVals = make([]V, 0, DefaultEvictedBufferSize)
}

// onEvicted save evicted key/val and sent in externally registered callback
// outside of critical section
func (c *WeightedCache[K, V]) onEvicted(k K, v V) {
	c.evictedKeys = append(c.evictedKeys, k)
	c.evictedVals = append(c.evictedVals, v)
}

// drainEvicted swaps out the buffered evicted key/vals, must be called
// with the lock held.
func (c *WeightedCache[K, V]) drainEvicted() (ks []K, vs []V) {
	if c.onEvictedCB != nil && len(c.evictedKeys) > 0 {
		ks, vs = c.evictedKeys, c.evictedVals
		c.initEvictBuffers()
	}
	return
}

// notifyEvicted invokes the registered callback for evicted key/vals,
// outside of the critical section.
func (c *WeightedCache[K, V]) notifyEvicted(ks []K, vs []V) {
	for i := 0; i < len(ks); i++ {
		c.onEvictedCB(ks[i], vs[i])
	}
}

// Purge is used to completely clear the cache.
func (c *WeightedCache[K, V]) Purge() {
	c.lock.Lock()
	c.lru.Purge()
	ks, vs := c.drainEvicted()
	c.lock.Unlock()
	if c.onEvictedCB != nil {
		c.notifyEvicted(ks, vs)
	}
}

// Add adds a value to the cache, evicting least recently used entries
// until the total weight fits within the capacity. Returns true if an
// eviction occurred.
func (c *WeightedCache[K, V]) Add(key K, value V) (evicted bool) {
	c.lock.Lock()
	evicted = c.lru.Add(key, value)
	ks, vs := c.drainEvicted()
	c.lock.Unlock()
	if c.onEvictedCB != nil {
		c.notifyEvicted(ks, vs)
	}
	return
}

// Get looks up a key's value from the cache.
func (c *WeightedCache[K, V]) Get(key K) (value V, ok bool) {
	c.lock.Lock()
	value, ok = c.lru.Get(key)
	c.lock.Unlock()
	return value, ok
}

// Contains checks if a key is in the cache, without updating the
// recent-ness or deleting it for being stale.
func (c *WeightedCache[K, V]) Contains(key K) bool {
	c.lock.RLock()
	containKey := c.lru.Contains(key)
	c.lock.RUnlock()
	return containKey
}

// Peek returns the key value (or undefined if not found) without updating
// the "recently used"-ness of the key.
func (c *WeightedCache[K, V]) Peek(key K) (value V, ok bool) {
	c.lock.RLock()
	value, ok = c.lru.Peek(key)
	c.lock.RUnlock()
	return value, ok
}

// ContainsOrAdd checks if a key is in the cache without updating the
// recent-ness or deleting it for being stale, and if not, adds the value.
// Returns whether found and whether an eviction occurred.
func (c *WeightedCache[K, V]) ContainsOrAdd(key K, value V) (ok, evicted bool) {
	c.lock.Lock()
	if c.lru.Contains(key) {
		c.lock.Unlock()
		return true, false
	}
	evicted = c.lru.Add(key, value)
	ks, vs := c.drainEvicted()
	c.lock.Unlock()
	if c.onEvictedCB != nil {
		c.notifyEvicted(ks, vs)
	}
	return false, evicted
}

// PeekOrAdd checks if a key is in the cache without updating the
// recent-ness or deleting it for being stale, and if not, adds the value.
// Returns whether found and whether an eviction occurred.
func (c *WeightedCache[K, V]) PeekOrAdd(key K, value V) (previous V, ok, evicted bool) {
	c.lock.Lock()
	previous, ok = c.lru.Peek(key)
	if ok {
		c.lock.Unlock()
		return previous, true, false
	}
	evicted = c.lru.Add(key, value)
	ks, vs := c.drainEvicted()
	c.lock.Unlock()
	if c.onEvictedCB != nil {
		c.notifyEvicted(ks, vs)
	}
	return
}

// Remove removes the provided key from the cache, returning if the
// key was contained.
func (c *WeightedCache[K, V]) Remove(key K) (present bool) {
	c.lock.Lock()
	present = c.lru.Remove(key)
	ks, vs := c.drainEvicted()
	c.lock.Unlock()
	if c.onEvictedCB != nil {
		c.notifyEvicted(ks, vs)
	}
	return
}

// Resize changes the cache weight capacity, returning the number of
// entries evicted.
func (c *WeightedCache[K, V]) Resize(capacity int64) (evicted int) {
	c.lock.Lock()
	evicted = c.lru.Resize(capacity)
	ks, vs := c.drainEvicted()
	c.lock.Unlock()
	if c.onEvictedCB != nil {
		c.notifyEvicted(ks, vs)
	}
	return evicted
}

// RemoveOldest removes the oldest item from the cache.
func (c *WeightedCache[K, V]) RemoveOldest() (key K, value V, ok bool) {
	c.lock.Lock()
	key, value, ok = c.lru.RemoveOldest()
	ks, vs := c.drainEvicted()
	c.lock.Unlock()
	if c.onEvictedCB != nil {
		c.notifyEvicted(ks, vs)
	}
	return
}

// GetOldest returns the oldest entry
func (c *WeightedCache[K, V]) GetOldest() (key K, value V, ok bool) {
	c.lock.RLock()
	key, value, ok = c.lru.GetOldest()
	c.lock.RUnlock()
	return
}

// Keys returns a slice of the keys in the cache, from oldest to newest.
func (c *WeightedCache[K, V]) Keys() []K {
	c.lock.RLock()
	keys := c.lru.Keys()
	c.lock.RUnlock()
	return keys
}

// Values returns a slice of the values in the cache, from oldest to newest.
func (c *WeightedCache[K, V]) Values() []V {
	c.lock.RLock()
	values := c.lru.Values()
	c.lock.RUnlock()
	return values
}

// Len returns the number of items in the cache.
func (c *WeightedCache[K, V]) Len() int {
	c.lock.RLock()
	length := c.lru.Len()
	c.lock.RUnlock()
	return length
}

// Weight returns the total weight of the items in the cache.
func (c *WeightedCache[K, V]) Weight() int64 {
	c.lock.RLock()
	weight := c.lru.Weight()
	c.lock.RUnlock()
	return weight
}

// Cap returns the weight capacity of the cache.
func (c *WeightedCache[K, V]) Cap() int64 {
	return c.lru.Cap()
}

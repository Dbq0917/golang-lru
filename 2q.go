// Copyright IBM Corp. 2014, 2025
// SPDX-License-Identifier: MPL-2.0

package lru

import (
	"errors"
	"sync"

	"github.com/hashicorp/golang-lru/v2/simplelru"
)

const (
	// Default2QRecentRatio is the ratio of the 2Q cache dedicated
	// to recently added entries that have only been accessed once.
	Default2QRecentRatio = 0.25

	// Default2QGhostEntries is the default ratio of ghost
	// entries kept to track entries recently evicted
	Default2QGhostEntries = 0.50
)

// TwoQueueCache is a thread-safe fixed size 2Q cache.
// 2Q is an enhancement over the standard LRU cache
// in that it tracks both frequently and recently used
// entries separately. This avoids a burst in access to new
// entries from evicting frequently used entries. It adds some
// additional tracking overhead to the standard LRU cache, and is
// computationally about 2x the cost, and adds some metadata over
// head. The ARCCache is similar, but does not require setting any
// parameters.
type TwoQueueCache[K comparable, V any] struct {
	size        int
	recentSize  int
	recentRatio float64
	ghostRatio  float64

	recent      simplelru.LRUCache[K, V]
	frequent    simplelru.LRUCache[K, V]
	recentEvict simplelru.LRUCache[K, struct{}]

	// onEvictedCB is the externally registered eviction callback. It is
	// invoked outside of the critical section so it is safe to re-enter
	// the cache from within the callback.
	onEvictedCB func(k K, v V)

	// evictedKeys/evictedVals buffer evicted entries while the lock is held.
	evictedKeys []K
	evictedVals []V

	// noEvict suppresses the internal sub-cache callbacks while an entry is
	// moved between segments rather than evicted from the cache.
	noEvict bool

	lock sync.RWMutex
}

// New2Q creates a new TwoQueueCache using the default
// values for the parameters.
func New2Q[K comparable, V any](size int) (*TwoQueueCache[K, V], error) {
	return New2QParams[K, V](size, Default2QRecentRatio, Default2QGhostEntries)
}

// New2QWithEvict creates a new TwoQueueCache using the default values for the
// parameters and the provided eviction callback. The callback is fired when an
// entry is evicted from the recently used or frequently used segment, or when
// a key falls out of the ghost (recently evicted) segment; ghost evictions
// report the zero value since the original value was already reported when the
// entry left the recently used segment. The callback is invoked outside of the
// cache lock, so re-entering the cache from within it is safe.
func New2QWithEvict[K comparable, V any](size int, onEvicted func(key K, value V)) (*TwoQueueCache[K, V], error) {
	return New2QParamsWithEvict[K, V](size, Default2QRecentRatio, Default2QGhostEntries, onEvicted)
}

// New2QParams creates a new TwoQueueCache using the provided
// parameter values.
func New2QParams[K comparable, V any](size int, recentRatio, ghostRatio float64) (*TwoQueueCache[K, V], error) {
	return New2QParamsWithEvict[K, V](size, recentRatio, ghostRatio, nil)
}

// New2QParamsWithEvict creates a new TwoQueueCache using the provided
// parameter values and eviction callback. See New2QWithEvict for the callback
// semantics.
func New2QParamsWithEvict[K comparable, V any](size int, recentRatio, ghostRatio float64, onEvicted func(key K, value V)) (*TwoQueueCache[K, V], error) {
	if size <= 0 {
		return nil, errors.New("invalid size")
	}
	if recentRatio < 0.0 || recentRatio > 1.0 {
		return nil, errors.New("invalid recent ratio")
	}
	if ghostRatio < 0.0 || ghostRatio > 1.0 {
		return nil, errors.New("invalid ghost ratio")
	}

	// Determine the sub-sizes
	recentSize := int(float64(size) * recentRatio)
	evictSize := int(float64(size) * ghostRatio)

	c := &TwoQueueCache[K, V]{
		size:        size,
		recentSize:  recentSize,
		recentRatio: recentRatio,
		ghostRatio:  ghostRatio,
		onEvictedCB: onEvicted,
	}
	if onEvicted != nil {
		c.initEvictBuffers()
	}

	// Only wire the internal callbacks when an external callback was
	// registered so that a nil-callback cache keeps zero callback overhead.
	var recentCB simplelru.EvictCallback[K, V]
	var frequentCB simplelru.EvictCallback[K, V]
	var ghostCB simplelru.EvictCallback[K, struct{}]
	if onEvicted != nil {
		recentCB = c.onEvicted
		frequentCB = c.onEvicted
		ghostCB = c.ghostEvicted
	}

	// Allocate the LRUs
	recent, err := simplelru.NewLRU[K, V](size, recentCB)
	if err != nil {
		return nil, err
	}
	frequent, err := simplelru.NewLRU[K, V](size, frequentCB)
	if err != nil {
		return nil, err
	}
	recentEvict, err := simplelru.NewLRU[K, struct{}](evictSize, ghostCB)
	if err != nil {
		return nil, err
	}

	c.recent = recent
	c.frequent = frequent
	c.recentEvict = recentEvict
	return c, nil
}

func (c *TwoQueueCache[K, V]) initEvictBuffers() {
	c.evictedKeys = make([]K, 0, DefaultEvictedBufferSize)
	c.evictedVals = make([]V, 0, DefaultEvictedBufferSize)
}

// onEvicted buffers entries evicted from the recently used or frequently used
// segment. It runs while the cache lock is held.
func (c *TwoQueueCache[K, V]) onEvicted(key K, value V) {
	if c.noEvict {
		return
	}
	c.evictedKeys = append(c.evictedKeys, key)
	c.evictedVals = append(c.evictedVals, value)
}

// ghostEvicted buffers keys that fall out of the ghost (recently evicted)
// segment. The original value was already reported when the entry left the
// recently used segment, so the zero value is reported.
func (c *TwoQueueCache[K, V]) ghostEvicted(key K, _ struct{}) {
	if c.noEvict {
		return
	}
	var zero V
	c.evictedKeys = append(c.evictedKeys, key)
	c.evictedVals = append(c.evictedVals, zero)
}

// drainEvicted takes ownership of the buffered evictions and resets the
// buffers. It must be called while holding the cache lock. The returned
// slices are only touched by the caller after the lock is released.
func (c *TwoQueueCache[K, V]) drainEvicted() (keys []K, vals []V) {
	if c.onEvictedCB == nil || len(c.evictedKeys) == 0 {
		return nil, nil
	}
	keys, vals = c.evictedKeys, c.evictedVals
	c.initEvictBuffers()
	return keys, vals
}

// invokeEvicted fires the registered callback outside of the cache lock so
// it is safe to re-enter the cache from within the callback. Evictions
// produced by re-entrant calls remain in the shared buffers and are drained
// by that call's own flush.
func (c *TwoQueueCache[K, V]) invokeEvicted(keys []K, vals []V) {
	for i := range keys {
		c.onEvictedCB(keys[i], vals[i])
	}
}

// Get looks up a key's value from the cache.
func (c *TwoQueueCache[K, V]) Get(key K) (value V, ok bool) {
	c.lock.Lock()
	// Check if this is a frequent value
	if val, ok := c.frequent.Get(key); ok {
		c.lock.Unlock()
		return val, ok
	}

	// If the value is contained in recent, then we
	// promote it to frequent
	if val, ok := c.recent.Peek(key); ok {
		// Promotion between segments is not an eviction.
		c.noEvict = true
		c.recent.Remove(key)
		c.frequent.Add(key, val)
		c.noEvict = false
		c.lock.Unlock()
		return val, ok
	}

	// No hit
	c.lock.Unlock()
	return
}

// Add adds a value to the cache.
func (c *TwoQueueCache[K, V]) Add(key K, value V) {
	c.lock.Lock()
	// Check if the value is frequently used already,
	// and just update the value
	if c.frequent.Contains(key) {
		c.frequent.Add(key, value)
		keys, vals := c.drainEvicted()
		c.lock.Unlock()
		c.invokeEvicted(keys, vals)
		return
	}

	// Check if the value is recently used, and promote
	// the value into the frequent list
	if c.recent.Contains(key) {
		// Promotion between segments is not an eviction.
		c.noEvict = true
		c.recent.Remove(key)
		c.frequent.Add(key, value)
		c.noEvict = false
		keys, vals := c.drainEvicted()
		c.lock.Unlock()
		c.invokeEvicted(keys, vals)
		return
	}

	// If the value was recently evicted, add it to the
	// frequently used list
	if c.recentEvict.Contains(key) {
		c.ensureSpace(true)
		// The key is being recalled from the ghost segment; removing it
		// there is not an eviction.
		c.noEvict = true
		c.recentEvict.Remove(key)
		c.noEvict = false
		c.frequent.Add(key, value)
		keys, vals := c.drainEvicted()
		c.lock.Unlock()
		c.invokeEvicted(keys, vals)
		return
	}

	// Add to the recently seen list
	c.ensureSpace(false)
	c.recent.Add(key, value)
	keys, vals := c.drainEvicted()
	c.lock.Unlock()
	c.invokeEvicted(keys, vals)
}

// ensureSpace is used to ensure we have space in the cache
func (c *TwoQueueCache[K, V]) ensureSpace(recentEvict bool) {
	// If we have space, nothing to do
	recentLen := c.recent.Len()
	freqLen := c.frequent.Len()
	if recentLen+freqLen < c.size {
		return
	}

	// If the recent buffer is larger than
	// the target, evict from there
	if recentLen > 0 && (recentLen > c.recentSize || (recentLen == c.recentSize && !recentEvict)) {
		k, _, _ := c.recent.RemoveOldest()
		// If the ghost segment is full, its oldest key is evicted and the
		// internal ghost callback reports it with the zero value.
		c.recentEvict.Add(k, struct{}{})
		return
	}

	// Remove from the frequent list otherwise
	c.frequent.RemoveOldest()
}

// Len returns the number of items in the cache.
func (c *TwoQueueCache[K, V]) Len() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.recent.Len() + c.frequent.Len()
}

// Cap returns the capacity of the cache
func (c *TwoQueueCache[K, V]) Cap() int {
	return c.size
}

// Resize changes the cache size.
func (c *TwoQueueCache[K, V]) Resize(size int) (evicted int) {
	c.lock.Lock()

	// Recalculate the sub-sizes
	recentSize := int(float64(size) * c.recentRatio)
	evictSize := int(float64(size) * c.ghostRatio)
	c.size = size
	c.recentSize = recentSize

	// ensureSpace
	diff := c.recent.Len() + c.frequent.Len() - size
	if diff < 0 {
		diff = 0
	}
	for i := 0; i < diff; i++ {
		c.ensureSpace(true)
	}

	// Reallocate the LRUs
	// Resizing the sub-caches only removes entries already accounted for
	// above (value-bearing segments) or stale ghost keys, so suppress the
	// internal callbacks to avoid duplicate notifications.
	c.noEvict = true
	c.recent.Resize(size)
	c.frequent.Resize(size)
	c.recentEvict.Resize(evictSize)
	c.noEvict = false

	keys, vals := c.drainEvicted()
	c.lock.Unlock()
	c.invokeEvicted(keys, vals)
	return diff
}

// Keys returns a slice of the keys in the cache.
// The frequently used keys are first in the returned slice.
func (c *TwoQueueCache[K, V]) Keys() []K {
	c.lock.RLock()
	defer c.lock.RUnlock()
	k1 := c.frequent.Keys()
	k2 := c.recent.Keys()
	return append(k1, k2...)
}

// Values returns a slice of the values in the cache.
// The frequently used values are first in the returned slice.
func (c *TwoQueueCache[K, V]) Values() []V {
	c.lock.RLock()
	defer c.lock.RUnlock()
	v1 := c.frequent.Values()
	v2 := c.recent.Values()
	return append(v1, v2...)
}

// Remove removes the provided key from the cache.
func (c *TwoQueueCache[K, V]) Remove(key K) {
	c.lock.Lock()
	if c.frequent.Remove(key) {
		keys, vals := c.drainEvicted()
		c.lock.Unlock()
		c.invokeEvicted(keys, vals)
		return
	}
	if c.recent.Remove(key) {
		keys, vals := c.drainEvicted()
		c.lock.Unlock()
		c.invokeEvicted(keys, vals)
		return
	}
	if c.recentEvict.Contains(key) {
		// An explicit removal of a ghost key is user-initiated, not an
		// eviction, so it must not trigger the callback.
		c.noEvict = true
		c.recentEvict.Remove(key)
		c.noEvict = false
		c.lock.Unlock()
		return
	}
	c.lock.Unlock()
}

// Purge is used to completely clear the cache.
func (c *TwoQueueCache[K, V]) Purge() {
	c.lock.Lock()
	// Entries leaving the value-bearing segments are reported; the ghost
	// entries were already reported when they left the recent segment, so
	// suppress their callback.
	c.noEvict = true
	c.recentEvict.Purge()
	c.noEvict = false
	c.recent.Purge()
	c.frequent.Purge()
	keys, vals := c.drainEvicted()
	c.lock.Unlock()
	c.invokeEvicted(keys, vals)
}

// Contains is used to check if the cache contains a key
// without updating recency or frequency.
func (c *TwoQueueCache[K, V]) Contains(key K) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.frequent.Contains(key) || c.recent.Contains(key)
}

// Peek is used to inspect the cache value of a key
// without updating recency or frequency.
func (c *TwoQueueCache[K, V]) Peek(key K) (value V, ok bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if val, ok := c.frequent.Peek(key); ok {
		return val, ok
	}
	return c.recent.Peek(key)
}

// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: MPL-2.0

package expirable

import (
	"container/heap"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/internal"
)

// EvictCallback is used to get a callback when a cache entry is evicted.
//
// Callbacks run in the goroutine that caused the eviction: for explicit
// removals (Add, Remove, RemoveOldest, Purge, Resize) they complete before
// the method returns; for expired entries they run in the background cleanup
// goroutine. They are always invoked outside the cache's internal lock, so
// they may safely re-enter the cache (e.g. call Get or Len). Because no lock
// is held while a callback runs, other goroutines may have mutated the cache
// by then, so callbacks must not assume they observe a consistent snapshot
// of the cache.
type EvictCallback[K comparable, V any] func(key K, value V)

// LRU implements a thread-safe LRU with expirable entries.
type LRU[K comparable, V any] struct {
	size      int
	evictList *internal.LruList[K, V]
	items     map[K]*internal.Entry[K, V]
	onEvict   EvictCallback[K, V]

	// expirable options
	mu     sync.Mutex
	ttl    time.Duration
	done   chan struct{}
	wake   chan struct{}
	reaper bool

	cleanupStopped bool

	// expiry is a min-heap ordered by ExpiresAt used to expire entries
	// with individual TTLs without scanning the whole cache.
	expiry expiryHeap[K, V]
}

// noEvictionTTL - very long ttl to prevent eviction
const noEvictionTTL = time.Hour * 24 * 365 * 10

// NewLRU returns a new thread-safe cache with expirable entries.
//
// Size parameter set to 0 makes cache of unlimited size, e.g. turns LRU mechanism off.
//
// Providing 0 TTL turns expiring off.
//
// Per-entry TTLs can be configured through AddWithTTL; see its documentation.
//
// onEvict, if non-nil, is invoked for each evicted entry outside the cache's
// internal lock; see EvictCallback for the guarantees callbacks get.
//
// A background goroutine reaps expired entries. It starts immediately when
// ttl is positive, and on the first finite-TTL Add otherwise. It runs until
// Close is called.
func NewLRU[K comparable, V any](size int, onEvict EvictCallback[K, V], ttl time.Duration) *LRU[K, V] {
	if size < 0 {
		size = 0
	}
	if ttl <= 0 {
		ttl = noEvictionTTL
	}

	res := LRU[K, V]{
		ttl:            ttl,
		size:           size,
		evictList:      internal.NewList[K, V](),
		items:          make(map[K]*internal.Entry[K, V]),
		onEvict:        onEvict,
		done:           make(chan struct{}),
		wake:           make(chan struct{}, 1),
		cleanupStopped: false,
	}

	if ttl != noEvictionTTL {
		res.startReaper()
	}
	return &res
}

// evictedEntry is a snapshot of a removed entry, kept so that the onEvict
// callback can be invoked after the lock is released.
type evictedEntry[K comparable, V any] struct {
	key   K
	value V
}

// fireCallbacks invokes the onEvict callback for each evicted entry.
// It must be called without holding c.mu so that callbacks can safely
// re-enter the cache.
func (c *LRU[K, V]) fireCallbacks(evicted []evictedEntry[K, V]) {
	if c.onEvict == nil {
		return
	}
	for _, e := range evicted {
		c.onEvict(e.key, e.value)
	}
}

// Purge clears the cache completely.
// onEvict is called for each evicted key, outside the cache's internal lock.
func (c *LRU[K, V]) Purge() {
	var evicted []evictedEntry[K, V]
	c.mu.Lock()
	for k, v := range c.items {
		delete(c.items, k)
		if c.onEvict != nil {
			evicted = append(evicted, evictedEntry[K, V]{key: k, value: v.Value})
		}
	}
	c.expiry = make(expiryHeap[K, V], 0)
	c.evictList.Init()
	c.mu.Unlock()
	c.fireCallbacks(evicted)
}

// Add adds a value to the cache using the cache's default TTL. Returns true
// if an eviction occurred. Returns false if there was no eviction: the item
// was already in the cache, or the size was not exceeded.
// If an eviction occurred, onEvict is invoked for the evicted entry outside
// the cache's internal lock.
func (c *LRU[K, V]) Add(key K, value V) (evicted bool) {
	return c.add(key, value, c.ttl)
}

// AddWithTTL adds a value to the cache with a per-entry TTL.
//
// The entry expires at now + ttl independently of the cache-wide TTL passed
// to NewLRU, so entries with different lifetimes (e.g. sessions, captchas,
// config) can share one cache without competing for separate capacity.
//
// A non-positive ttl makes the entry fall back to the cache's default TTL.
// Re-adding an existing key refreshes both its value and its expiration,
// using the provided ttl.
//
// Returns true if an LRU eviction occurred; onEvict is then invoked for the
// evicted entry outside the cache's internal lock.
func (c *LRU[K, V]) AddWithTTL(key K, value V, ttl time.Duration) (evicted bool) {
	if ttl <= 0 {
		ttl = c.ttl
	}
	return c.add(key, value, ttl)
}

// add inserts or refreshes an entry with the given TTL. Has to be called
// without the lock, which is managed here.
func (c *LRU[K, V]) add(key K, value V, ttl time.Duration) (evicted bool) {
	var evictedEntries []evictedEntry[K, V]
	c.mu.Lock()
	defer func() {
		c.mu.Unlock()
		c.fireCallbacks(evictedEntries)
	}()
	now := time.Now()

	// Check for existing item
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value = value
		ent.ExpiresAt = now.Add(ttl)
		heap.Fix(&c.expiry, ent.HeapIndex)
		c.wakeCleanupIfEarliest(ent)
		return false
	}

	// Add new item
	ent := c.evictList.PushFrontExpirable(key, value, now.Add(ttl))
	c.items[key] = ent
	heap.Push(&c.expiry, ent)
	if ttl != noEvictionTTL {
		c.startReaper()
	}
	c.wakeCleanupIfEarliest(ent)

	evicted = c.size > 0 && c.evictList.Length() > c.size
	// Verify size not exceeded
	if evicted {
		c.removeOldest(&evictedEntries)
	}
	return evicted
}

// wakeCleanupIfEarliest nudges the reaper when ent is now the soonest to
// expire, so a short-TTL entry added among long-TTL ones is reaped on time.
// Has to be called with lock!
func (c *LRU[K, V]) wakeCleanupIfEarliest(ent *internal.Entry[K, V]) {
	if c.expiry.Len() == 0 || c.expiry[0] != ent {
		return
	}
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// Get looks up a key's value from the cache.
func (c *LRU[K, V]) Get(key K) (value V, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var ent *internal.Entry[K, V]
	if ent, ok = c.items[key]; ok {
		// Expired item check
		if !c.cleanupStopped && time.Now().After(ent.ExpiresAt) {
			return value, false
		}
		c.evictList.MoveToFront(ent)
		return ent.Value, true
	}
	return
}

// Contains checks if a key is in the cache, without updating the recent-ness
// or deleting it for being stale.
func (c *LRU[K, V]) Contains(key K) (ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok = c.items[key]
	return ok
}

// Peek returns the key value (or undefined if not found) without updating
// the "recently used"-ness of the key.
func (c *LRU[K, V]) Peek(key K) (value V, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var ent *internal.Entry[K, V]
	if ent, ok = c.items[key]; ok {
		// Expired item check
		if !c.cleanupStopped && time.Now().After(ent.ExpiresAt) {
			return value, false
		}
		return ent.Value, true
	}
	return
}

// Remove removes the provided key from the cache, returning if the
// key was contained. If it was, onEvict is invoked for the removed
// entry outside the cache's internal lock.
func (c *LRU[K, V]) Remove(key K) bool {
	var evictedEntries []evictedEntry[K, V]
	c.mu.Lock()
	defer func() {
		c.mu.Unlock()
		c.fireCallbacks(evictedEntries)
	}()
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent, &evictedEntries)
		return true
	}
	return false
}

// RemoveOldest removes the oldest item from the cache.
// If there was one, onEvict is invoked for it outside the cache's internal lock.
func (c *LRU[K, V]) RemoveOldest() (key K, value V, ok bool) {
	var evictedEntries []evictedEntry[K, V]
	c.mu.Lock()
	defer func() {
		c.mu.Unlock()
		c.fireCallbacks(evictedEntries)
	}()
	if ent := c.evictList.Back(); ent != nil {
		c.removeElement(ent, &evictedEntries)
		return ent.Key, ent.Value, true
	}
	return
}

// GetOldest returns the oldest entry
func (c *LRU[K, V]) GetOldest() (key K, value V, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ent := c.evictList.Back(); ent != nil {
		return ent.Key, ent.Value, true
	}
	return
}

// Keys returns a slice of the keys in the cache, from oldest to newest.
// Expired entries are filtered out.
func (c *LRU[K, V]) Keys() []K {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]K, 0, len(c.items))
	now := time.Now()
	for ent := c.evictList.Back(); ent != nil; ent = ent.PrevEntry() {
		if !c.cleanupStopped && now.After(ent.ExpiresAt) {
			continue
		}
		keys = append(keys, ent.Key)
	}
	return keys
}

// Values returns a slice of the values in the cache, from oldest to newest.
// Expired entries are filtered out.
func (c *LRU[K, V]) Values() []V {
	c.mu.Lock()
	defer c.mu.Unlock()
	values := make([]V, 0, len(c.items))
	now := time.Now()
	for ent := c.evictList.Back(); ent != nil; ent = ent.PrevEntry() {
		if !c.cleanupStopped && now.After(ent.ExpiresAt) {
			continue
		}
		values = append(values, ent.Value)
	}
	return values
}

// Len returns the number of items in the cache.
func (c *LRU[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.evictList.Length()
}

// Resize changes the cache size. Size of 0 means unlimited.
// onEvict is invoked for each evicted entry outside the cache's internal lock.
func (c *LRU[K, V]) Resize(size int) (evicted int) {
	var evictedEntries []evictedEntry[K, V]
	c.mu.Lock()
	defer func() {
		c.mu.Unlock()
		c.fireCallbacks(evictedEntries)
	}()
	if size <= 0 {
		c.size = 0
		return 0
	}
	diff := c.evictList.Length() - size
	if diff < 0 {
		diff = 0
	}
	for i := 0; i < diff; i++ {
		c.removeOldest(&evictedEntries)
	}
	c.size = size
	return diff
}

// Close destroys cleanup goroutine. To clean up the cache, run Purge() before Close().
func (c *LRU[K, V]) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.done:
		return
	default:
	}
	close(c.done)
	c.cleanupStopped = true
}

// removeOldest removes the oldest item from the cache. Has to be called with lock!
func (c *LRU[K, V]) removeOldest(evicted *[]evictedEntry[K, V]) {
	if ent := c.evictList.Back(); ent != nil {
		c.removeElement(ent, evicted)
	}
}

// removeElement is used to remove a given list element from the cache. Has to be called with lock!
// The removed entry is recorded in evicted instead of invoking onEvict
// directly, so the callback can run after the lock is released.
func (c *LRU[K, V]) removeElement(e *internal.Entry[K, V], evicted *[]evictedEntry[K, V]) {
	c.evictList.Remove(e)
	delete(c.items, e.Key)
	if e.HeapIndex >= 0 {
		heap.Remove(&c.expiry, e.HeapIndex)
	}
	if c.onEvict != nil {
		*evicted = append(*evicted, evictedEntry[K, V]{key: e.Key, value: e.Value})
	}
}

// removeFromExpiry drops an entry that has already been popped from the
// expiry heap by deleteExpired, without touching the heap again. Has to be
// called with lock!
func (c *LRU[K, V]) removeFromExpiry(e *internal.Entry[K, V], evicted *[]evictedEntry[K, V]) {
	c.evictList.Remove(e)
	delete(c.items, e.Key)
	if c.onEvict != nil {
		*evicted = append(*evicted, evictedEntry[K, V]{key: e.Key, value: e.Value})
	}
}

// deleteExpired removes every entry whose ExpiresAt is in the past.
// The expiry min-heap orders entries by expiration, so only expired entries
// are touched: the scan stops at the first still-valid entry instead of
// walking the whole cache.
func (c *LRU[K, V]) deleteExpired() {
	var evicted []evictedEntry[K, V]
	c.mu.Lock()
	now := time.Now()
	for c.expiry.Len() > 0 && !now.Before(c.expiry[0].ExpiresAt) {
		ent := heap.Pop(&c.expiry).(*internal.Entry[K, V])
		c.removeFromExpiry(ent, &evicted)
	}
	c.mu.Unlock()
	c.fireCallbacks(evicted)
}

// Cap returns the capacity of the cache
func (c *LRU[K, V]) Cap() int {
	return c.size
}

// Restart recreates the cleanup goroutine after Close() has been called.
func (c *LRU[K, V]) Restart() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if the goroutine is already running
	select {
	case <-c.done:
		// Channel is closed, need to recreate it
		c.done = make(chan struct{})
		c.wake = make(chan struct{}, 1)
		c.cleanupStopped = false
		c.startReaper()
	default:
		// Goroutine is already running, nothing to do
	}
}

// startReaper launches the background cleanup goroutine once. It is started
// eagerly for a finite default TTL, and lazily on the first finite-TTL entry
// when expiration was disabled at construction. Has to be called with lock!
func (c *LRU[K, V]) startReaper() {
	if c.reaper || c.cleanupStopped {
		return
	}
	c.reaper = true

	go c.reap(c.done, c.wake)
}

// reap is the cleanup loop. It sleeps exactly until the soonest expiration
// (or until a sooner-dying entry is added), then removes expired entries
// from the top of the heap, so entries with different TTLs never take each
// other down and no full-table scan is needed.
func (c *LRU[K, V]) reap(done <-chan struct{}, wake <-chan struct{}) {
	var timer *time.Timer
	for {
		c.mu.Lock()
		if c.expiry.Len() == 0 {
			c.mu.Unlock()

			// Nothing to expire yet: park until an entry is added or
			// the cache is closed.
			select {
			case <-done:
				return
			case <-wake:
			}
			continue
		}
		wait := time.Until(c.expiry[0].ExpiresAt)
		c.mu.Unlock()

		if wait <= 0 {
			wait = time.Nanosecond
		}
		if timer == nil {
			timer = time.NewTimer(wait)
		} else {
			timer.Reset(wait)
		}

		select {
		case <-done:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-wake:
			// A new sooner-expiring entry may have arrived; recompute.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
			c.deleteExpired()
		}
	}
}

// expiryHeap is a min-heap of entries ordered by ExpiresAt, soonest first.
// It mirrors the entries tracked in c.items and keeps each entry's position
// in sync through Entry.HeapIndex.
type expiryHeap[K comparable, V any] []*internal.Entry[K, V]

func (h expiryHeap[K, V]) Len() int { return len(h) }

func (h expiryHeap[K, V]) Less(i, j int) bool {
	return h[i].ExpiresAt.Before(h[j].ExpiresAt)
}

func (h expiryHeap[K, V]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].HeapIndex = i
	h[j].HeapIndex = j
}

func (h *expiryHeap[K, V]) Push(x any) {
	ent := x.(*internal.Entry[K, V])
	ent.HeapIndex = len(*h)
	*h = append(*h, ent)
}

func (h *expiryHeap[K, V]) Pop() any {
	old := *h
	n := len(old)
	ent := old[n-1]
	old[n-1] = nil
	ent.HeapIndex = -1
	*h = old[:n-1]
	return ent
}

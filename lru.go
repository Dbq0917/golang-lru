// Copyright IBM Corp. 2014, 2025
// SPDX-License-Identifier: MPL-2.0

package lru

import (
	"errors"
	"hash/maphash"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/golang-lru/v2/internal"
)

const (
	// DefaultEvictedBufferSize defines the default buffer size to store evicted key/val
	DefaultEvictedBufferSize = 16

	maxShardCount      = 64
	shardCacheLineSize = 64
)

// Cache is a thread-safe fixed size LRU cache.
//
// Keys are hashed into independently locked shards. Every access receives a
// monotonic global recency sequence. Each shard publishes the sequence of its
// oldest entry in tailRecent, allowing eviction to choose the globally oldest
// tail without making readers contend on one cache-wide mutex.
type Cache[K comparable, V any] struct {
	recent atomic.Uint64
	_      [shardCacheLineSize - 8]byte

	length   atomic.Int64
	capacity atomic.Int64
	_        [shardCacheLineSize - 16]byte

	evictionMu sync.Mutex
	shards     []*cacheShard[K, V]
	shardMask  uint64
	hashSeed   maphash.Seed

	// tailRecent is indexed by shard. Zero means an empty shard.
	tailRecent [maxShardCount]atomic.Uint64

	onEvictedCB func(k K, v V)
}

type cacheShard[K comparable, V any] struct {
	id    int
	mu    sync.RWMutex
	items map[K]*internal.Entry[K, V]
	list  *internal.LruList[K, V]

	// Separate neighboring shard locks onto different cache lines.
	_ [shardCacheLineSize]byte
}

type entrySnapshot[K comparable, V any] struct {
	recent uint64
	key    K
	value  V
}

type evictedEntry[K comparable, V any] struct {
	key   K
	value V
}

// New creates an LRU of the given size.
func New[K comparable, V any](size int) (*Cache[K, V], error) {
	return NewWithEvict[K, V](size, nil)
}

// NewWithEvict constructs a fixed size cache with the given eviction
// callback.
func NewWithEvict[K comparable, V any](size int, onEvicted func(key K, value V)) (*Cache[K, V], error) {
	if size <= 0 {
		return nil, errors.New("must provide a positive size")
	}

	shardCount := defaultShardCount(size)
	c := &Cache[K, V]{
		shards:      make([]*cacheShard[K, V], shardCount),
		shardMask:   uint64(shardCount - 1),
		hashSeed:    maphash.MakeSeed(),
		onEvictedCB: onEvicted,
	}
	c.capacity.Store(int64(size))

	for i := range c.shards {
		c.shards[i] = &cacheShard[K, V]{
			id:    i,
			items: make(map[K]*internal.Entry[K, V]),
			list:  internal.NewList[K, V](),
		}
	}

	return c, nil
}

func defaultShardCount(size int) int {
	desired := runtime.GOMAXPROCS(0) * 4
	if desired < 4 {
		desired = 4
	}
	if desired > maxShardCount {
		desired = maxShardCount
	}

	count := 1
	for count < desired {
		count <<= 1
	}

	// Tiny caches have too few resident entries to benefit from many shards.
	sizePower := 1
	for sizePower < size && sizePower < count {
		sizePower <<= 1
	}
	if sizePower < count {
		count = sizePower
	}

	return count
}

func (c *Cache[K, V]) shardForKey(key K) *cacheShard[K, V] {
	index := maphash.Comparable(c.hashSeed, key) & c.shardMask
	return c.shards[index]
}

func (c *Cache[K, V]) insertNewLocked(shard *cacheShard[K, V], key K, value V) *internal.Entry[K, V] {
	entry := shard.list.PushFront(key, value)
	entry.Recent = c.recent.Add(1)
	shard.items[key] = entry
	c.length.Add(1)
	return entry
}

func removeEntryLocked[K comparable, V any](shard *cacheShard[K, V], entry *internal.Entry[K, V]) (K, V) {
	key, value := entry.Key, entry.Value
	shard.list.Remove(entry)
	delete(shard.items, key)
	return key, value
}

// publishTailLocked makes the shard's current oldest recency visible to
// lock-free global victim selection. shard.mu must be held.
func (c *Cache[K, V]) publishTailLocked(shardID int) {
	entry := c.shards[shardID].list.Back()
	if entry == nil {
		c.tailRecent[shardID].Store(0)
	} else {
		c.tailRecent[shardID].Store(entry.Recent)
	}
}

func (c *Cache[K, V]) notifyEvicted(key K, value V) {
	if c.onEvictedCB != nil {
		c.onEvictedCB(key, value)
	}
}

// Purge is used to completely clear the cache.
func (c *Cache[K, V]) Purge() {
	var entries []evictedEntry[K, V]

	c.evictionMu.Lock()
	c.lockAllShards()

	if c.onEvictedCB != nil {
		for _, shard := range c.shards {
			for entry := shard.list.Back(); entry != nil; entry = entry.PrevEntry() {
				entries = append(entries, evictedEntry[K, V]{
					key:   entry.Key,
					value: entry.Value,
				})
			}
		}
	}

	for i, shard := range c.shards {
		shard.items = make(map[K]*internal.Entry[K, V])
		shard.list.Init()
		c.tailRecent[i].Store(0)
	}
	c.length.Store(0)

	c.unlockAllShards()
	c.evictionMu.Unlock()

	for _, entry := range entries {
		c.onEvictedCB(entry.key, entry.value)
	}
}

// Add adds a value to the cache. Returns true if an eviction occurred.
func (c *Cache[K, V]) Add(key K, value V) (evicted bool) {
	shard := c.shardForKey(key)

	shard.mu.Lock()
	oldTail := shard.list.Back()
	if entry, ok := shard.items[key]; ok {
		touchesTail := entry == oldTail
		shard.list.MoveToFront(entry)
		entry.Value = value
		entry.Recent = c.recent.Add(1)
		if touchesTail {
			c.publishTailLocked(shard.id)
		}
		shard.mu.Unlock()
		return false
	}

	c.insertNewLocked(shard, key, value)
	if oldTail == nil {
		c.publishTailLocked(shard.id)
	}
	shard.mu.Unlock()

	evictedKey, evictedValue, ok := c.evictForCapacity()
	if ok {
		c.notifyEvicted(evictedKey, evictedValue)
	}
	return ok
}

// Get looks up a key's value from the cache.
func (c *Cache[K, V]) Get(key K) (value V, ok bool) {
	shard := c.shardForKey(key)

	shard.mu.Lock()
	entry, found := shard.items[key]
	if !found {
		shard.mu.Unlock()
		return value, false
	}

	touchesTail := entry == shard.list.Back()
	shard.list.MoveToFront(entry)
	entry.Recent = c.recent.Add(1)
	value = entry.Value
	if touchesTail {
		c.publishTailLocked(shard.id)
	}
	shard.mu.Unlock()

	return value, true
}

// Contains checks if a key is in the cache, without updating the
// recent-ness or deleting it for being stale.
func (c *Cache[K, V]) Contains(key K) bool {
	shard := c.shardForKey(key)

	shard.mu.RLock()
	_, ok := shard.items[key]
	shard.mu.RUnlock()

	return ok
}

// Peek returns the key value (or undefined if not found) without updating
// the "recently used"-ness of the key.
func (c *Cache[K, V]) Peek(key K) (value V, ok bool) {
	shard := c.shardForKey(key)

	shard.mu.RLock()
	entry, ok := shard.items[key]
	if ok {
		value = entry.Value
	}
	shard.mu.RUnlock()

	return value, ok
}

// ContainsOrAdd checks if a key is in the cache without updating the
// recent-ness or deleting it for being stale, and if not, adds the value.
// Returns whether found and whether an eviction occurred.
func (c *Cache[K, V]) ContainsOrAdd(key K, value V) (ok, evicted bool) {
	shard := c.shardForKey(key)

	shard.mu.Lock()
	if _, exists := shard.items[key]; exists {
		shard.mu.Unlock()
		return true, false
	}

	wasEmpty := shard.list.Back() == nil
	c.insertNewLocked(shard, key, value)
	if wasEmpty {
		c.publishTailLocked(shard.id)
	}
	shard.mu.Unlock()

	evictedKey, evictedValue, evictionOK := c.evictForCapacity()
	if evictionOK {
		c.notifyEvicted(evictedKey, evictedValue)
	}
	return false, evictionOK
}

// PeekOrAdd checks if a key is in the cache without updating the
// recent-ness or deleting it for being stale, and if not, adds the value.
// Returns whether found and whether an eviction occurred.
func (c *Cache[K, V]) PeekOrAdd(key K, value V) (previous V, ok, evicted bool) {
	shard := c.shardForKey(key)

	shard.mu.Lock()
	if entry, exists := shard.items[key]; exists {
		previous = entry.Value
		shard.mu.Unlock()
		return previous, true, false
	}

	wasEmpty := shard.list.Back() == nil
	c.insertNewLocked(shard, key, value)
	if wasEmpty {
		c.publishTailLocked(shard.id)
	}
	shard.mu.Unlock()

	evictedKey, evictedValue, evictionOK := c.evictForCapacity()
	if evictionOK {
		c.notifyEvicted(evictedKey, evictedValue)
	}
	return previous, false, evictionOK
}

// Remove removes the provided key from the cache.
func (c *Cache[K, V]) Remove(key K) (present bool) {
	shard := c.shardForKey(key)

	shard.mu.Lock()
	entry, ok := shard.items[key]
	if !ok {
		shard.mu.Unlock()
		return false
	}

	removedTail := entry == shard.list.Back()
	removedKey, removedValue := removeEntryLocked(shard, entry)
	c.length.Add(-1)
	if removedTail {
		c.publishTailLocked(shard.id)
	}
	shard.mu.Unlock()

	c.notifyEvicted(removedKey, removedValue)
	return true
}

// Resize changes the cache size.
func (c *Cache[K, V]) Resize(size int) (evicted int) {
	c.capacity.Store(int64(size))

	var entries []evictedEntry[K, V]
	c.evictionMu.Lock()
	for c.length.Load() > int64(size) {
		key, value, ok := c.removeOldestLocked()
		if !ok {
			break
		}
		evicted++
		if c.onEvictedCB != nil {
			entries = append(entries, evictedEntry[K, V]{key: key, value: value})
		}
	}
	c.evictionMu.Unlock()

	for _, entry := range entries {
		c.onEvictedCB(entry.key, entry.value)
	}
	return evicted
}

// RemoveOldest removes the oldest item from the cache.
func (c *Cache[K, V]) RemoveOldest() (key K, value V, ok bool) {
	c.evictionMu.Lock()
	key, value, ok = c.removeOldestLocked()
	c.evictionMu.Unlock()

	if ok {
		c.notifyEvicted(key, value)
	}
	return key, value, ok
}

// GetOldest returns the oldest entry
func (c *Cache[K, V]) GetOldest() (key K, value V, ok bool) {
	c.lockAllShardsForRead()

	var oldest *internal.Entry[K, V]
	oldestRecent := ^uint64(0)
	for _, shard := range c.shards {
		entry := shard.list.Back()
		if entry != nil && entry.Recent < oldestRecent {
			oldest = entry
			oldestRecent = entry.Recent
		}
	}

	if oldest != nil {
		key, value, ok = oldest.Key, oldest.Value, true
	}

	c.unlockAllShardsForRead()
	return key, value, ok
}

// Keys returns a slice of the keys in the cache, from oldest to newest.
func (c *Cache[K, V]) Keys() []K {
	entries := c.snapshotEntries()
	keys := make([]K, len(entries))
	for i, entry := range entries {
		keys[i] = entry.key
	}
	return keys
}

// Values returns a slice of the values in the cache, from oldest to newest.
func (c *Cache[K, V]) Values() []V {
	entries := c.snapshotEntries()
	values := make([]V, len(entries))
	for i, entry := range entries {
		values[i] = entry.value
	}
	return values
}

// Len returns the number of items in the cache.
func (c *Cache[K, V]) Len() int {
	return int(c.length.Load())
}

// Cap returns the capacity of the cache
func (c *Cache[K, V]) Cap() int {
	return int(c.capacity.Load())
}

func (c *Cache[K, V]) evictForCapacity() (key K, value V, ok bool) {
	for {
		if c.length.Load() <= c.capacity.Load() {
			return key, value, false
		}

		candidateShard, candidateRecent := c.oldestTailCandidate()
		if candidateShard == -1 {
			return key, value, false
		}

		shard := c.shards[candidateShard]
		shard.mu.Lock()
		entry := shard.list.Back()
		if entry == nil || entry.Recent != candidateRecent {
			c.publishTailLocked(candidateShard)
			shard.mu.Unlock()
			runtime.Gosched()
			continue
		}

		// Claim the eviction only after the victim is locked. This keeps the
		// authorized length consistent with the entry being removed and avoids
		// leaving a dangling claim if Purge concurrently clears the shards.
		for {
			current := c.length.Load()
			capacity := c.capacity.Load()
			if current <= capacity {
				shard.mu.Unlock()
				return key, value, false
			}
			if c.length.CompareAndSwap(current, current-1) {
				break
			}
		}

		key, value = removeEntryLocked(shard, entry)
		c.publishTailLocked(candidateShard)
		shard.mu.Unlock()
		return key, value, true
	}
}

func (c *Cache[K, V]) oldestTailCandidate() (int, uint64) {
	candidateShard := -1
	candidateRecent := ^uint64(0)
	for i := range c.shards {
		recent := c.tailRecent[i].Load()
		if recent != 0 && recent < candidateRecent {
			candidateRecent = recent
			candidateShard = i
		}
	}
	return candidateShard, candidateRecent
}

// removeOldestLocked removes the globally oldest tail. Callers that also use
// Resize/RemoveOldest serialization may hold evictionMu, but shard locks alone
// make the operation safe.
func (c *Cache[K, V]) removeOldestLocked() (key K, value V, ok bool) {
	for {
		candidateShard, candidateRecent := c.oldestTailCandidate()
		if candidateShard == -1 {
			return key, value, false
		}

		shard := c.shards[candidateShard]
		shard.mu.Lock()
		entry := shard.list.Back()
		if entry != nil && entry.Recent == candidateRecent {
			key, value = removeEntryLocked(shard, entry)
			c.length.Add(-1)
			c.publishTailLocked(candidateShard)
			shard.mu.Unlock()
			return key, value, true
		}

		// The selected tail changed while selecting it. Its published value is
		// repaired by the mutation path, but re-publish to make this loop
		// robust against any future caller that changes the list directly.
		c.publishTailLocked(candidateShard)
		shard.mu.Unlock()
		runtime.Gosched()
	}
}

func (c *Cache[K, V]) snapshotEntries() []entrySnapshot[K, V] {
	c.lockAllShardsForRead()

	count := 0
	for _, shard := range c.shards {
		count += shard.list.Length()
	}

	entries := make([]entrySnapshot[K, V], 0, count)
	for _, shard := range c.shards {
		for entry := shard.list.Back(); entry != nil; entry = entry.PrevEntry() {
			entries = append(entries, entrySnapshot[K, V]{
				recent: entry.Recent,
				key:    entry.Key,
				value:  entry.Value,
			})
		}
	}

	c.unlockAllShardsForRead()

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].recent < entries[j].recent
	})
	return entries
}

func (c *Cache[K, V]) lockAllShardsForRead() {
	for _, shard := range c.shards {
		shard.mu.RLock()
	}
}

func (c *Cache[K, V]) unlockAllShardsForRead() {
	for i := len(c.shards) - 1; i >= 0; i-- {
		c.shards[i].mu.RUnlock()
	}
}

func (c *Cache[K, V]) lockAllShards() {
	for _, shard := range c.shards {
		shard.mu.Lock()
	}
}

func (c *Cache[K, V]) unlockAllShards() {
	for i := len(c.shards) - 1; i >= 0; i-- {
		c.shards[i].mu.Unlock()
	}
}

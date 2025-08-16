package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"
)

// QueryCache implements an LRU cache with TTL support for query results.
// It provides fast access to frequently used query results while preventing
// memory exhaustion through size limits and time-based expiration.
//
// Features:
// - LRU (Least Recently Used) eviction policy
// - TTL (Time To Live) automatic expiration
// - Thread-safe concurrent access
// - Configurable size and TTL limits
// - Cache statistics for monitoring
// - Query normalization for consistent caching
type QueryCache struct {
	cache       map[string]*CacheEntry // Main cache storage
	lruList     *LRUNode               // LRU linked list for eviction
	config      QueryCacheConfig       // Cache configuration
	mutex       sync.RWMutex           // Thread-safe access
	stats       CacheStats             // Cache performance statistics
	lastCleanup time.Time              // Last cleanup timestamp
}

// CacheEntry represents a single cached query result with metadata.
type CacheEntry struct {
	Key         string      // Cache key (query hash)
	Response    RPCResponse // Cached query response
	CreatedAt   time.Time   // When the entry was cached
	AccessedAt  time.Time   // Last access time
	AccessCount int64       // Number of times accessed
	prev        *CacheEntry // Previous entry in LRU list
	next        *CacheEntry // Next entry in LRU list
}

// LRUNode represents the head of the LRU doubly-linked list.
type LRUNode struct {
	head *CacheEntry // Most recently used entry
	tail *CacheEntry // Least recently used entry
	size int         // Current number of entries in list
}

// QueryCacheConfig defines configuration options for the query cache.
type QueryCacheConfig struct {
	MaxSize         int           // Maximum number of cached entries
	TTL             time.Duration // Time to live for cache entries
	CleanupInterval time.Duration // How often to run cleanup (remove expired entries)
	Enabled         bool          // Whether caching is enabled
	LogLevel        bool          // Whether to enable detailed logging
}

// CacheStats contains cache performance statistics.
type CacheStats struct {
	Hits          int64        // Number of cache hits
	Misses        int64        // Number of cache misses
	Evictions     int64        // Number of entries evicted
	Expirations   int64        // Number of entries expired
	TotalRequests int64        // Total cache requests
	LastCleanup   time.Time    // Last cleanup time
	CurrentSize   int          // Current number of cached entries
	mutex         sync.RWMutex // Thread-safe stats access
}

// DefaultQueryCacheConfig returns a default cache configuration optimized for typical workloads.
func DefaultQueryCacheConfig() QueryCacheConfig {
	return QueryCacheConfig{
		MaxSize:         1000,             // Cache up to 1000 queries
		TTL:             15 * time.Minute, // Entries expire after 15 minutes
		CleanupInterval: 5 * time.Minute,  // Cleanup every 5 minutes
		Enabled:         true,             // Enable caching by default
		LogLevel:        false,            // Default: minimal logging
	}
}

// NewQueryCache creates a new query cache with the specified configuration.
//
// Parameters:
//   - config: Cache configuration options
//
// Returns:
//   - *QueryCache: Configured cache instance ready for use
func NewQueryCache(config QueryCacheConfig) *QueryCache {
	if config.MaxSize <= 0 {
		config.MaxSize = 1000
	}
	if config.TTL <= 0 {
		config.TTL = 15 * time.Minute
	}
	if config.CleanupInterval <= 0 {
		config.CleanupInterval = 5 * time.Minute
	}

	cache := &QueryCache{
		cache:       make(map[string]*CacheEntry),
		lruList:     &LRUNode{},
		config:      config,
		stats:       CacheStats{},
		lastCleanup: time.Now(),
	}

	if config.LogLevel {
		log.Printf("[server] Query cache initialized: maxSize=%d, ttl=%v, cleanup=%v",
			config.MaxSize, config.TTL, config.CleanupInterval)
	}

	return cache
}

// Get retrieves a cached query result if it exists and is still valid.
//
// Parameters:
//   - query: SQL query string
//   - params: Query parameters
//
// Returns:
//   - *RPCResponse: Cached response if found and valid
//   - bool: Whether a valid cache entry was found
func (qc *QueryCache) Get(query string, params []interface{}) (*RPCResponse, bool) {
	if !qc.config.Enabled {
		return nil, false
	}

	qc.mutex.Lock()
	defer qc.mutex.Unlock()

	// Generate cache key from normalized query and parameters
	key := qc.generateCacheKey(query, params)

	// Update total requests
	qc.stats.mutex.Lock()
	qc.stats.TotalRequests++
	qc.stats.mutex.Unlock()

	// Check if entry exists
	entry, exists := qc.cache[key]
	if !exists {
		qc.recordMiss()
		return nil, false
	}

	// Check if entry has expired
	if time.Since(entry.CreatedAt) > qc.config.TTL {
		// Entry expired, remove it
		qc.removeEntry(entry)
		qc.recordExpiration()
		return nil, false
	}

	// Entry is valid, update access info and move to front
	entry.AccessedAt = time.Now()
	entry.AccessCount++
	qc.moveToFront(entry)
	qc.recordHit()

	// Return a copy of the cached response
	return &entry.Response, true
}

// Set stores a query result in the cache.
//
// Parameters:
//   - query: SQL query string
//   - params: Query parameters
//   - response: Query response to cache
func (qc *QueryCache) Set(query string, params []interface{}, response RPCResponse) {
	if !qc.config.Enabled {
		return
	}

	qc.mutex.Lock()
	defer qc.mutex.Unlock()

	// Generate cache key
	key := qc.generateCacheKey(query, params)

	// Check if entry already exists
	if existing, exists := qc.cache[key]; exists {
		// Update existing entry
		existing.Response = response
		existing.CreatedAt = time.Now()
		existing.AccessedAt = time.Now()
		existing.AccessCount++
		qc.moveToFront(existing)
		return
	}

	// Create new cache entry
	entry := &CacheEntry{
		Key:         key,
		Response:    response,
		CreatedAt:   time.Now(),
		AccessedAt:  time.Now(),
		AccessCount: 1,
	}

	// Add to cache
	qc.cache[key] = entry
	qc.addToFront(entry)

	// Check if we need to evict entries
	if qc.lruList.size > qc.config.MaxSize {
		qc.evictLRU()
	}

	// Periodic cleanup
	if time.Since(qc.lastCleanup) > qc.config.CleanupInterval {
		go qc.cleanupExpired()
	}
}

// Clear removes all entries from the cache.
func (qc *QueryCache) Clear() {
	qc.mutex.Lock()
	defer qc.mutex.Unlock()

	qc.cache = make(map[string]*CacheEntry)
	qc.lruList = &LRUNode{}

	if qc.config.LogLevel {
		log.Printf("[server] Query cache cleared")
	}
}

// GetStats returns current cache statistics.
func (qc *QueryCache) GetStats() CacheStats {
	qc.stats.mutex.RLock()
	defer qc.stats.mutex.RUnlock()

	qc.mutex.RLock()
	currentSize := len(qc.cache)
	qc.mutex.RUnlock()

	// Return a copy of the stats without the mutex
	return CacheStats{
		Hits:          qc.stats.Hits,
		Misses:        qc.stats.Misses,
		Evictions:     qc.stats.Evictions,
		Expirations:   qc.stats.Expirations,
		TotalRequests: qc.stats.TotalRequests,
		LastCleanup:   qc.stats.LastCleanup,
		CurrentSize:   currentSize,
		// Don't copy the mutex
	}
}

// generateCacheKey creates a consistent cache key from query and parameters.
func (qc *QueryCache) generateCacheKey(query string, params []interface{}) string {
	// Normalize query (remove extra whitespace, convert to lowercase)
	normalizedQuery := normalizeQuery(query)

	// Create a struct to ensure consistent JSON marshaling
	cacheKey := struct {
		Query  string        `json:"query"`
		Params []interface{} `json:"params"`
	}{
		Query:  normalizedQuery,
		Params: params,
	}

	// Marshal to JSON for consistent key generation
	jsonBytes, _ := json.Marshal(cacheKey)

	// Generate SHA256 hash for the key
	hash := sha256.Sum256(jsonBytes)
	return hex.EncodeToString(hash[:])
}

// normalizeQuery normalizes a SQL query for consistent caching.
func normalizeQuery(query string) string {
	// Simple normalization: trim whitespace and convert to lowercase
	// In a production system, you might want more sophisticated normalization
	normalized := strings.TrimSpace(strings.ToLower(query))

	// Remove extra whitespace
	normalized = strings.Join(strings.Fields(normalized), " ")

	return normalized
}

// moveToFront moves an entry to the front of the LRU list.
func (qc *QueryCache) moveToFront(entry *CacheEntry) {
	// Remove from current position
	qc.removeFromList(entry)
	// Add to front
	qc.addToFront(entry)
}

// addToFront adds an entry to the front of the LRU list.
func (qc *QueryCache) addToFront(entry *CacheEntry) {
	if qc.lruList.head == nil {
		// First entry
		qc.lruList.head = entry
		qc.lruList.tail = entry
	} else {
		// Add to front
		entry.next = qc.lruList.head
		qc.lruList.head.prev = entry
		qc.lruList.head = entry
	}
	qc.lruList.size++
}

// removeFromList removes an entry from the LRU list.
func (qc *QueryCache) removeFromList(entry *CacheEntry) {
	if entry.prev != nil {
		entry.prev.next = entry.next
	} else {
		// This was the head
		qc.lruList.head = entry.next
	}

	if entry.next != nil {
		entry.next.prev = entry.prev
	} else {
		// This was the tail
		qc.lruList.tail = entry.prev
	}

	// Clear the entry's links
	entry.prev = nil
	entry.next = nil
	qc.lruList.size--
}

// removeEntry removes an entry from both the cache and LRU list.
func (qc *QueryCache) removeEntry(entry *CacheEntry) {
	delete(qc.cache, entry.Key)
	qc.removeFromList(entry)
}

// evictLRU removes the least recently used entry.
func (qc *QueryCache) evictLRU() {
	if qc.lruList.tail == nil {
		return
	}

	// Remove the tail (least recently used)
	lru := qc.lruList.tail
	qc.removeEntry(lru)
	qc.recordEviction()

	if qc.config.LogLevel {
		log.Printf("[server] Evicted LRU cache entry: %s", lru.Key[:16]+"...")
	}
}

// cleanupExpired removes expired entries from the cache.
func (qc *QueryCache) cleanupExpired() {
	qc.mutex.Lock()
	defer qc.mutex.Unlock()

	now := time.Now()
	var expiredKeys []string

	// Find expired entries
	for key, entry := range qc.cache {
		if now.Sub(entry.CreatedAt) > qc.config.TTL {
			expiredKeys = append(expiredKeys, key)
		}
	}

	// Remove expired entries
	for _, key := range expiredKeys {
		if entry, exists := qc.cache[key]; exists {
			qc.removeEntry(entry)
			qc.recordExpiration()
		}
	}

	qc.lastCleanup = now

	if len(expiredKeys) > 0 && qc.config.LogLevel {
		log.Printf("[server] Cleaned up %d expired cache entries", len(expiredKeys))
	}
}

// Record cache statistics
func (qc *QueryCache) recordHit() {
	qc.stats.mutex.Lock()
	qc.stats.Hits++
	qc.stats.mutex.Unlock()
}

func (qc *QueryCache) recordMiss() {
	qc.stats.mutex.Lock()
	qc.stats.Misses++
	qc.stats.mutex.Unlock()
}

func (qc *QueryCache) recordEviction() {
	qc.stats.mutex.Lock()
	qc.stats.Evictions++
	qc.stats.mutex.Unlock()
}

func (qc *QueryCache) recordExpiration() {
	qc.stats.mutex.Lock()
	qc.stats.Expirations++
	qc.stats.mutex.Unlock()
}

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/valyala/fasthttp"
)

const (
	// Maximum allowed length for keys and values.
	MaxStringLength = 256

	// Default configuration values.
	DefaultMaxEntries         = 1000000      
	DefaultMemoryThreshold    = 70          
	DefaultCacheCheckInterval = 5 * time.Second

	
	NumShards = 256
)


type CacheResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Key     string `json:"key,omitempty"`
	Value   string `json:"value,omitempty"`
}

func successResponse(key, value string) CacheResponse {
	return CacheResponse{Status: "OK", Key: key, Value: value}
}

func errorResponse(msg string) CacheResponse {
	return CacheResponse{Status: "ERROR", Message: msg}
}


type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}


type CacheEntry struct {
	value     string
	frequency int64 // Updated via atomic operations.
}


type shard struct {
	lock sync.RWMutex
	m    map[string]*CacheEntry
}


type ShardedCache struct {
	shards     []*shard
	numShards  uint64
	maxEntries int

	// Global count of entries (avoiding costly full scans).
	count int64
}


func NewShardedCache(numShards, maxEntries int) *ShardedCache {
	s := make([]*shard, numShards)
	for i := 0; i < numShards; i++ {
		s[i] = &shard{
			m: make(map[string]*CacheEntry),
		}
	}
	return &ShardedCache{
		shards:     s,
		numShards:  uint64(numShards),
		maxEntries: maxEntries,
		count:      0,
	}
}

func (sc *ShardedCache) getShard(key string) *shard {
	h := xxhash.Sum64String(key)
	return sc.shards[h&(sc.numShards-1)]
}


func (sc *ShardedCache) Size() int {
	return int(atomic.LoadInt64(&sc.count))
}


func (sc *ShardedCache) Put(key, value string) bool {
	if key == "" || value == "" || len(key) > MaxStringLength || len(value) > MaxStringLength {
		return false
	}
	// If we've reached maximum capacity, evict roughly 10% of entries.
	if atomic.LoadInt64(&sc.count) >= int64(sc.maxEntries) {
		evictCount := sc.maxEntries / 10
		sc.EvictGlobal(evictCount)
	}
	s := sc.getShard(key)
	s.lock.Lock()
	defer s.lock.Unlock()

	if entry, exists := s.m[key]; exists {
		entry.value = value
		atomic.StoreInt64(&entry.frequency, 1)
	} else {
		s.m[key] = &CacheEntry{
			value:     value,
			frequency: 1,
		}
		atomic.AddInt64(&sc.count, 1)
	}
	return true
}


func (sc *ShardedCache) Get(key string) (string, bool) {
	if key == "" || len(key) > MaxStringLength {
		return "", false
	}
	s := sc.getShard(key)
	s.lock.RLock()
	entry, ok := s.m[key]
	s.lock.RUnlock()
	if !ok {
		return "", false
	}
	atomic.AddInt64(&entry.frequency, 1)
	return entry.value, true
}


func (sc *ShardedCache) EvictGlobal(count int) {
	type candidate struct {
		shardIndex int
		key        string
		frequency  int64
	}

	candidates := make([]candidate, 0, count*2)
	for i, s := range sc.shards {
		s.lock.RLock()
		for k, entry := range s.m {
			candidates = append(candidates, candidate{
				shardIndex: i,
				key:        k,
				frequency:  atomic.LoadInt64(&entry.frequency),
			})
		}
		s.lock.RUnlock()
	}
	if len(candidates) == 0 {
		return
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].frequency < candidates[j].frequency
	})
	if count > len(candidates) {
		count = len(candidates)
	}
	for i := 0; i < count; i++ {
		c := candidates[i]
		s := sc.shards[c.shardIndex]
		s.lock.Lock()
		if _, exists := s.m[c.key]; exists {
			delete(s.m, c.key)
			atomic.AddInt64(&sc.count, -1)
		}
		s.lock.Unlock()
	}
}

func memoryMonitor(sc *ShardedCache, memoryThreshold int, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		usagePercent := 0
		if memStats.HeapSys > 0 {
			usagePercent = int((memStats.HeapAlloc * 100) / memStats.HeapSys)
		}
		if usagePercent > memoryThreshold {
			currentSize := sc.Size()
			evictCount := currentSize / 4 // Evict 25% of entries.
			sc.EvictGlobal(evictCount)
			log.Printf("High memory usage: %d%%, evicted %d entries", usagePercent, evictCount)
		}
	}
}

// requestHandler processes HTTP requests for /put and /get endpoints.
func requestHandler(ctx *fasthttp.RequestCtx, cache *ShardedCache) {
	switch string(ctx.Path()) {
	case "/put":
		if string(ctx.Method()) != "POST" {
			ctx.Error("Method Not Allowed", fasthttp.StatusMethodNotAllowed)
			return
		}
		var req PutRequest
		if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			resp, _ := json.Marshal(errorResponse("Invalid request body"))
			ctx.SetBody(resp)
			return
		}
		if req.Key == "" || req.Value == "" {
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			resp, _ := json.Marshal(errorResponse("Key and value cannot be null"))
			ctx.SetBody(resp)
			return
		}
		if len(req.Key) > MaxStringLength || len(req.Value) > MaxStringLength {
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			resp, _ := json.Marshal(errorResponse("Key or value exceeds maximum length"))
			ctx.SetBody(resp)
			return
		}
		if cache.Put(req.Key, req.Value) {
			resp, _ := json.Marshal(successResponse(req.Key, req.Value))
			ctx.SetBody(resp)
		} else {
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			resp, _ := json.Marshal(errorResponse("Failed to insert key"))
			ctx.SetBody(resp)
		}
	case "/get":
		if string(ctx.Method()) != "GET" {
			ctx.Error("Method Not Allowed", fasthttp.StatusMethodNotAllowed)
			return
		}
		key := string(ctx.QueryArgs().Peek("key"))
		if key == "" {
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			resp, _ := json.Marshal(errorResponse("Missing key parameter"))
			ctx.SetBody(resp)
			return
		}
		if value, ok := cache.Get(key); ok {
			resp, _ := json.Marshal(successResponse(key, value))
			ctx.SetBody(resp)
		} else {
			resp, _ := json.Marshal(errorResponse("Key not found"))
			ctx.SetBody(resp)
		}
	default:
		ctx.Error("Endpoint not found", fasthttp.StatusNotFound)
	}
}


func main() {
	
	runtime.GOMAXPROCS(runtime.NumCPU())

	
	port := os.Getenv("PORT")
	if port == "" {
		port = "7171"
	}

	
	cache := NewShardedCache(NumShards, DefaultMaxEntries)

	
	go memoryMonitor(cache, DefaultMemoryThreshold, DefaultCacheCheckInterval)


	server := &fasthttp.Server{
		Handler: func(ctx *fasthttp.RequestCtx) {
			requestHandler(ctx, cache)
		},
		Name:                 "OptimizedCacheServer",
		ReadBufferSize:       64 * 1024,
		WriteBufferSize:      64 * 1024,
		MaxKeepaliveDuration: 300 * time.Second,
	}

	addr := ":" + port
	fmt.Printf("Starting cache server on %s\n", addr)
	if err := server.ListenAndServe(addr); err != nil {
		log.Fatalf("ListenAndServe error: %s", err)
	}
}

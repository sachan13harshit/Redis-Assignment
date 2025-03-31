package main

import (
	"fmt"
	"log"
	"net/http/pprof" // HTTP profiling handlers
	"os"
	"runtime"
	rpprof "runtime/pprof" // Runtime profiling tools with renamed import
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	jsoniter "github.com/json-iterator/go"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

// Use json-iterator with fastest config for maximized JSON performance
var json = jsoniter.ConfigFastest

// ShardedCache with optimized sharding
type ShardedCache struct {
	shards     []*ristretto.Cache
	shardCount uint32
	shardMask  uint32 // For faster shard selection using bitwise AND
}

// Pre-allocate common response strings
var (
	keyNotFoundBytes   = []byte(`{"status":"ERROR","message":"Key not found"}`)
	invalidRequestBytes = []byte(`{"status":"ERROR","message":"Invalid request body or missing key/value"}`)
	missingKeyBytes    = []byte(`{"status":"ERROR","message":"Missing key parameter"}`)
	successPutBytes    = []byte(`{"status":"OK","message":"Key submitted for insertion/update"}`)
)

// NewShardedCache creates a sharded cache with power-of-2 shards for faster selection
func NewShardedCache(shardBits int, capacityPerShard int64) (*ShardedCache, error) {
	// Use power-of-2 shards for faster selection with bitwise AND
	shardCount := 1 << shardBits // 2^shardBits
	shardMask := uint32(shardCount - 1)
	
	if shardCount <= 0 {
		return nil, fmt.Errorf("shardCount must be positive")
	}
	if capacityPerShard <= 0 {
		return nil, fmt.Errorf("capacityPerShard must be positive")
	}

	shards := make([]*ristretto.Cache, shardCount)
	
	// Configure Ristretto for higher performance
	for i := 0; i < shardCount; i++ {
		cache, err := ristretto.NewCache(&ristretto.Config{
			NumCounters: capacityPerShard * 20,  // Increase counters for better accuracy
			MaxCost:     capacityPerShard,
			BufferItems: 1024,                   // Larger buffer for better throughput
			Metrics:     false,                  // Disable metrics for performance
			KeyToHash:   nil,                    // Use default (FNV-1a) but avoid key conversion
			Cost:        nil,                    // Use default cost function
			IgnoreInternalCost: true,            // Ignore internal cost calculations
		})
		if err != nil {
			// Cleanup on error
			for j := 0; j < i; j++ {
				shards[j].Close()
			}
			return nil, fmt.Errorf("failed to create cache shard %d: %w", i, err)
		}
		shards[i] = cache
	}
	
	return &ShardedCache{
		shards:     shards,
		shardCount: uint32(shardCount),
		shardMask:  shardMask,
	}, nil
}

// Optimized hash function for byte slices
func fnvHashBytes(key []byte) uint32 {
	// FNV-1a 32-bit hash
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h
}

// getShard returns the cache shard for a key using bitwise AND for faster selection
func (sc *ShardedCache) getShard(key []byte) *ristretto.Cache {
	// Use mask for faster modulo operation (works because shardCount is power of 2)
	return sc.shards[fnvHashBytes(key) & sc.shardMask]
}

// Put stores a key-value pair in the cache
func (sc *ShardedCache) Put(key []byte, value []byte) bool {
	shard := sc.getShard(key)
	
	// Make a copy of the value to prevent corruption from reuse
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	
	// Convert key to string for Ristretto (required for its key type)
	return shard.Set(string(key), valueCopy, 1)
}

// Get retrieves a value from the cache
func (sc *ShardedCache) Get(key []byte) ([]byte, bool) {
	shard := sc.getShard(key)
	
	// Get value from cache
	value, found := shard.Get(string(key))
	if !found {
		return nil, false
	}
	
	// Type assertion with direct return
	if byteValue, ok := value.([]byte); ok {
		return byteValue, true
	}
	
	return nil, false
}

// Close releases all cache resources
func (sc *ShardedCache) Close() {
	for _, shard := range sc.shards {
		shard.Close()
	}
}

// --- Request/Response Management with Advanced Pooling ---

// PutRequest represents the JSON structure for PUT requests
type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Pool for PutRequest objects
var putRequestPool = sync.Pool{
	New: func() interface{} {
		return &PutRequest{}
	},
}

// Pool for byte slices to reduce GC pressure
var bytesPool = sync.Pool{
	New: func() interface{} {
		// Pre-allocate with a reasonable size (adjust based on average size)
		return make([]byte, 0, 1024)
	},
}

// getByteSlice gets a byte slice from the pool
func getByteSlice() []byte {
	return bytesPool.Get().([]byte)[:0] // Reset length but keep capacity
}

// putByteSlice returns a byte slice to the pool
func putByteSlice(b []byte) {
	// Only return to pool if under reasonable size
	if cap(b) <= 8192 {
		bytesPool.Put(b)
	}
}

// SuccessResponse represents a successful operation
type SuccessResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Key     string `json:"key,omitempty"`
	Value   string `json:"value,omitempty"`
}

// Pool for success responses
var successResponsePool = sync.Pool{
	New: func() interface{} {
		return &SuccessResponse{Status: "OK"}
	},
}

// --- Global Cache ---
var cache *ShardedCache

// --- Optimized HTTP Handlers ---

// putHandler processes PUT requests
func putHandler(ctx *fasthttp.RequestCtx) {
	// Get request object from pool
	req := putRequestPool.Get().(*PutRequest)
	defer putRequestPool.Put(req)
	req.Key = ""   // Reset fields
	req.Value = ""

	// Fast path check for empty body
	if len(ctx.PostBody()) == 0 {
		ctx.SetContentType("application/json")
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.Write(invalidRequestBytes)
		return
	}

	// Decode request body
	err := json.Unmarshal(ctx.PostBody(), req)
	if err != nil || req.Key == "" || req.Value == "" {
		ctx.SetContentType("application/json")
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.Write(invalidRequestBytes)
		return
	}

	// Store in cache
	cache.Put([]byte(req.Key), []byte(req.Value))

	// Use pre-allocated success response
	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.Write(successPutBytes)
}

// getHandler processes GET requests
func getHandler(ctx *fasthttp.RequestCtx) {
	// Get key from query args
	keyBytes := ctx.QueryArgs().Peek("key")
	if len(keyBytes) == 0 {
		ctx.SetContentType("application/json")
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.Write(missingKeyBytes)
		return
	}

	// Get from cache
	valueBytes, found := cache.Get(keyBytes)
	if !found {
		ctx.SetContentType("application/json")
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		ctx.Write(keyNotFoundBytes)
		return
	}

	// Prepare success response
	resp := successResponsePool.Get().(*SuccessResponse)
	defer successResponsePool.Put(resp)
	resp.Key = string(keyBytes)
	resp.Value = string(valueBytes)
	resp.Message = ""

	// Get byte buffer from pool
	buf := getByteSlice()
	defer putByteSlice(buf)

	// Encode response directly to buffer
	encodedResponse, err := json.Marshal(resp)
	if err != nil {
		ctx.Error("Internal Server Error", fasthttp.StatusInternalServerError)
		return
	}

	// Write response
	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.Write(encodedResponse)
}

// healthHandler provides a simple health check endpoint
func healthHandler(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("text/plain")
	ctx.WriteString("OK")
}

func main() {
	// Enable profiling if needed
	if os.Getenv("ENABLE_PROFILING") == "1" {
		f, err := os.Create("cpu.prof")
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := rpprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer rpprof.StopCPUProfile()
		
		// Schedule heap profile
		go func() {
			time.Sleep(30 * time.Second)
			hf, err := os.Create("heap.prof")
			if err != nil {
				log.Fatal("could not create heap profile: ", err)
			}
			defer hf.Close()
			if err := rpprof.WriteHeapProfile(hf); err != nil {
				log.Fatal("could not write heap profile: ", err)
			}
		}()
	}

	// Use all available CPU cores
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	// Configure GC for higher throughput (trade memory for speed)
	// Increase GC threshold to reduce GC frequency
	// GOGC=200 means GC triggers when heap size is 200% of live data
	if os.Getenv("GOGC") == "" {
		os.Setenv("GOGC", "200")
	}

	// Pre-allocate memory to reduce GC impact during high load
	// Adjust based on expected memory usage
	runtime.MemProfileRate = 0 // Disable memory profiling in production

	// --- Cache Initialization ---
	// Use power of 2 for shard count (for bitwise operations)
	shardBits := 4 // 2^4 = 16 shards
	if numCPU > 8 {
		shardBits = 5 // 2^5 = 32 shards for high-core machines
	}
	
	shardCount := 1 << shardBits
	totalCapacity := int64(2 * 1000 * 1000) // Target 2M items (increased from 1M)
	capacityPerShard := totalCapacity / int64(shardCount)

	var err error
	cache, err = NewShardedCache(shardBits, capacityPerShard)
	if err != nil {
		log.Fatalf("Failed to initialize sharded cache: %v", err)
	}
	defer cache.Close()

	log.Printf("Initialized cache with %d shards, capacity/shard: %d (Total ~%d)",
		shardCount, capacityPerShard, totalCapacity)

	// --- HTTP Server Setup ---
	router := func(ctx *fasthttp.RequestCtx) {
		path := string(ctx.Path())
		method := string(ctx.Method())
		
		switch {
		case path == "/put" && method == "POST":
			putHandler(ctx)
		case path == "/get" && method == "GET":
			getHandler(ctx)
		case path == "/health":
			healthHandler(ctx)
		case path == "/debug/pprof" || path == "/debug/pprof/":
			fasthttpadaptor.NewFastHTTPHandlerFunc(pprof.Index)(ctx)
		case path == "/debug/pprof/cmdline":
			fasthttpadaptor.NewFastHTTPHandlerFunc(pprof.Cmdline)(ctx)
		case path == "/debug/pprof/profile":
			fasthttpadaptor.NewFastHTTPHandlerFunc(pprof.Profile)(ctx)
		case path == "/debug/pprof/symbol":
			fasthttpadaptor.NewFastHTTPHandlerFunc(pprof.Symbol)(ctx)
		case path == "/debug/pprof/trace":
			fasthttpadaptor.NewFastHTTPHandlerFunc(pprof.Trace)(ctx)
		default:
			ctx.Error("Not Found", fasthttp.StatusNotFound)
		}
	}

	// Configure the fasthttp server for maximum performance
	server := &fasthttp.Server{
		Handler:                       router,
		Name:                          "GoKVStore",
		ReadBufferSize:                16 * 1024,      // Increased buffer size
		WriteBufferSize:               16 * 1024,      // Increased buffer size
		ReadTimeout:                   5 * time.Second,
		WriteTimeout:                  5 * time.Second,
		IdleTimeout:                   60 * time.Second, // Keep connections alive longer
		MaxConnsPerIP:                 0,              // No limit
		MaxRequestsPerConn:            0,              // Unlimited
		Concurrency:                   512 * 1024,     // Higher concurrency
		ReduceMemoryUsage:             true,           // Reduce memory usage
		GetOnly:                       false,          // Support all methods
		DisableHeaderNamesNormalizing: true,           // Skip normalizing headers
		NoDefaultServerHeader:         true,           // Skip default server header
		NoDefaultDate:                 true,           // Skip default date
		NoDefaultContentType:          false,          // Keep content type
		KeepHijackedConns:             false,          // Don't keep hijacked connections
	}

	// Listen address
	addr := ":7171"
	if envPort := os.Getenv("PORT"); envPort != "" {
		addr = ":" + envPort
	}

	log.Printf("Starting fasthttp server on %s", addr)
	if err := server.ListenAndServe(addr); err != nil {
		log.Fatalf("Error in ListenAndServe: %s", err)
	}
}
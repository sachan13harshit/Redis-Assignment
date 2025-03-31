
# Optimized Redis Cache Server

An in-memory HTTP cache server built in Go, designed for high-throughput performance with dynamic eviction strategies. This cache server is sharded to minimize lock contention and optimized to monitor memory usage in real-time.

## Features

- **High Throughput:** Uses 256 shards to reduce lock contention, allowing for efficient concurrent access.
- **LFU-Based Eviction:** Evicts about 10% of entries when the cache reaches maximum capacity, targeting the least frequently accessed entries.
- **Memory Monitoring:** A background goroutine monitors heap usage and triggers eviction if memory usage exceeds a defined threshold (default 70%).
- **Efficient Concurrency:** Fine-grained locks are used per shard, along with atomic operations for frequency counters, ensuring low-overhead concurrent operations.
- **High-Performance HTTP Server:** Built with the `fasthttp` library, ensuring fast routing on both `/put` and `/get` endpoints.
- **Docker Ready:** Easily install and run the cache server using the provided Dockerfile.

## Design Choices and Optimizations

1. **Sharded Cache Structure:**  
   The cache is divided into 256 shards, each with its own RW-mutex. This minimizes lock contention and enables multiple goroutines to operate on different shards concurrently.

2. **Atomic Frequency Counters:**  
   Each cache entry maintains an atomic counter that tracks accesses. This enables an approximate least-frequently-used (LFU) eviction strategy with minimal overhead.

3. **Adaptive Eviction Policies:**  
   - **Capacity-Based Eviction:** When the cache reaches its maximum size, around 10% of the least-accessed entries are evicted.
   - **Memory-Based Eviction:** A background goroutine periodically checks heap usage and evicts about 25% of entries if the memory usage exceeds 70%.

4. **Optimized HTTP Handling:**  
   Using the `fasthttp` package ensures that the server can handle high traffic and respond quickly to incoming HTTP requests, making the server suitable for production environments.

## Prerequisites

- Docker installed on your machine.
- (Optional) Go 1.13+ if you wish to build the project locally.

## Installation Using Docker

The Dockerfile provided builds the Go binary from source and packages it in a minimal Alpine Linux container.

### Steps to run the server:

1. Clone the repository:
   ```bash
   git clone https://github.com/sachan13harshit/Redis-Assignment.git
   cd Redis-Assignment
   ```

2. Build the Docker image:
   ```bash
   docker build -t redis_cache_server .
   ```

3. Run the Docker container:
   ```bash
   docker run -p 7171:7171 redis_cache_server
   ```

4. The cache server will now be running on `http://localhost:7171`.

## API Endpoints

- **PUT /put:**  
   Adds an item to the cache.
   - Request body: `{"key": "string", "value": "string"}`
   - Example:
     ```json
     { "key": "user123", "value": "John Doe" }
     ```

- **GET /get:**  
   Retrieves an item from the cache.
   - Request body: `{"key": "string"}`
   - Example:
     ```json
     { "key": "user123" }
     ```


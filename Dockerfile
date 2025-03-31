# Use a specific Go version
FROM golang:1.20-alpine

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum first (for better layer caching)
COPY go.mod go.sum* ./

# Download dependencies (if you have a go.mod)
RUN go mod download

# Copy the source code
COPY *.go ./

# Build with verbose output to see more details about any failures
RUN go build -v -o redis_cache_server

# Expose the port
EXPOSE 7171

# Run the application
CMD ["./redis_cache_server"]

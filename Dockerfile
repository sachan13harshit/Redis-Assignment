# Use official Go image as base
FROM golang:alpine

# Set working directory inside container
WORKDIR /app

# Copy go mod files first for layer caching
COPY go.mod ./

# Download dependencies - this creates a separate layer that can be cached
RUN go mod download

# Copy source code into container
COPY . .

# Build Go application inside container
RUN go build -o lru_cache main.go

# Expose port for HTTP requests
EXPOSE 7171

# Command to run application when container starts
CMD ["./lru_cache"]
# Universal Dockerfile for burrowctl server
# Builds the main server from examples/server/

# Multi-stage build for optimal image size
FROM golang:1.22-alpine AS builder

# Install git for go mod download
RUN apk add --no-cache git

# Set working directory
WORKDIR /build

# Copy all project files (context should be from project root)
COPY . .

# Build the server example
WORKDIR /build/examples/server
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o server main.go

# Production stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

# Create non-root user
RUN addgroup -g 1001 -S burrowctl && \
    adduser -S -D -H -u 1001 -h /app -s /sbin/nologin -G burrowctl burrowctl

WORKDIR /app

# Copy binary from builder
COPY --from=builder --chown=burrowctl:burrowctl /build/examples/server/server .

# Switch to non-root user
USER burrowctl

# Default command
CMD ["./server"]
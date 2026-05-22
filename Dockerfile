# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY agent_module/go.mod agent_module/go.sum ./agent_module/
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY agent_module ./agent_module
COPY configs ./configs

# Build the agent
RUN cd agent_module && CGO_ENABLED=0 GOOS=linux go build -o /agent .

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /agent ./agent

# Copy configs
COPY --from=builder /app/configs ./configs

# Set environment variables
ENV CONFIG=configs/income-analyzer-config.md
ENV NATS_URL=nats://localhost:4222
ENV REDIS_URL=localhost:6379

# Run the agent
CMD ["./agent", "-config", "${CONFIG}", "-nats", "${NATS_URL}", "-redis", "${REDIS_URL}"]

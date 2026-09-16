# Build stage
FROM golang:1.27-alpine AS builder

WORKDIR /app

# Install git and ca-certificates if needed for module downloads
RUN apk add --no-cache ca-certificates

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/simulator ./services/simulator

# Runtime stage
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S appgroup && adduser -S appuser -G appgroup

USER appuser

COPY --from=builder /bin/simulator /usr/local/bin/simulator

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/simulator"]

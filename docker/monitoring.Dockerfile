# Build stage
FROM golang:1.27-alpine AS builder

WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/monitoring ./services/monitoring

# Runtime stage
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S appgroup && adduser -S appuser -G appgroup

USER appuser

COPY --from=builder /bin/monitoring /usr/local/bin/monitoring

EXPOSE 8081

ENTRYPOINT ["/usr/local/bin/monitoring"]

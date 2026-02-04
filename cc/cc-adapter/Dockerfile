FROM golang:1.23-alpine AS builder

WORKDIR /build

COPY go.mod go.sum* ./
RUN go mod download 2>/dev/null || true

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o payload-writer .

# Runtime
FROM alpine:latest

RUN apk --no-cache add ca-certificates jq

COPY --from=builder /build/payload-writer .

ENTRYPOINT ["./payload-writer"]

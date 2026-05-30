# ── Build stage ────────────────────────────────────────────────────────────────
FROM golang:1.24-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /bin/oncloudkv ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /bin/oncloudkv-cli ./cmd/cli

# ── Runtime stage ──────────────────────────────────────────────────────────────
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /bin/oncloudkv     /oncloudkv
COPY --from=builder /bin/oncloudkv-cli /oncloudkv-cli

EXPOSE 7000 7001 7002 7003 7004

ENTRYPOINT ["/oncloudkv"]

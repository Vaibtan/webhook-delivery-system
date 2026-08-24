# Multi-stage build for the Go webhook delivery system.
# Stage 1 compiles a static binary; stage 2 is a minimal distroless image.

FROM golang:1.27-alpine AS builder
WORKDIR /src

# Cache module downloads.
COPY go.mod go.sum ./
RUN go mod download

# Build (CGO off → fully static; trimpath + -s -w for a small reproducible binary).
COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

# Stage 2: distroless static (no shell, non-root, tiny attack surface).
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /out/server /server
EXPOSE 8080
USER nonroot:nonroot
# Migrations run embedded on startup; DATABASE_URL/REDIS_URL/ADMIN_API_KEY come from env.
ENTRYPOINT ["/server"]

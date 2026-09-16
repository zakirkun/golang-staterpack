# ---- build stage -------------------------------------------------------------
FROM golang:1.25-alpine AS builder

WORKDIR /src

# Cache dependencies separately from source so code edits don't refetch modules.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static, stripped binary. CGO is off so the result runs on a scratch base.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION:-dev}" \
    -o /out/api ./cmd/api

# ---- runtime stage -----------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/api /api

# Distroless nonroot already runs as uid 65532; stated explicitly for clarity.
USER nonroot:nonroot

EXPOSE 8080

ENTRYPOINT ["/api"]

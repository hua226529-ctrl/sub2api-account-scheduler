# syntax=docker/dockerfile:1

ARG NODE_IMAGE=node:24-alpine
ARG GO_IMAGE=golang:1.26.3-alpine

FROM ${NODE_IMAGE} AS frontend-builder
WORKDIR /src
COPY frontend/package.json frontend/package-lock.json ./frontend/
RUN cd frontend && npm ci --no-audit --no-fund
COPY frontend ./frontend
RUN mkdir -p internal/webui/dist && cd frontend && npm run build

FROM ${GO_IMAGE} AS backend-builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY cmd ./cmd
COPY internal ./internal
COPY docs ./docs
COPY --from=frontend-builder /src/internal/webui/dist ./internal/webui/dist
RUN test -z "$(gofmt -l cmd internal)" \
    && go test -buildvcs=false -count=1 ./... \
    && go vet ./...
RUN CGO_ENABLED=0 GOOS=linux go build -buildvcs=false -trimpath -ldflags="-s -w" -o /out/scheduler ./cmd/scheduler

FROM alpine:3.24
RUN apk add --no-cache ca-certificates tzdata wget && addgroup -g 10001 scheduler && adduser -D -u 10001 -G scheduler scheduler
WORKDIR /app
COPY --from=backend-builder /out/scheduler /app/scheduler
RUN mkdir -p /data && chown -R scheduler:scheduler /data /app
USER scheduler
EXPOSE 8323
VOLUME ["/data"]
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD wget -q -T 3 -O /dev/null http://127.0.0.1:8323/healthz || exit 1
STOPSIGNAL SIGTERM
ENTRYPOINT ["/app/scheduler"]

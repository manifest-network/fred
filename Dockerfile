# ---- builder stage ----
FROM golang:1.25.9-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -ldflags "-s -w -X main.version=${VERSION}" -o /out/providerd ./cmd/providerd \
 && CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -ldflags "-s -w -X main.version=${VERSION}" -o /out/docker-backend ./cmd/docker-backend

# Pre-create /data owned by nonroot (65532) for the docker-backend stage.
RUN mkdir -p /out/data && chown 65532:65532 /out/data

# ---- providerd runtime ----
# Config must be mounted at runtime:
#   docker run -v ./config.yaml:/config.yaml fred-providerd --config /config.yaml
FROM gcr.io/distroless/static-debian12 AS providerd

COPY --from=builder /out/providerd /providerd

USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/providerd"]

# ---- docker-backend runtime ----
# Config must be mounted at runtime. The Docker socket must also be accessible.
# Persist /data to retain callbacks.db, diagnostics.db, and releases.db:
#   docker run -v db-data:/data \
#     -v ./docker-backend.yaml:/data/docker-backend.yaml \
#     -v /var/run/docker.sock:/var/run/docker.sock \
#     --group-add <docker-gid> \
#     fred-docker-backend
FROM gcr.io/distroless/static-debian12 AS docker-backend

COPY --from=builder /out/docker-backend /docker-backend

# /data holds persistent state (callbacks.db, diagnostics.db, releases.db).
# Declare as a volume so data survives container restarts.
COPY --from=builder --chown=65532:65532 /out/data /data
WORKDIR /data
VOLUME /data
USER nonroot:nonroot
EXPOSE 9001
ENTRYPOINT ["/docker-backend"]

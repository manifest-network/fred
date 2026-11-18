# ---- builder stage ----
FROM golang:1.26.11-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -ldflags "-s -w -X main.version=${VERSION}" -o /out/providerd ./cmd/providerd \
 && CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -ldflags "-s -w -X main.version=${VERSION}" -o /out/placement-preflight ./cmd/placement-preflight \
 && CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -ldflags "-s -w -X main.version=${VERSION}" -o /out/placement-repair ./cmd/placement-repair \
 && CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -ldflags "-s -w -X main.version=${VERSION}" -o /out/docker-backend ./cmd/docker-backend

# Pre-create /data owned by nonroot (65532) for the docker-backend stage.
RUN mkdir -p /out/data && chown 65532:65532 /out/data

# ---- providerd runtime ----
# Config must be mounted at runtime:
#   docker run -v ./config.yaml:/config.yaml fred-providerd --config /config.yaml
FROM gcr.io/distroless/static-debian12 AS providerd

COPY --from=builder /out/providerd /providerd
COPY --from=builder /out/placement-preflight /placement-preflight
COPY --from=builder /out/placement-repair /placement-repair

USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/providerd"]

# ---- docker-backend runtime ----
# Stateless development only: this image has no filesystem quota tools or
# supported host-mount/capability contract for managed tenant storage. Run the
# released native binary under systemd with XFS for stateful production.
# Configure only disk_mb: 0 SKUs and omit volume_data_path, volume_mount_path,
# and volume_filesystem; /data below persists backend state, not tenant volumes.
# This stage is kept LAST so `docker build .` without an explicit --target
# produces the docker-backend image. Callers that want a specific stage should
# pass --target providerd or --target docker-backend explicitly.
#
# Config must be mounted at runtime. The Docker socket must also be accessible.
# Persist /data to retain callbacks.db, diagnostics.db, releases.db, and the
# always-required retention.db. A genuinely new empty volume must first run this
# same image once with `-initialize-storage-identity new`; normal startup is
# verification-only:
#   docker run -v db-data:/data \
#     -v ./docker-backend.yaml:/data/docker-backend.yaml \
#     -v /var/run/docker.sock:/var/run/docker.sock \
#     --group-add <docker-gid> \
#     fred-docker-backend
FROM gcr.io/distroless/static-debian12 AS docker-backend

COPY --from=builder /out/docker-backend /docker-backend

# /data holds persistent state (callbacks.db, diagnostics.db, releases.db,
# retention.db, and the storage-identity marker pair).
# Declare as a volume so data survives container restarts.
COPY --from=builder --chown=65532:65532 /out/data /data
WORKDIR /data
VOLUME /data
USER nonroot:nonroot
EXPOSE 9001
ENTRYPOINT ["/docker-backend"]

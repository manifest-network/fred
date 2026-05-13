# ---- builder stage ----
FROM golang:1.25.9-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -ldflags "-s -w -X main.version=${VERSION}" -o /out/providerd ./cmd/providerd \
 && CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -ldflags "-s -w -X main.version=${VERSION}" -o /out/docker-backend ./cmd/docker-backend \
 && CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -ldflags "-s -w -X main.version=${VERSION}" -o /out/k3s-backend ./cmd/k3s-backend

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

# ---- k3s-backend runtime ----
# Config must be mounted at runtime. The kubeconfig must also be readable
# at the path the config points at — config.k3s.yaml.example defaults
# kubeconfig_path to /etc/rancher/k3s/k3s.yaml, so either mount the
# kubeconfig there OR override kubeconfig_path in the YAML to wherever
# you mount it.
#
# Permissions note: this image runs as nonroot (UID 65532). K3s installs
# its kubeconfig as 0600 root:root, so the mounted file MUST be readable
# by UID 65532. Two common approaches:
#   1. Copy + chmod a working copy:
#        sudo cp /etc/rancher/k3s/k3s.yaml /tmp/k3s.yaml
#        sudo chown 65532:65532 /tmp/k3s.yaml
#        sudo chmod 0600 /tmp/k3s.yaml
#        docker run ... -v /tmp/k3s.yaml:/etc/rancher/k3s/k3s.yaml:ro ...
#   2. Use Docker's --user 0:0 flag to run as root inside the container
#      (DEFEATS the nonroot hardening — not recommended for production).
#
# Networking note: K3s writes its default kubeconfig with
# `server: https://127.0.0.1:6443`, which inside a non-host-networked
# container resolves to the container itself — /health will fail with
# connection-refused even though the file is mounted. Two options:
#   a. Run with --network host (Linux only; simplest for local dev).
#   b. Rewrite the kubeconfig's `server:` URL to an address the
#      container can reach (e.g. the host's LAN IP, or
#      host.docker.internal on Docker Desktop). Do this on the
#      already-copied-and-chowned working copy from step 1 above.
#
# Persist /data to retain k3s-callbacks.db, k3s-diagnostics.db, and
# k3s-releases.db:
#   docker run --network host -v k3s-db-data:/data \
#     -v ./config.k3s.yaml:/data/config.k3s.yaml \
#     -v /tmp/k3s.yaml:/etc/rancher/k3s/k3s.yaml:ro \
#     fred-k3s-backend
FROM gcr.io/distroless/static-debian12 AS k3s-backend

COPY --from=builder /out/k3s-backend /k3s-backend

# /data holds persistent state (k3s-callbacks.db, k3s-diagnostics.db,
# k3s-releases.db). Declare as a volume so data survives container restarts.
COPY --from=builder --chown=65532:65532 /out/data /data
WORKDIR /data
VOLUME /data
USER nonroot:nonroot
EXPOSE 9002
ENTRYPOINT ["/k3s-backend"]

# ---- docker-backend runtime ----
# This stage is kept LAST so `docker build .` without an explicit --target
# preserves the pre-k3s default of producing the docker-backend image.
# Callers that want a specific stage should pass --target providerd,
# --target docker-backend, or --target k3s-backend explicitly.
#
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

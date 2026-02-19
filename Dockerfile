# ---- builder stage ----
FROM golang:1.25-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w -X main.version=${VERSION}" -o /out/providerd ./cmd/providerd \
 && CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w -X main.version=${VERSION}" -o /out/docker-backend ./cmd/docker-backend

# ---- providerd runtime ----
FROM gcr.io/distroless/static-debian12 AS providerd

COPY --from=builder /out/providerd /providerd

USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/providerd"]

# ---- docker-backend runtime ----
FROM gcr.io/distroless/static-debian12 AS docker-backend

COPY --from=builder /out/docker-backend /docker-backend

USER nonroot:nonroot
EXPOSE 9001
ENTRYPOINT ["/docker-backend"]

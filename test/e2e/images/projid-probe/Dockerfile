# Carries /probe, which asks the kernel to take a file or a directory out of
# its XFS project, and a /data volume to try it on. It sets no user and its volume belongs to
# root, so the workload runs as root: the owner of its files, with no
# capability, which is all the kernel asks for. TestBackend_TenantCannotChangeProjectID
# runs it (ENG-1118).
FROM golang:1.26.11@sha256:4165ee6bf3b7811fd7b40373290405c83d0167dcb87ee5f29c427cdcef45ff8f AS build
COPY probe.go /src/probe.go
RUN CGO_ENABLED=0 go build -trimpath -ldflags=-s -o /probe /src/probe.go

FROM busybox:1.37.0@sha256:a7dbc319b1438115a207e11930dc4bb3e643dcb3ec0bdd694bb36cd8fad289ec
COPY --from=build /probe /probe
VOLUME ["/data"]
CMD ["sleep", "3600"]

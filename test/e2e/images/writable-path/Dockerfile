# Runs as an account that owns one directory, /var/lib/app, and declares no
# volume. A backend backs such a directory with the lease's disk without it
# being a volume: TestBackend_LeaseWithOnlyAWritablePathIsNotRetained closes a
# lease that has nothing else on disk.
FROM busybox:1.37.0@sha256:a7dbc319b1438115a207e11930dc4bb3e643dcb3ec0bdd694bb36cd8fad289ec
RUN mkdir -p /var/lib/app && chown 1000:1000 /var/lib/app
USER 1000:1000
CMD ["sleep", "3600"]

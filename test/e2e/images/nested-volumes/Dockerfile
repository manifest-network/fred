# Declares two volumes, one of them below a path that the owner of a plain
# /data volume can turn into a link. TestBackend_LinkedVolumeDoesNotBlockOthers
# deploys it over such a volume (ENG-2181).
FROM busybox:1.37.0@sha256:a7dbc319b1438115a207e11930dc4bb3e643dcb3ec0bdd694bb36cd8fad289ec
VOLUME ["/data/b", "/data/a/c"]
CMD ["sleep", "3600"]

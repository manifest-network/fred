# The Docker Official Image, unchanged: it declares the same volume as the
# redis image, so a lease can be updated from one to the other and keep its
# data. The suite pulls it from this repository because a backend pulls
# without credentials, once for every test, and Docker Hub limits such pulls
# (see README.md).
FROM redis:7.4.12-alpine@sha256:7f7d30b7fdd09467bac58198a1f166dd69789d348d4fe859cccb20e0cf287fbe

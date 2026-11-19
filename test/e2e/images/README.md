# Images built for the black-box suite

Most tests run public images pinned by digest. A test that needs something no
public image has (a particular `VOLUME` list, say) gets an image built from a
directory here.

- One directory per image, holding a `Dockerfile` and whatever it copies. Base
  images are pinned by digest.
- `.github/workflows/e2e-images.yml` builds each directory and pushes it to
  `ghcr.io/manifest-network/fred-e2e`, tagged `<directory>-<hash of its files>`.
  A changed directory gets a new tag; an existing tag is never rebuilt, so the
  digest a test pins stays valid.
- A test names its image as `tag@digest`, in `test/e2e/docker.go`, like every
  other image of the suite. The workflow's summary prints that reference.

To change an image: edit its directory, push, wait for the workflow, then put
the reference it prints into `test/e2e/docker.go` and the e2e pre-pull list in
`integration.yml`.

The package is public, because the backend under test pulls without
credentials. Nothing here may contain a secret.

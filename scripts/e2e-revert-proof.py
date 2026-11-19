#!/usr/bin/env python3
"""One-off proof that each tenant regression test fails without its fix.

For each case this removes one guard from a copy of one source file, builds
docker-backend with that copy through `go build -overlay`, and runs the one
test against that binary. The test must fail. Run from the repository root
after `make build-e2e`. TEMPORARY: not part of the suite.
"""
import json
import os
import re
import subprocess
import sys

ROOT = os.getcwd()
OUT = os.path.join(ROOT, "build", "e2e", "revert")
SHARED_LEASE = '"00000000-0000-4000-8000-000000000000"'


def once(old, new):
    def edit(source):
        if source.count(old) != 1:
            raise SystemExit(f"expected exactly one match of {old!r}, found {source.count(old)}")
        return source.replace(old, new)
    return edit


def share_lease_network(source):
    """Every lease uses one network: the functions that take a lease UUID use a constant one."""
    pattern = re.compile(r"^(func [^\n]*\bleaseUUID\b[^\n]*\{\n)", re.M)
    edited, count = pattern.subn(lambda m: m.group(1) + f"\tleaseUUID = {SHARED_LEASE}\n", source)
    if count < 6:
        raise SystemExit(f"expected at least 6 lease-network functions, found {count}")
    return edited


CASES = [
    ("eng-1516", "TestBackend_LeasesOfOneTenantAreIsolated",
     "internal/backend/docker/lease_network.go", share_lease_network,
     "every lease is given the same network"),
    ("eng-1118", "TestBackend_TenantCannotChangeProjectID",
     "internal/backend/docker/tenantseccomp/profile_linux.go",
     once("\t\t{mask: ioctlDecisionMask, value: write | 'X'<<8 | 32},\n", ""),
     "the seccomp profile no longer denies FS_IOC_FSSETXATTR"),
    ("eng-2091", "TestBackend_RootContainerWritesItsVolume",
     "internal/backend/docker/provision.go",
     once("if err := root.Lchown(sanitized, uid, gid); err != nil {",
          "if err := func() error {\n\t\t\tif uid == 0 && gid == 0 {\n\t\t\t\treturn nil\n\t\t\t}\n\t\t\treturn root.Lchown(sanitized, uid, gid)\n\t\t}(); err != nil {"),
     "the volume directory is not handed to a root container"),
    ("eng-795", "TestBackend_SymlinkedVolumeDirectoryIsRefused",
     "internal/backend/docker/provision.go",
     once("if info.Mode()&fs.ModeSymlink != 0 {", "if info.Mode()&fs.ModeSymlink != 0 && false {"),
     "a symlinked volume directory is no longer refused"),
    ("eng-2181", "TestBackend_LinkedVolumeDoesNotBlockOtherLeases",
     "internal/backend/docker/volume_launch.go",
     once("protectedVolumesAffectSource(volumes, volumeWriterMountEvidence(writer.LeaseUUID, mount.Source))",
          "protectedVolumesAffectSource(volumes, mount.Source)"),
     "a writer's mount source is resolved whole, links included"),
    ("eng-658", "TestBackend_CloseDuringRestoreKeepsTheData",
     "internal/backend/docker/deprovision.go",
     once("func (b *Backend) ensureCommittedRestoreDestinationForClose(ctx context.Context, leaseUUID string) error {\n\tif b.retentionStore == nil {",
          "func (b *Backend) ensureCommittedRestoreDestinationForClose(ctx context.Context, leaseUUID string) error {\n\tif true || b.retentionStore == nil {"),
     "a close is admitted without checking that a restore into the lease committed"),
]


def build(case, path, edit):
    directory = os.path.join(OUT, case)
    os.makedirs(directory, exist_ok=True)
    original = os.path.join(ROOT, path)
    with open(original) as handle:
        edited = edit(handle.read())
    copy = os.path.join(directory, os.path.basename(path))
    with open(copy, "w") as handle:
        handle.write(edited)
    overlay = os.path.join(directory, "overlay.json")
    with open(overlay, "w") as handle:
        json.dump({"Replace": {original: copy}}, handle)
    subprocess.run(["go", "build", "-overlay", overlay, "-o", os.path.join(directory, "docker-backend"),
                    "./cmd/docker-backend"], check=True)
    return directory


def main():
    build_only = len(sys.argv) > 1 and sys.argv[1] == "--build-only"
    results = []
    for case, test, path, edit, what in CASES:
        directory = build(case, path, edit)
        if build_only:
            print(f"built {case}")
            continue
        env = ["env", "PATH=" + os.environ["PATH"],
               "FRED_E2E_BACKEND_BIN_DIR=" + directory,
               "FRED_E2E_LOG_DIR=" + os.path.join(directory, "logs"),
               "FRED_E2E_DOCKER_SOCKETS=" + os.environ.get("FRED_E2E_DOCKER_SOCKETS", ""),
               "FRED_TEST_IMAGE_DATA_PATH=" + os.environ.get("FRED_TEST_IMAGE_DATA_PATH", "")]
        run = subprocess.run(["sudo"] + env + [os.path.join(ROOT, "build", "e2e", "tests", "e2e.test"),
                              "-test.v", "-test.run", f"^{test}$", "-test.timeout", "10m"],
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        failed = f"--- FAIL: {test}" in run.stdout
        passed = f"--- PASS: {test}" in run.stdout
        verdict = "PROVEN" if failed and not passed else "NOT PROVEN"
        results.append((case, test, what, verdict))
        print(f"::group::{case}: {test} against a backend where {what} -> {verdict}")
        # The test's own output, up to the process logs it attaches.
        for line in run.stdout.splitlines():
            if re.match(r"\s+process\.go:\d+:", line):
                break
            print(line[:400])
        print("::endgroup::")
    if not build_only:
        print("\nRevert proof summary")
        for case, test, what, verdict in results:
            print(f"{verdict:<11} {case:<9} {test}: {what}")


if __name__ == "__main__":
    main()

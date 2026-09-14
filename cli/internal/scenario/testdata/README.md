# Packaged partial-read count regression gate

From the repository root, build the engine and run the opt-in Go test:

```sh
./engine/gradlew -p engine :bundle:dockerBuildImage -PdockerImageTag=partial-count-test --no-daemon
SPT_RANGE_TEST_IMAGE=ghcr.io/dell/storage-performance-tool:partial-count-test \
  go -C cli test ./internal/scenario -run TestPackagedRangeCountExceedsInventory -count=1 -v
```

Requires Docker, Python 3, and the repository's Go toolchain. The test never pulls
an image or uses an external S3 target. It runs the actual generated scenarios,
including their normal phase handoff and settlement delays, against an isolated
HTTP fixture. Two inventory objects must produce twelve successful three-byte
reads in seeded, existing-inventory, and two-worker fixed/random variants.

Assertions cover exact requests and range bounds, canonical per-worker counters,
validated bytes, reconciliation, ordinary seed/cleanup requests without Range,
and removal of objects, containers, the fixture server and Docker network.
Deterministic Java operation tests separately prove fresh random selection on
successful recycling and retention across retries; this gate does not rely on
random samples differing by chance.

Without `SPT_RANGE_TEST_IMAGE`, the Go test skips explicitly. Set
`SPT_RANGE_TEST_ARTIFACTS` to an existing directory to retain artifacts there;
otherwise they go under the system temporary directory. Each subtest prints its
artifact directory, including the exact command, generated script, image ID,
engine logs, canonical CSV and receipt. Use a source-specific image tag for
recorded release evidence. No registry publication is needed.

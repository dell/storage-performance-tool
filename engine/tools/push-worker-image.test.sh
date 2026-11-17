#!/usr/bin/env bash
# Tests for push-worker-image.sh. Needs no Docker daemon and no SSH.
#
# Run from anywhere:  bash engine/tools/push-worker-image.test.sh
# Requires bash, python3 and tar. Exits 0 when every case passes.
#
# The script under test is copied into a temporary tree, so it never sources
# the repository's cli/.env. Stub `docker` and `ssh` commands go first in PATH:
#   - local `docker save` writes a tar chosen by STUB_SAVE_MODE
#     (good, manifest-only, empty-layers);
#   - `ssh HOST CMD` runs CMD with bash as the "remote" side, where the stub
#     `docker` answers `load` and `image inspect` for that host;
#   - every ssh call is logged, to check which hosts were contacted.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

mkdir -p "${WORK}/repo/engine/tools" "${WORK}/bin"
cp "${SCRIPT_DIR}/push-worker-image.sh" "${WORK}/repo/engine/tools/"
SUT="${WORK}/repo/engine/tools/push-worker-image.sh"

IMAGE="example.test/spt:stub"
LOCAL_IDENTITY='linux/amd64 ["sha256:1111","sha256:2222"]'

cat >"${WORK}/bin/docker" <<'STUB'
#!/usr/bin/env bash
# Stub docker. STUB_HOST is set on the "remote" side by the ssh stub.
set -euo pipefail
host="${STUB_HOST:-local}"
case "$1 ${2:-}" in
  "image inspect")
    shift 2
    format=""
    ref=""
    while [ $# -gt 0 ]; do
      case "$1" in
        --format=*) format="${1#--format=}"; shift ;;
        --format) format="$2"; shift 2 ;;
        *) ref="$1"; shift ;;
      esac
    done
    [ "${ref}" = "${STUB_IMAGE}" ] || { echo "Error: No such image: ${ref}" >&2; exit 1; }
    if [ "${host}" != local ]; then
      case ",${STUB_INSPECT_FAIL_HOSTS:-}," in
        *",${host},"*) echo "Error: No such image: ${ref}" >&2; exit 1 ;;
      esac
    fi
    case "${format}" in
      "") echo '[{}]' ;;
      "{{.Size}}") echo 762740000 ;;
      '{{.Os}}/{{.Architecture}} {{json .RootFS.Layers}}')
        var="STUB_IDENTITY_${host//[^A-Za-z0-9]/_}"
        printf '%s\n' "${!var:-${STUB_LOCAL_IDENTITY}}" ;;
      *) echo "stub docker: unexpected format ${format}" >&2; exit 3 ;;
    esac ;;
  "save ${STUB_IMAGE}")
    [ "$3" = "-o" ] || { echo "stub docker: unexpected save args $*" >&2; exit 3; }
    cp "${STUB_TARS}/${STUB_SAVE_MODE}.tar" "$4" ;;
  "load -q")
    cat >/dev/null
    case ",${STUB_LOAD_FAIL_HOSTS:-}," in
      *",${host},"*) echo "Error: load failed" >&2; exit 1 ;;
    esac
    echo "Loaded image: ${STUB_IMAGE}" ;;
  *) echo "stub docker: unexpected command $*" >&2; exit 3 ;;
esac
STUB

cat >"${WORK}/bin/ssh" <<'STUB'
#!/usr/bin/env bash
# Stub ssh: skip options, then run the command with bash as HOST would.
set -euo pipefail
while [ $# -gt 0 ]; do
  case "$1" in
    -o) shift 2 ;;
    -*) shift ;;
    *) break ;;
  esac
done
host="$1"; shift
echo "${host} $*" >>"${STUB_SSH_LOG}"
STUB_HOST="${host}" exec bash -c "$*"
STUB
chmod +x "${WORK}/bin/docker" "${WORK}/bin/ssh"

# Build the tars `docker save` returns.
mkdir -p "${WORK}/tars"
python3 - "${WORK}/tars" "${IMAGE}" <<'PY'
import io, json, sys, tarfile

out, image = sys.argv[1], sys.argv[2]
config = "blobs/sha256/" + "c" * 64
layers = ["blobs/sha256/" + ch * 64 for ch in "ab"]

def write(name, manifest_layers, blobs):
    files = {
        "index.json": b'{"schemaVersion":2,"manifests":[]}',
        "oci-layout": b'{"imageLayoutVersion":"1.0.0"}',
        "manifest.json": json.dumps(
            [{"Config": config, "RepoTags": [image], "Layers": manifest_layers}]
        ).encode(),
    }
    for blob in blobs:
        files[blob] = b"x" * 512
    with tarfile.open("%s/%s.tar" % (out, name), "w") as tar:
        for path, data in files.items():
            info = tarfile.TarInfo(path)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

write("good", layers, [config] + layers)
write("manifest-only", layers, [])
write("empty-layers", [], [config])
PY

export PATH="${WORK}/bin:${PATH}"
export STUB_IMAGE="${IMAGE}" STUB_TARS="${WORK}/tars" STUB_LOCAL_IDENTITY="${LOCAL_IDENTITY}"
export STUB_SSH_LOG="${WORK}/ssh.log"
export WORKER_HOSTS="w1,w2"
unset PRIMARY_HOST HOSTS SSH_OPTS IMAGE_TAG IMAGE_NAME

failures=0
OUT=""
RC=0

# run_case NAME [VAR=value ...]: run the script with the given environment.
run_case() {
  CASE="$1"; shift
  : >"${STUB_SSH_LOG}"
  set +e
  OUT=$(env STUB_SAVE_MODE=good "$@" bash "${SUT}" --image "${IMAGE}" 2>&1)
  RC=$?
  set -e
}

fail() {
  echo "FAIL [${CASE}]: $*"
  echo "----- output (exit ${RC}) -----"
  echo "${OUT}"
  echo "----- ssh calls -----"
  cat "${STUB_SSH_LOG}"
  echo "-----"
  failures=$((failures+1))
}

expect_rc() { [ "${RC}" -eq "$1" ] || fail "exit ${RC}, want $1"; }
expect_out() { grep -qF -- "$1" <<<"${OUT}" || fail "output lacks: $1"; }
expect_no_out() { ! grep -qF -- "$1" <<<"${OUT}" || fail "output has: $1"; }
pass_if_clean() { [ "${failures}" -eq "$1" ] && echo "ok   [${CASE}]"; return 0; }

before=${failures}
run_case "good image on two workers"
expect_rc 0
expect_out "Image tar created: "
expect_out "(local image size: 727.406 MiB)"
expect_out "✓ Successfully loaded image on w1"
expect_out "✓ Successfully loaded image on w2"
expect_out "Distribution complete: 2/2 nodes successful"
grep -q "^w2 docker image inspect" "${STUB_SSH_LOG}" || fail "no remote inspect of w2"
pass_if_clean "${before}"

before=${failures}
run_case "manifest-only tar" STUB_SAVE_MODE=manifest-only
expect_rc 1
expect_out "ERROR: The saved image ${IMAGE} has no layer content"
expect_out "are not in the tar"
expect_out "local image size: 727.406 MiB"
expect_out "Re-pull the base image or rebuild the image"
expect_no_out "Successfully loaded"
[ ! -s "${STUB_SSH_LOG}" ] || fail "a host was contacted"
pass_if_clean "${before}"

before=${failures}
run_case "manifest with an empty Layers list" STUB_SAVE_MODE=empty-layers
expect_rc 1
expect_out "has no layer content: manifest.json lists no layers"
[ ! -s "${STUB_SSH_LOG}" ] || fail "a host was contacted"
pass_if_clean "${before}"

before=${failures}
run_case "worker reports another platform" 'STUB_IDENTITY_w2=unknown/unknown ["sha256:1111","sha256:2222"]'
expect_rc 1
expect_out "✓ Successfully loaded image on w1"
expect_out "✗ Image loaded on w2 does not match the local image"
expect_out "Distribution complete: 1/2 nodes successful"
expect_out "Failed nodes: w2"
pass_if_clean "${before}"

before=${failures}
run_case "worker reports other layers" 'STUB_IDENTITY_w1=linux/amd64 []'
expect_rc 1
expect_out "✗ Image loaded on w1 does not match the local image"
expect_out "✓ Successfully loaded image on w2"
expect_out "Failed nodes: w1"
pass_if_clean "${before}"

before=${failures}
run_case "worker inspect fails" STUB_INSPECT_FAIL_HOSTS=w2
expect_rc 1
expect_out "✗ Loaded image on w2 but could not inspect it there"
expect_out "Failed nodes: w2"
pass_if_clean "${before}"

before=${failures}
run_case "worker load fails" STUB_LOAD_FAIL_HOSTS=w1
expect_rc 1
expect_out "✗ Failed to distribute image to w1"
expect_out "✓ Successfully loaded image on w2"
expect_out "Failed nodes: w1"
grep -q "^w1 docker image inspect" "${STUB_SSH_LOG}" && fail "inspected w1 after a failed load"
pass_if_clean "${before}"

before=${failures}
CASE="unknown argument"
set +e
OUT=$(bash "${SUT}" --bogus 2>&1)
RC=$?
set -e
expect_rc 2
pass_if_clean "${before}"

if [ "${failures}" -gt 0 ]; then
  echo "${failures} check(s) failed"
  exit 1
fi
echo "all push-worker-image.sh checks passed"

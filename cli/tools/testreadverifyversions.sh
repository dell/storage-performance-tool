#!/usr/bin/env bash

# End-to-end qualification for read-verify historical-version discovery.
# Seeds controlled S3 version histories with mc, runs the candidate SPT CLI/image,
# and compares canonical manifests with an independent version-list oracle.

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -f "$ROOT_DIR/.env" ]]; then
	set -a
	# shellcheck disable=SC1091
	source "$ROOT_DIR/.env"
	set +a
fi

SPT_BIN=${SPT_BIN:-"$ROOT_DIR/spt"}
PHASE=all
SUITE=full
DRIVERS=${QUAL_DRIVERS:-"netty,aws"}
THREADS=${THREADS:-8}
HOSTS=${QUAL_HOSTS:-"127.0.0.1"}
MIN_HOSTS=${MIN_HOSTS:-1}
DISTRIBUTED_HOSTS=${QUAL_DISTRIBUTED_HOSTS:-""}
DISTRIBUTED_MIN_HOSTS=${QUAL_DISTRIBUTED_MIN_HOSTS:-2}
PAGE_VERSIONS=${QUAL_PAGE_VERSIONS:-1005}
RESULTS_DIR=${RESULTS_DIR:-"$ROOT_DIR/results"}
EVIDENCE_DIR=""
QUAL_ID=""
BUCKET=""
CLEANUP_ON_SUCCESS=false
CONFIRM_TARGET_MUTATION=false
ALLOW_INCOMPLETE=false
SKIP_IMAGE_PULL=${SPT_SKIP_IMAGE_PULL:-false}
IMAGE=${SPT_IMAGE:-""}
ENDPOINTS=${S3_ENDPOINTS:-${S3_ENDPOINT:-""}}
ACCESS_KEY=${S3_ACCESS_KEY:-""}
SECRET_KEY=${S3_SECRET_KEY:-""}
RESTRICTED_ACCESS_KEY=${S3_VERSION_DENIED_ACCESS_KEY:-""}
RESTRICTED_SECRET_KEY=${S3_VERSION_DENIED_SECRET_KEY:-""}
MC_ALIAS=qual
MC_CONFIG_DIR=""
WORK_DIR=""
RUNS_DIR=""
ORACLE_DIR=""
STATE_FILE=""
SUSPENDED_BUCKET=""
UNVERSIONED_BUCKET=""
BAD_VERSION_ID=""
MISSING_VERSION_ID=""

die() {
	echo "error: $*" >&2
	exit 1
}

require_value() {
	if (( $# < 2 )) || [[ -z "${2:-}" ]]; then
		die "$1 requires a value"
	fi
}

enabled() {
	case "${1,,}" in
		1|true|yes|on) return 0 ;;
		*) return 1 ;;
	esac
}

usage() {
	cat <<EOF
Usage: $(basename "$0") [options]

Seed and qualify read-verify --versions=current|all against an S3 target.
Target mutation is refused unless --confirm-target-mutation is supplied.

Modes:
  --phase all       Seed fixtures and run qualification (default)
  --phase seed      Seed fixtures and write independent oracles
  --phase run       Run against fixtures recorded in --evidence-dir
  --phase cleanup   Remove only recorded spt-version-qual-* buckets

Qualification:
  --suite smoke|full          smoke skips pagination and special-bucket cases
  --drivers CSV               netty,aws (default: $DRIVERS)
  --page-versions N           Data versions in pagination fixture (default: $PAGE_VERSIONS)
  --threads N                 SPT verification concurrency (default: $THREADS)
  --hosts CSV                 Hosts for ordinary cases (default: $HOSTS)
  --min-hosts N               Minimum ordinary hosts (default: $MIN_HOSTS)
  --distributed-hosts CSV     Optional hosts for the distributed pagination gate
  --distributed-min-hosts N   Minimum distributed hosts (default: $DISTRIBUTED_MIN_HOSTS)
  --allow-incomplete          Permit full-suite gaps and report passed-with-gaps

State and target:
  --evidence-dir DIR          Qualification state/results directory
  --bucket NAME               Dedicated bucket; must start spt-version-qual-
  --results-dir DIR           Parent for new evidence (default: $RESULTS_DIR)
  --confirm-target-mutation   Required for seed/all/cleanup
  --cleanup-on-success        Remove all recorded versions/buckets after a passing run

Candidate:
  --spt-bin PATH              Candidate CLI (default: $SPT_BIN)
  --image IMAGE               Commit-specific engine image
  --skip-image-pull           Use the preloaded image

Connection values are read from cli/.env and may be overridden by S3_ENDPOINTS,
S3_ACCESS_KEY, S3_SECRET_KEY, and QUAL_HOSTS. Permission-denied cases run only when
S3_VERSION_DENIED_ACCESS_KEY and S3_VERSION_DENIED_SECRET_KEY are configured.

Examples:
  $(basename "$0") --phase seed --confirm-target-mutation
  $(basename "$0") --phase run --evidence-dir ./results/version-qualification-...
  $(basename "$0") --phase all --image ghcr.io/dell/storage-performance-tool:allversions-<commit> \
    --skip-image-pull --confirm-target-mutation
  $(basename "$0") --phase cleanup --evidence-dir ./results/version-qualification-... \
    --confirm-target-mutation
EOF
}

while (( $# > 0 )); do
	case "$1" in
		--phase) require_value "$@"; PHASE=$2; shift 2 ;;
		--suite) require_value "$@"; SUITE=$2; shift 2 ;;
		--drivers) require_value "$@"; DRIVERS=$2; shift 2 ;;
		--page-versions) require_value "$@"; PAGE_VERSIONS=$2; shift 2 ;;
		--threads) require_value "$@"; THREADS=$2; shift 2 ;;
		--hosts) require_value "$@"; HOSTS=$2; shift 2 ;;
		--min-hosts) require_value "$@"; MIN_HOSTS=$2; shift 2 ;;
		--distributed-hosts) require_value "$@"; DISTRIBUTED_HOSTS=$2; shift 2 ;;
		--distributed-min-hosts) require_value "$@"; DISTRIBUTED_MIN_HOSTS=$2; shift 2 ;;
		--evidence-dir) require_value "$@"; EVIDENCE_DIR=$2; shift 2 ;;
		--bucket) require_value "$@"; BUCKET=$2; shift 2 ;;
		--results-dir) require_value "$@"; RESULTS_DIR=$2; shift 2 ;;
		--spt-bin) require_value "$@"; SPT_BIN=$2; shift 2 ;;
		--image) require_value "$@"; IMAGE=$2; shift 2 ;;
		--skip-image-pull) SKIP_IMAGE_PULL=true; shift ;;
		--no-skip-image-pull) SKIP_IMAGE_PULL=false; shift ;;
		--cleanup-on-success) CLEANUP_ON_SUCCESS=true; shift ;;
		--confirm-target-mutation) CONFIRM_TARGET_MUTATION=true; shift ;;
		--allow-incomplete) ALLOW_INCOMPLETE=true; shift ;;
		-h|--help) usage; exit 0 ;;
		*) die "unknown argument $1" ;;
	esac
done

case "$PHASE" in all|seed|run|cleanup) ;; *) die "--phase must be all, seed, run, or cleanup" ;; esac
case "$SUITE" in smoke|full) ;; *) die "--suite must be smoke or full" ;; esac
[[ "$PAGE_VERSIONS" =~ ^[0-9]+$ ]] || die "--page-versions must be a positive integer"
(( PAGE_VERSIONS > 0 )) || die "--page-versions must be positive"
if [[ "$SUITE" == "full" ]]; then
	(( PAGE_VERSIONS > 1000 )) || die "full suite requires --page-versions greater than 1000"
fi
[[ "$THREADS" =~ ^[0-9]+$ ]] && (( THREADS > 0 )) || die "--threads must be positive"
[[ "$MIN_HOSTS" =~ ^[0-9]+$ ]] && (( MIN_HOSTS > 0 )) || die "--min-hosts must be positive"
[[ "$DISTRIBUTED_MIN_HOSTS" =~ ^[0-9]+$ ]] && (( DISTRIBUTED_MIN_HOSTS > 0 )) ||
	die "--distributed-min-hosts must be positive"

IFS=',' read -r -a DRIVER_LIST <<< "$DRIVERS"
(( ${#DRIVER_LIST[@]} > 0 )) || die "--drivers must not be empty"
for driver in "${DRIVER_LIST[@]}"; do
	case "$driver" in
		netty|aws) ;;
		rdma) die "native RDMA is explicitly unqualified without hardware; use netty,aws" ;;
		*) die "unsupported qualification driver: $driver" ;;
	esac
done

for tool in mc jq sha256sum stat sort diff awk find; do
	command -v "$tool" >/dev/null || die "required tool is missing: $tool"
done

[[ -n "$ENDPOINTS" ]] || die "S3_ENDPOINTS or S3_ENDPOINT is required"
[[ -n "$ACCESS_KEY" ]] || die "S3_ACCESS_KEY is required"
[[ -n "$SECRET_KEY" ]] || die "S3_SECRET_KEY is required"

if [[ "$PHASE" == "run" || "$PHASE" == "cleanup" ]]; then
	[[ -n "$EVIDENCE_DIR" ]] || die "--evidence-dir is required for phase $PHASE"
	EVIDENCE_DIR="$(cd -- "$EVIDENCE_DIR" && pwd)"
	STATE_FILE="$EVIDENCE_DIR/state.json"
	[[ -f "$STATE_FILE" ]] || die "state file is missing: $STATE_FILE"
	QUAL_ID=$(jq -er '.qualification_id' "$STATE_FILE")
	BUCKET=$(jq -er '.bucket' "$STATE_FILE")
	SUSPENDED_BUCKET=$(jq -er '.suspended_bucket' "$STATE_FILE")
	UNVERSIONED_BUCKET=$(jq -er '.unversioned_bucket' "$STATE_FILE")
	BAD_VERSION_ID=$(jq -r '.bad_version_id // ""' "$STATE_FILE")
	MISSING_VERSION_ID=$(jq -r '.missing_version_id // ""' "$STATE_FILE")
	SUITE=$(jq -er '.suite' "$STATE_FILE")
else
	QUAL_ID=${QUAL_ID:-"$(date -u +%Y%m%d-%H%M%S)-$$"}
	BUCKET=${BUCKET:-"spt-version-qual-$QUAL_ID"}
	SUSPENDED_BUCKET="$BUCKET-suspended"
	UNVERSIONED_BUCKET="$BUCKET-unversioned"
	if [[ -z "$EVIDENCE_DIR" ]]; then
		EVIDENCE_DIR="$RESULTS_DIR/version-qualification-$QUAL_ID"
	fi
	mkdir -p "$EVIDENCE_DIR"
	EVIDENCE_DIR="$(cd -- "$EVIDENCE_DIR" && pwd)"
	STATE_FILE="$EVIDENCE_DIR/state.json"
fi

RUNS_DIR="$EVIDENCE_DIR/runs"
ORACLE_DIR="$EVIDENCE_DIR/oracles"
mkdir -p "$RUNS_DIR" "$ORACLE_DIR"
WORK_DIR=$(mktemp -d /tmp/spt-version-qualification.XXXXXX)
MC_CONFIG_DIR=$(mktemp -d /tmp/spt-version-mc.XXXXXX)

cleanup_local() {
	rm -rf -- "$WORK_DIR" "$MC_CONFIG_DIR"
}
trap cleanup_local EXIT

mcq() {
	mc --config-dir "$MC_CONFIG_DIR" "$@"
}

qual_endpoint=${ENDPOINTS%%,*}
mcq alias set "$MC_ALIAS" "$qual_endpoint" "$ACCESS_KEY" "$SECRET_KEY" >/dev/null

validate_qualification_bucket() {
	local bucket=$1
	[[ "$bucket" == spt-version-qual-* ]] ||
		die "refusing target mutation for non-qualification bucket: $bucket"
	[[ "$bucket" != *"/"* ]] || die "bucket name must not contain slash: $bucket"
}

for target_bucket in "$BUCKET" "$SUSPENDED_BUCKET" "$UNVERSIONED_BUCKET"; do
	validate_qualification_bucket "$target_bucket"
done

if [[ "$PHASE" == "all" || "$PHASE" == "seed" || "$PHASE" == "cleanup" ]]; then
	enabled "$CONFIRM_TARGET_MUTATION" ||
		die "--confirm-target-mutation is required for phase $PHASE"
fi

if enabled "$CLEANUP_ON_SUCCESS"; then
	enabled "$CONFIRM_TARGET_MUTATION" ||
		die "--cleanup-on-success requires --confirm-target-mutation"
fi
if [[ "$PHASE" == "run" || "$PHASE" == "all" ]]; then
	[[ -n "$IMAGE" ]] || die "--image or SPT_IMAGE is required for candidate qualification"
	if [[ "$SUITE" == "full" ]] && ! enabled "$ALLOW_INCOMPLETE"; then
		[[ -n "$RESTRICTED_ACCESS_KEY" && -n "$RESTRICTED_SECRET_KEY" ]] ||
			die "full suite requires restricted version-LIST credentials or --allow-incomplete"
		[[ -n "$DISTRIBUTED_HOSTS" ]] ||
			die "full suite requires --distributed-hosts or --allow-incomplete"
	fi
fi

write_state() {
	jq -n \
		--arg qualification_id "$QUAL_ID" \
		--arg suite "$SUITE" \
		--arg bucket "$BUCKET" \
		--arg suspended_bucket "$SUSPENDED_BUCKET" \
		--arg unversioned_bucket "$UNVERSIONED_BUCKET" \
		--arg bad_version_id "$BAD_VERSION_ID" \
		--arg missing_version_id "$MISSING_VERSION_ID" \
		--arg endpoint "$qual_endpoint" \
		--argjson page_versions "$PAGE_VERSIONS" \
		'{
			qualification_id: $qualification_id,
			suite: $suite,
			bucket: $bucket,
			suspended_bucket: $suspended_bucket,
			unversioned_bucket: $unversioned_bucket,
			bad_version_id: $bad_version_id,
			missing_version_id: $missing_version_id,
			endpoint: $endpoint,
			page_versions: $page_versions
		}' > "$STATE_FILE"
}

put_payload() {
	local bucket=$1
	local key=$2
	local content=$3
	local digest_override=${4:-}
	local payload="$WORK_DIR/payload.bin"
	local digest size attrs
	printf '%s' "$content" > "$payload"
	digest=$(sha256sum "$payload" | awk '{print $1}')
	size=$(stat -c '%s' "$payload")
	if [[ -n "$digest_override" ]]; then
		digest=$digest_override
	fi
	attrs="spt-integrity-version=1;spt-integrity-algorithm=sha256;spt-integrity-digest=$digest;spt-integrity-size=$size"
	mcq cp --quiet --attr "$attrs" "$payload" "$MC_ALIAS/$bucket/$key" >/dev/null
}

put_without_metadata() {
	local bucket=$1
	local key=$2
	local content=$3
	local payload="$WORK_DIR/payload.bin"
	printf '%s' "$content" > "$payload"
	mcq cp --quiet "$payload" "$MC_ALIAS/$bucket/$key" >/dev/null
}

only_data_version_id() {
	local bucket=$1
	local prefix=$2
	local key=$3
	mcq ls --recursive --versions --json "$MC_ALIAS/$bucket/$prefix" |
		jq -er --arg key "$key" \
			'select(.status == "success" and .key == $key and (.isDeleteMarker // false) == false) | .versionId' |
		head -n 1
}

write_oracle() {
	local bucket=$1
	local prefix=$2
	local name=$3
	local mode=$4
	local raw="$ORACLE_DIR/$name.raw.jsonl"
	local data="$ORACLE_DIR/$name.data.tsv"
	local markers="$ORACLE_DIR/$name.markers.tsv"
	if [[ "$mode" == "all" ]]; then
		mcq ls --recursive --versions --json "$MC_ALIAS/$bucket/$prefix" > "$raw"
		jq -r --arg bucket "$bucket" --arg prefix "$prefix" \
			'select(.status == "success" and (.isDeleteMarker // false) == false) |
			 [$bucket, ($prefix + .key), (.size | tostring), .versionId] | @tsv' \
			"$raw" | LC_ALL=C sort -t$'\t' -k1,1 -k2,2 -k4,4 > "$data"
		jq -r --arg bucket "$bucket" --arg prefix "$prefix" \
			'select(.status == "success" and (.isDeleteMarker // false) == true) |
			 [$bucket, ($prefix + .key), (.versionId // "")] | @tsv' \
			"$raw" | LC_ALL=C sort -t$'\t' -k1,1 -k2,2 -k3,3 > "$markers"
	else
		mcq ls --recursive --json "$MC_ALIAS/$bucket/$prefix" > "$raw"
		jq -r --arg bucket "$bucket" --arg prefix "$prefix" \
			'select(.status == "success") |
			 [$bucket, ($prefix + .key), (.size | tostring), ""] | @tsv' \
			"$raw" | LC_ALL=C sort -t$'\t' -k1,1 -k2,2 -k4,4 > "$data"
		: > "$markers"
	fi
}

line_count() {
	awk 'END {print NR + 0}' "$1"
}

seed_fixtures() {
	echo "Creating dedicated qualification buckets"
	write_state
	mcq mb --with-versioning "$MC_ALIAS/$BUCKET" >/dev/null
	mcq version info --json "$MC_ALIAS/$BUCKET" > "$EVIDENCE_DIR/main-bucket-versioning.json"

	put_payload "$BUCKET" "healthy/alpha" "a"
	put_payload "$BUCKET" "healthy/alpha" "alpha-version-two-is-longer"
	put_payload "$BUCKET" "healthy/alpha" "alpha-v3"
	put_payload "$BUCKET" "healthy/beta" "beta-v1"
	put_payload "$BUCKET" "healthy/beta" "beta-v2"
	mcq rm --quiet "$MC_ALIAS/$BUCKET/healthy/beta" >/dev/null
	put_payload "$BUCKET" "healthy/gamma" "gamma-v1"

	put_payload "$BUCKET" "corrupt/object" "corrupt-old" \
		"0000000000000000000000000000000000000000000000000000000000000000"
	BAD_VERSION_ID=$(only_data_version_id "$BUCKET" "corrupt/" "object")
	put_payload "$BUCKET" "corrupt/object" "corrupt-current-healthy"

	put_without_metadata "$BUCKET" "missing/object" "missing-old"
	MISSING_VERSION_ID=$(only_data_version_id "$BUCKET" "missing/" "object")
	put_payload "$BUCKET" "missing/object" "missing-current-healthy"

	if [[ "$SUITE" == "full" ]]; then
		echo "Seeding version-completeness partition fixture"
		local partition_index
		for ((partition_index = 1; partition_index <= THREADS; partition_index++)); do
			put_payload "$BUCKET" "partition/live-$partition_index/object" \
				"partition-live-$partition_index"
		done
		put_payload "$BUCKET" "partition/deleted-only/object" "deleted-historical-data"
		mcq rm --quiet "$MC_ALIAS/$BUCKET/partition/deleted-only/object" >/dev/null

		echo "Seeding $PAGE_VERSIONS data versions for paired-marker pagination"
		local i
		for ((i = 1; i <= PAGE_VERSIONS; i++)); do
			put_payload "$BUCKET" "paged/same-key" "paged-payload"
			if (( i == PAGE_VERSIONS / 3 || i == (PAGE_VERSIONS * 2) / 3 )); then
				mcq rm --quiet "$MC_ALIAS/$BUCKET/paged/same-key" >/dev/null
			fi
			if (( i % 100 == 0 )); then
				echo "  seeded $i/$PAGE_VERSIONS versions"
			fi
		done

		mcq mb --with-versioning "$MC_ALIAS/$SUSPENDED_BUCKET" >/dev/null
		put_payload "$SUSPENDED_BUCKET" "suspended/object" "opaque-version"
		mcq version suspend "$MC_ALIAS/$SUSPENDED_BUCKET" >/dev/null
		mcq version info --json "$MC_ALIAS/$SUSPENDED_BUCKET" > "$EVIDENCE_DIR/suspended-bucket-versioning.json"
		put_payload "$SUSPENDED_BUCKET" "suspended/object" "literal-null-version"

		mcq mb "$MC_ALIAS/$UNVERSIONED_BUCKET" >/dev/null
		mcq version info --json "$MC_ALIAS/$UNVERSIONED_BUCKET" > "$EVIDENCE_DIR/unversioned-bucket-versioning.json"
		put_payload "$UNVERSIONED_BUCKET" "unversioned/object" "unversioned-payload"
	fi

	write_oracle "$BUCKET" "healthy/" "healthy-current" current
	write_oracle "$BUCKET" "healthy/" "healthy-all" all
	write_oracle "$BUCKET" "corrupt/" "corrupt-current" current
	write_oracle "$BUCKET" "corrupt/" "corrupt-all" all
	write_oracle "$BUCKET" "missing/" "missing-current" current
	write_oracle "$BUCKET" "missing/" "missing-all" all
	if [[ "$SUITE" == "full" ]]; then
		write_oracle "$BUCKET" "partition/" "partition-all" all
		write_oracle "$BUCKET" "paged/" "paged-all" all
		write_oracle "$SUSPENDED_BUCKET" "suspended/" "suspended-all" all
		write_oracle "$UNVERSIONED_BUCKET" "unversioned/" "unversioned-all" all
	fi

	write_state

	echo "Seed complete"
	echo "  Evidence: $EVIDENCE_DIR"
	echo "  Bucket:   $BUCKET"
	echo "  Healthy:  $(line_count "$ORACLE_DIR/healthy-all.data.tsv") data versions, $(line_count "$ORACLE_DIR/healthy-all.markers.tsv") markers"
	if [[ "$SUITE" == "full" ]]; then
		echo "  Partition: $(line_count "$ORACLE_DIR/partition-all.data.tsv") data versions, $(line_count "$ORACLE_DIR/partition-all.markers.tsv") markers"
		echo "  Paged:    $(line_count "$ORACLE_DIR/paged-all.data.tsv") data versions, $(line_count "$ORACLE_DIR/paged-all.markers.tsv") markers"
	fi
}

normalize_manifest() {
	local manifest=$1
	local output=$2
	awk -F',' 'NR > 1 {
		sub(/\r$/, "", $4)
		print $1 "\t" $2 "\t" $3 "\t" $4
	}' "$manifest" | LC_ALL=C sort -t$'\t' -k1,1 -k2,2 -k4,4 > "$output"
}

expected_selection() {
	local oracle=$1
	local cap=$2
	local output=$3
	if (( cap > 0 )); then
		head -n "$cap" "$oracle" > "$output"
	else
		cp "$oracle" "$output"
	fi
}

find_result_dir() {
	local label=$1
	find "$RUNS_DIR" -mindepth 1 -maxdepth 1 -type d -name "$label-*" -print |
		LC_ALL=C sort | tail -n 1
}

assert_artifacts() {
	local result_dir=$1
	local oracle=$2
	local cap=$3
	local marker_count=$4
	local corrupt_version=${5:-}
	local failure_reason=${6:-}
	local source_multiplier=${7:-1}
	local index="$result_dir/index.json"
	local input="$result_dir/verify-input.csv"
	local completion="$result_dir/verify-input.complete.json"
	local verified="$result_dir/verified.csv"
	local remaining="$result_dir/verify-remaining.csv"
	local expected="$WORK_DIR/expected.tsv"
	local actual="$WORK_DIR/actual.tsv"
	local expected_verified="$WORK_DIR/expected-verified.tsv"
	local actual_verified="$WORK_DIR/actual-verified.tsv"
	local actual_remaining="$WORK_DIR/actual-remaining.tsv"
	local source_count raw_source_count selected_count corrupt_count verified_count
	[[ -f "$index" ]] || die "missing index.json in $result_dir"
	[[ -f "$input" && -f "$completion" && -f "$verified" && -f "$remaining" ]] ||
		die "missing canonical integrity artifacts in $result_dir"

	source_count=$(line_count "$oracle")
	# Every distributed node receives the LIST seed; finalization de-duplicates the overlap.
	raw_source_count=$((source_count * source_multiplier))
	marker_count=$((marker_count * source_multiplier))
	if (( cap > 0 && cap < source_count )); then
		selected_count=$cap
	else
		selected_count=$source_count
	fi
	if [[ -n "$corrupt_version" ]]; then
		corrupt_count=1
	else
		corrupt_count=0
	fi
	verified_count=$((selected_count - corrupt_count))

	jq -e \
		--argjson source "$raw_source_count" \
		--argjson unique "$source_count" \
		--argjson selected "$selected_count" \
		--argjson markers "$marker_count" \
		--argjson corrupt "$corrupt_count" \
		--argjson verified "$verified_count" \
		'.integrity.complete == true and
		 .integrity.selection_counts_valid == true and
		 .integrity.selection_source_count == $source and
		 .integrity.selection_unique_count == $unique and
		 .integrity.selection_count == $selected and
		 .integrity.excluded_delete_marker_count == $markers and
		 .integrity.verification_attempted_count == $selected and
		 .integrity.verified_count == $verified and
		 .integrity.corrupt_count == $corrupt and
		 .integrity.remaining_count == $corrupt' "$index" >/dev/null ||
		die "integrity count assertion failed for $result_dir"

	expected_selection "$oracle" "$cap" "$expected"
	normalize_manifest "$input" "$actual"
	diff -u "$expected" "$actual" ||
		die "verify-input.csv differs from the independent oracle"

	if [[ -n "$corrupt_version" ]]; then
		awk -F'\t' -v version="$corrupt_version" '$4 != version' "$expected" > "$expected_verified"
	else
		cp "$expected" "$expected_verified"
	fi
	normalize_manifest "$verified" "$actual_verified"
	diff -u "$expected_verified" "$actual_verified" ||
		die "verified.csv differs from expected healthy identities"

	normalize_manifest "$remaining" "$actual_remaining"
	if [[ -n "$corrupt_version" ]]; then
		awk -F'\t' -v version="$corrupt_version" '$4 == version' "$expected" > "$WORK_DIR/expected-remaining.tsv"
		diff -u "$WORK_DIR/expected-remaining.tsv" "$actual_remaining" ||
			die "verify-remaining.csv does not contain the exact corrupt identity"
		local failure_file="$result_dir/integrity.failures.csv"
		[[ -f "$failure_file" ]] || die "missing integrity.failures.csv"
		awk -F',' -v reason="$failure_reason" -v version="$corrupt_version" \
			'NR > 1 && $6 == version && $9 == reason {found = 1} END {exit !found}' "$failure_file" ||
			die "missing exact $failure_reason failure row for version $corrupt_version"
	else
		[[ ! -s "$actual_remaining" ]] || die "healthy run has remaining identities"
	fi

	local manifest_bytes manifest_sha
	manifest_bytes=$(stat -c '%s' "$input")
	manifest_sha=$(sha256sum "$input" | awk '{print $1}')
	jq -e \
		--argjson source "$raw_source_count" \
		--argjson unique "$source_count" \
		--argjson selected "$selected_count" \
		--argjson markers "$marker_count" \
		--argjson bytes "$manifest_bytes" \
		--arg sha "$manifest_sha" \
		'.version == 2 and .status == "complete" and
		 .source_record_count == $source and
		 .unique_record_count == $unique and
		 .selected_record_count == $selected and
		 .excluded_delete_marker_count == $markers and
		 .manifest_bytes == $bytes and .manifest_sha256 == $sha' "$completion" >/dev/null ||
		die "completion evidence assertion failed for $result_dir"
}

assert_distributed_identity() {
	local result_dir=$1
	local expected_host_count=$2
	local params="$result_dir/spt_run_params.json"
	[[ -f "$params" ]] || die "missing distributed runtime identity evidence in $result_dir"
	jq -e --argjson expected "$expected_host_count" '
		. as $root |
		.multiHost.enabled == true and
		.multiHost.hostCount == $expected and
		.runtimeIdentity.tier == "immutable_image_and_payload" and
		(.runtimeIdentity.imageId | startswith("sha256:")) and
		(.runtimeIdentity.payloadSha256 | test("^[0-9a-f]{64}$")) and
		(.runtimeIdentity.participants | length) == $expected and
		all(.runtimeIdentity.participants[];
			.imageId == $root.runtimeIdentity.imageId and
			.payloadSha256 == $root.runtimeIdentity.payloadSha256)
	' "$params" >/dev/null || die "distributed runtime identity assertion failed for $result_dir"
}

record_candidate_identity() {
	local commit cli_sha image_id=""
	commit=$(git -C "$ROOT_DIR/.." rev-parse HEAD)
	cli_sha=$(sha256sum "$SPT_BIN" | awk '{print $1}')
	if [[ -n "$IMAGE" ]] && command -v docker >/dev/null; then
		image_id=$(docker image inspect "$IMAGE" --format '{{.Id}}' 2>/dev/null || true)
	fi
	if enabled "$SKIP_IMAGE_PULL"; then
		[[ -n "$image_id" ]] || die "preloaded candidate image is not locally inspectable: $IMAGE"
	fi
	jq -n \
		--arg commit "$commit" \
		--arg cli "$SPT_BIN" \
		--arg cli_sha256 "$cli_sha" \
		--arg image "$IMAGE" \
		--arg image_id "$image_id" \
		'{
			source_commit: $commit,
			cli_path: $cli,
			cli_sha256: $cli_sha256,
			engine_image: $image,
			engine_image_id: $image_id,
			rdma: "native path unqualified: hardware unavailable"
		}' > "$EVIDENCE_DIR/candidate-identity.json"
}

run_case() {
	local driver=$1
	local case_name=$2
	local bucket=$3
	local prefix=$4
	local versions=$5
	local oracle=$6
	local cap=$7
	local corrupt_version=$8
	local failure_reason=$9
	local hosts=${10}
	local min_hosts=${11}
	local access=${12:-$ACCESS_KEY}
	local secret=${13:-$SECRET_KEY}
	local source_multiplier=${14:-1}
	local expected_exit=0
	local source_count marker_count label result_dir console_log
	local -a cmd
	source_count=$(line_count "$oracle")
	marker_count=$(line_count "${oracle%.data.tsv}.markers.tsv")
	if [[ -n "$corrupt_version" ]]; then
		expected_exit=20
	fi
	label="avq-${QUAL_ID//[^A-Za-z0-9]/}-${case_name//[^A-Za-z0-9]/}-$driver"
	label=${label:0:60}
	console_log="$EVIDENCE_DIR/$label.console.log"

	cmd=("$SPT_BIN" run read-verify
		--headless
		--auto-results=true
		--shutdown-on-complete=true
		--endpoints "$ENDPOINTS"
		--access-key "$access"
		--secret-key "$secret"
		--bucket "$bucket"
		--prefix "$prefix"
		--versions "$versions"
		--s3-driver "$driver"
		--test-hosts "$hosts"
		--min-hosts "$min_hosts"
		--threads "$THREADS"
		--results-dir "$RUNS_DIR"
		--label "$label"
		--force)
	if (( cap > 0 )); then
		cmd+=(--object-count "$cap")
	fi
	if (( source_count == 0 )); then
		cmd+=(--allow-empty-selection)
	fi
	if (( source_multiplier > 1 )); then
		# Distributed qualification is release evidence, so require the stronger payload tier.
		cmd+=(--integrity-runtime-identity-tier payload)
	fi

	echo "Running $case_name with $driver ($versions, source=$source_count, markers=$marker_count)"
	set +e
	"${cmd[@]}" > "$console_log" 2>&1
	local exit_code=$?
	set -e
	if (( exit_code != expected_exit )); then
		tail -80 "$console_log" >&2
		die "$case_name/$driver exit $exit_code, expected $expected_exit"
	fi
	result_dir=$(find_result_dir "$label")
	[[ -n "$result_dir" ]] || die "cannot locate results for $case_name/$driver"
	assert_artifacts "$result_dir" "$oracle" "$cap" "$marker_count" "$corrupt_version" "$failure_reason" \
		"$source_multiplier"
	if (( source_multiplier > 1 )); then
		assert_distributed_identity "$result_dir" "$source_multiplier"
	fi
	echo "  PASS: $result_dir"
}

run_permission_case() {
	local driver=$1
	if [[ -z "$RESTRICTED_ACCESS_KEY" || -z "$RESTRICTED_SECRET_KEY" ]]; then
		echo "SKIP: version-LIST permission case ($driver): restricted credential is not configured"
		return
	fi
	run_case "$driver" "permission-current" "$BUCKET" "healthy/" current \
		"$ORACLE_DIR/healthy-current.data.tsv" 0 "" "" "$HOSTS" "$MIN_HOSTS" \
		"$RESTRICTED_ACCESS_KEY" "$RESTRICTED_SECRET_KEY"

	local label="avq-${QUAL_ID//[^A-Za-z0-9]/}-permissionall-$driver"
	label=${label:0:60}
	local console_log="$EVIDENCE_DIR/$label.console.log"
	local -a cmd=("$SPT_BIN" run read-verify
		--headless --auto-results=true --shutdown-on-complete=true
		--endpoints "$ENDPOINTS"
		--access-key "$RESTRICTED_ACCESS_KEY"
		--secret-key "$RESTRICTED_SECRET_KEY"
		--bucket "$BUCKET"
		--prefix "healthy/"
		--versions all
		--s3-driver "$driver"
		--test-hosts "$HOSTS"
		--min-hosts "$MIN_HOSTS"
		--threads "$THREADS"
		--results-dir "$RUNS_DIR"
		--label "$label"
		--force)
	echo "Running permission-all with $driver (expected authorization failure)"
	set +e
	"${cmd[@]}" > "$console_log" 2>&1
	local exit_code=$?
	set -e
	(( exit_code == 1 )) || {
		tail -80 "$console_log" >&2
		die "permission-all/$driver exit $exit_code, expected 1"
	}
	local result_dir
	result_dir=$(find_result_dir "$label")
	if [[ -n "$result_dir" && -f "$result_dir/verify-input.complete.json" ]]; then
		die "permission-all/$driver published a usable discovery completion"
	fi
	echo "  PASS: authorization failure was fail-closed"
}

run_qualification() {
	[[ -x "$SPT_BIN" ]] || die "SPT binary is missing or not executable: $SPT_BIN"
	[[ -f "$STATE_FILE" ]] || die "seed state is missing: $STATE_FILE"
	[[ -n "$BAD_VERSION_ID" ]] || die "seed state has no corrupt historical version ID"
	[[ -n "$MISSING_VERSION_ID" ]] || die "seed state has no missing-metadata historical version ID"
	for oracle in healthy-current healthy-all corrupt-current corrupt-all missing-current missing-all; do
		[[ -f "$ORACLE_DIR/$oracle.data.tsv" ]] || die "oracle is missing: $oracle"
	done

	if [[ -n "$IMAGE" ]]; then
		export SPT_IMAGE="$IMAGE"
	fi
	if enabled "$SKIP_IMAGE_PULL"; then
		export SPT_SKIP_IMAGE_PULL=true
	else
		unset SPT_SKIP_IMAGE_PULL
	fi
	record_candidate_identity

	for driver in "${DRIVER_LIST[@]}"; do
		run_case "$driver" "healthy-current" "$BUCKET" "healthy/" current \
			"$ORACLE_DIR/healthy-current.data.tsv" 0 "" "" "$HOSTS" "$MIN_HOSTS"
		run_case "$driver" "healthy-all" "$BUCKET" "healthy/" all \
			"$ORACLE_DIR/healthy-all.data.tsv" 0 "" "" "$HOSTS" "$MIN_HOSTS"
		run_case "$driver" "healthy-cap" "$BUCKET" "healthy/" all \
			"$ORACLE_DIR/healthy-all.data.tsv" 3 "" "" "$HOSTS" "$MIN_HOSTS"
		run_case "$driver" "corrupt-current" "$BUCKET" "corrupt/" current \
			"$ORACLE_DIR/corrupt-current.data.tsv" 0 "" "" "$HOSTS" "$MIN_HOSTS"
		run_case "$driver" "corrupt-all" "$BUCKET" "corrupt/" all \
			"$ORACLE_DIR/corrupt-all.data.tsv" 0 "$BAD_VERSION_ID" "digest_mismatch" "$HOSTS" "$MIN_HOSTS"
		run_case "$driver" "missing-current" "$BUCKET" "missing/" current \
			"$ORACLE_DIR/missing-current.data.tsv" 0 "" "" "$HOSTS" "$MIN_HOSTS"
		run_case "$driver" "missing-all" "$BUCKET" "missing/" all \
			"$ORACLE_DIR/missing-all.data.tsv" 0 "$MISSING_VERSION_ID" "metadata_missing" "$HOSTS" "$MIN_HOSTS"

		if [[ "$SUITE" == "full" ]]; then
			run_case "$driver" "partition-all" "$BUCKET" "partition/" all \
				"$ORACLE_DIR/partition-all.data.tsv" 0 "" "" "$HOSTS" "$MIN_HOSTS"
			run_case "$driver" "paged-all" "$BUCKET" "paged/" all \
				"$ORACLE_DIR/paged-all.data.tsv" 0 "" "" "$HOSTS" "$MIN_HOSTS"
			run_case "$driver" "suspended-all" "$SUSPENDED_BUCKET" "suspended/" all \
				"$ORACLE_DIR/suspended-all.data.tsv" 0 "" "" "$HOSTS" "$MIN_HOSTS"
			run_case "$driver" "unversioned-all" "$UNVERSIONED_BUCKET" "unversioned/" all \
				"$ORACLE_DIR/unversioned-all.data.tsv" 0 "" "" "$HOSTS" "$MIN_HOSTS"
		fi
		run_permission_case "$driver"
	done

	if [[ "$SUITE" == "full" && -n "$DISTRIBUTED_HOSTS" ]]; then
		local -a distributed_host_list
		IFS=',' read -r -a distributed_host_list <<< "$DISTRIBUTED_HOSTS"
		local distributed_host_count=${#distributed_host_list[@]}
		for driver in "${DRIVER_LIST[@]}"; do
			run_case "$driver" "paged-distributed" "$BUCKET" "paged/" all \
				"$ORACLE_DIR/paged-all.data.tsv" 0 "" "" \
				"$DISTRIBUTED_HOSTS" "$DISTRIBUTED_MIN_HOSTS" \
				"$ACCESS_KEY" "$SECRET_KEY" "$distributed_host_count"
		done
	elif [[ "$SUITE" == "full" ]]; then
		echo "SKIP: distributed pagination gate: --distributed-hosts is not configured"
	fi

	local status=passed
	local restricted_gap=""
	local distributed_gap=""
	if [[ -z "$RESTRICTED_ACCESS_KEY" || -z "$RESTRICTED_SECRET_KEY" ]]; then
		restricted_gap="version-LIST permission case not run"
	fi
	if [[ "$SUITE" == "full" && -z "$DISTRIBUTED_HOSTS" ]]; then
		distributed_gap="distributed pagination case not run"
	fi
	if [[ -n "$restricted_gap" || -n "$distributed_gap" ]]; then
		status=passed-with-gaps
	fi
	jq -n \
		--arg status "$status" \
		--arg qualification_id "$QUAL_ID" \
		--arg drivers "$DRIVERS" \
		--arg distributed "${DISTRIBUTED_HOSTS:-not run}" \
		--arg restricted_gap "$restricted_gap" \
		--arg distributed_gap "$distributed_gap" \
		--arg rdma "native path unqualified: hardware unavailable" \
		'{
			status: $status,
			qualification_id: $qualification_id,
			e2e_drivers: ($drivers | split(",")),
			distributed_hosts: $distributed,
			gaps: [$restricted_gap, $distributed_gap] | map(select(length > 0)),
			rdma: $rdma
		}' > "$EVIDENCE_DIR/qualification-summary.json"
	echo "Qualification $status: $EVIDENCE_DIR"
}

cleanup_bucket() {
	local bucket=$1
	validate_qualification_bucket "$bucket"
	echo "Removing all versions and bucket $bucket"
	mcq rb --force "$MC_ALIAS/$bucket" >/dev/null
}

cleanup_target() {
	for target_bucket in "$BUCKET" "$SUSPENDED_BUCKET" "$UNVERSIONED_BUCKET"; do
		if mcq stat "$MC_ALIAS/$target_bucket" >/dev/null 2>&1; then
			cleanup_bucket "$target_bucket"
		fi
	done
	echo "Qualification buckets removed"
}

case "$PHASE" in
	seed)
		seed_fixtures
		;;
	run)
		run_qualification
		if enabled "$CLEANUP_ON_SUCCESS"; then
			cleanup_target
		fi
		;;
	all)
		seed_fixtures
		run_qualification
		if enabled "$CLEANUP_ON_SUCCESS"; then
			cleanup_target
		fi
		;;
	cleanup)
		cleanup_target
		;;
esac


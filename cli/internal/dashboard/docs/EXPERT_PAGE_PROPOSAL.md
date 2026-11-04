# Expert page: analysis and proposal

*Spec only. Expert mode belongs to Yine (PR #15, Expert view redesign). Her redesign reaches this branch through #16, but the workloads and flag groups proposed here are not implemented. IDs (H-, P-) refer to the requirements traceability list kept with the branch artifacts.*

Source: `spt run --help` (SPT 5.16.1-dev), the 10/14 punch list (P-10), Mike 10-21 [00:23:06]–[00:23:40] (H-42) and 10/5 (P-15).

**Workloads offered**
- `mock`: no S3 target needed (default for demos).
- `write`, `read`, `write-verify` (Guided default), `read-verify`, `mixed`, `list`.
- `delete` is destructive. Show it only behind an explicit confirm and only for the run's own `dashboard/<runID>/` prefix.
- `tables` is hidden as not applicable.

**Flags shown, in four groups**
1. **Load**: `--threads`, `--object-count`, `--object-size`, `--duration`, `--auto-terminate-seconds`, `--min-hosts`
2. **Mix** (mixed/read only): `--get-distrib`, `--put-distrib`, `--delete-distrib`, `--stat-distrib`, `--shuffle`, `--seed-objects`, `--range-size/offset/align`, `--prefix-shards`
3. **Data**: `--object-data-compressibility`, `--object-data-dedupable`, `--checksum`, `--part-size`
4. **Protocol**: `--s3-driver`, `--auth-version`

**Flags blocked**: anything that changes where files go, what image or code runs, or what gets deleted, plus credentials and ports:
- `--results-dir`, `-O/--output-dir`
- `--spt-image`, `--skip-image-pull`, `--engine-override`
- `--trace-file`, `--log-file`
- `--items-file`, `--read-items-file`, `--delete-items-file`
- `--delete-existing`, `--allow-empty-prefix`, `--force`
- `--network-mode`, `--attach-existing`, `--generate-only`, `--keep-scenario`
- `rdma-*`
- `-a/-s` credentials, `--api-port`, RMI ports, `--dns-server`

The server, not the UI, owns this allow-list. An unknown flag is rejected (`DisallowUnknownFields`, already the pattern in `run.go`).

**Validation driven by flag definitions**
- Generate a JSON schema from the cobra flag set: name, type, default, usage, and enum where the help lists values.
- Expose it at `GET /api/expert/schema` and render the form from it. Client-side checks are type and shape only.
- Range limits: per Mike 10-21 H-42, the UI should **not** filter large values. SPT enforces them, and the UI shows SPT's error, through the new run log panel.

**Configurable caps**
- Today's defaults (punch-list `config.go`, overridable in the `limits` JSON): Guided offers 1–2 threads, up to 320 objects, and up to 600 s. Expert allows 1–64 threads, 1–100,000 objects, 1 KiB–1 GiB objects, 10–3,600 s, and at most 100 GiB in total.
- Keep the Guided presets as they are.
- In Expert mode the caps come from the settings file, with no hard-coded ceiling beyond request-size and total-bytes sanity checks. Reason: "customer-scale tests are much bigger" (P-10).

**Scope guard (P-15)**: implement after Guided reliability work. This branch does not implement it.

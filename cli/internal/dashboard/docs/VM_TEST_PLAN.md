# Dev VM test plan: catalog, compare, logs, mock driver

This plan checks the `feature/stupelify/run-catalog-compare` dashboard on the Capstone Dev VM. It covers the parts the box could not: a real S3 write/verify run started from the dashboard, the real `~/results` folder, and the SSH tunnel. Record each step as **pass**, **fail**, or **not run**, with the time (CT) and a screenshot or the command output.

Placeholders: `<vm>` is the VM's SSH host (for example `hgoyal7@<vm-host>`), and `<branch-dir>` is where the repo is checked out on the VM.

## 0. Before you start

- [ ] `ssh <vm> 'spt version; docker info --format "{{.ServerVersion}}"; ls ~/results'`. Expect Docker to answer and `~/results` to list `mt-dashboard-20261021T170103-518aaa08` and `manual-real-20261016T220230Z-22d12f`.
- [ ] Note the installed SPT version (`/usr/local/bin/spt` was 5.16.0 on 10-23). Do **not** overwrite it; run the branch build from its own path.
- [ ] Confirm the S3 settings exist in the VM's `.env` (endpoint, bucket, `S3_ACCESS_KEY`, `S3_SECRET_KEY`) by **name only**: `grep -c '^S3_ACCESS_KEY=' ~/.env`. Never print the values.
- [ ] No other dashboard is running: `ss -ltnp | grep -E ':8000\b'` prints nothing.

## 1. Build

Pick one:

**A. Build on the VM** (needs Go 1.25+ and Node 22.6+ there):

```sh
cd <branch-dir>
git fetch origin && git checkout feature/stupelify/run-catalog-compare   # after the branch is pushed
cd cli/internal/dashboard/frontend && npm ci && npm run build && cd -
make build-cli                 # writes cli/spt
./cli/spt version
```

**B. Build elsewhere and copy** (Mac or the box, cross-compiled for the VM):

```sh
cd cli && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o spt-linux-amd64 . && cd -
scp cli/spt-linux-amd64 <vm>:~/spt-branch
ssh <vm> 'chmod +x ~/spt-branch && ~/spt-branch version'
```

- [ ] The build succeeds and `version` prints.
- [ ] `git diff --exit-code cli/internal/dashboard/server/index.html` is clean after `npm run build`. The committed page matches the source.

## 2. Start the dashboard on the VM (loopback only)

```sh
ssh <vm>
cd ~ && BROWSER=true ./spt-branch dashboard --port 8000      # or <branch-dir>/cli/spt
```

`BROWSER=true` makes the dashboard run `true` instead of `xdg-open`, so nothing tries to open a browser on the VM. `--no-browser` (from Alex's PR #12) does the same; either works. Keep this terminal open.

- [ ] The log prints `http://127.0.0.1:8000`.
- [ ] In a second VM shell, `ss -ltnp | grep 8000` shows only `127.0.0.1:8000`, not `0.0.0.0` or `*`.

## 3. Tunnel from your laptop

```sh
ssh -N -L 8000:127.0.0.1:8000 <vm>
```

Open <http://127.0.0.1:8000> on the laptop.

- [ ] The page loads; the footer says "SPT server connected".
- [ ] No Dell branding appears in the UI.

## 4. HTTP endpoint check (through the tunnel)

```sh
B=http://127.0.0.1:8000
curl -s -o /dev/null -w '%{http_code} state\n'   $B/api/state
curl -s -o /dev/null -w '%{http_code} catalog\n' $B/api/catalog
curl -s -o /dev/null -w '%{http_code} runs\n'    $B/api/runs
for p in /api/state /api/runs /api/catalog /api/catalog/compare /api/catalog/log "/api/catalog/run?id=x" \
         /api/runs/mt-dashboard-20261021T170103-518aaa08 /api/runs/mt-dashboard-20261021T170103-518aaa08/log \
         /api/runs/mt-dashboard-20261021T170103-518aaa08/csv; do
  curl -s -o /dev/null -w "%{http_code} foreign-host $p (want 403)\n" -H 'Host: evil.example' "$B$p"
  curl -s -o /dev/null -w "%{http_code} foreign-origin $p (want 403)\n" -H 'Origin: http://evil.example' "$B$p"
done
for p in /api/runs /api/attach /api/storage/verify; do
  curl -s -o /dev/null -w "%{http_code} foreign-host POST $p (want 403)\n" -X POST -H 'Host: evil.example' "$B$p"
done
curl -s -o /dev/null -w '%{http_code} localhost:8000 host (want 200)\n' -H 'Host: localhost:8000' $B/api/state
curl -s -o /dev/null -w '%{http_code} POST catalog (want 405)\n' -X POST $B/api/catalog
curl -s $B/api/state $B/api/catalog | grep -ciE 'secretkey|accesskey|secret_key|access_key'   # want 0
```

- [ ] state, catalog, and runs return 200, also with `Host: localhost:8000`. Every `/api` route answers 403 to a foreign Host or a foreign Origin, before any handler runs. The POST to `/api/catalog` gets 405.
- [ ] `/` itself still loads with any Host (the static page has no data); only `/api` is guarded. A tunnel that uses a different local port (`-L 9000:127.0.0.1:8000`) gets 403 on `/api`; forward the same port number.
- [ ] The credential grep prints `0`.

## 5. Guided 25 × 1 MiB S3 run (real storage)

1. **Configure run**, step 1: client machine `127.0.0.1` (or the agreed hosts), endpoint blank (it uses `.env`). Click **Verify connection**. Expect every node and the S3 endpoint to pass.
2. Step 2: driver **S3 write + verify**, mode **Guided**, Maximum objects **25**, Object size **1MiB**, threads 1, limit 60 s. **Review run**, then **Start run**.
3. Watch the **Live dashboard** until the run completes.

- [ ] Status reaches Completed; integrity passed; 25 objects written and verified; 0 corrupt.
- [ ] Charts show the Create/Verify phases; **Errors and logs** is collapsed, opens, and filters All / WARN+ / ERROR; "From:" names the log files.
- [ ] **Download CSV report** works (team feature; check it still works).
- [ ] Compare the numbers with the 10-16 direct CLI run (Create ≈ 10.6 IOPS, P50 ≈ 42 ms). Note them; don't expect a match.

## 6. Mock run

1. Step 2: choose **Mock driver**. The S3 endpoint check is not required, but the client machines must have passed.
2. Guided, 25 objects, 1MiB, **Review run**. The review must say "Mock run: no data is written to S3". **Start run**.

- [ ] The run completes; the workload shows **Mock driver** in the run strip and footer.
- [ ] After another run starts, or after a dashboard restart, **Run history** still shows the mock run as **completed** (not partial).
- [ ] Nothing is written to the bucket (check the bucket listing or the S3 metrics if you have access; otherwise mark **not checked**).
- [ ] With 2+ client machines configured, the per-machine table appears during the run. With one machine it is hidden by design.

## 7. Stop-run check

Stop from the UI is Omar's `feature/omar/gui-stop-run`; it is not on this branch. On this branch:

1. Start a longer mock run (Guided, 320 objects, 16MiB, 600 s).
2. Press Ctrl+C in the dashboard terminal while it is running.

- [ ] The dashboard exits, and `docker ps` shows no SPT containers left within about 30 s.
- [ ] Restart the dashboard. Open **Run history** first (before the live page). The run shows **interrupted**, not running, partial, or failed. The run page says "Run interrupted".
- [ ] The status does not change: check `/api/runs` and `/api/state` again about 30 s and 60 s after the restart, and after another run. It stays **interrupted** (SPT's exit code, usually 1, is kept as `returncode`).
- [ ] If Omar's branch is merged into the build under test, also check its **Stop** button and record the result separately.

## 8. Catalog and compare against `~/results`

1. **Results catalog**: the header shows `~/results`. Expect at least `mt-dashboard-20261021T170103-518aaa08`, `manual-real-20261016T220230Z-22d12f`, and the runs from steps 5–7.
2. Open **Details** for `manual-real-…`. Check the settings (write-verify, 25 × 1MiB), the phase totals, and the engine logs with level filters.
3. Select `manual-real-…` and `mt-dashboard-20261021T170103-…`, make `manual-real` the baseline, and **Compare**.
4. Compare the step 5 run against `manual-real` as well.
5. Change the threshold to 50 % and press **Apply**. The flags update and the header still reads "Threshold check, not statistical significance".

- [ ] Both historical runs are listed; any partial or malformed folder is listed with its reason, not hidden.
- [ ] Compare shows the overlaid series and the delta table; runs with different settings are marked **Not comparable** with the difference named.
- [ ] `curl -s "$B/api/catalog/run?id=manual-real-20261016T220230Z-22d12f" | grep -ciE 'secret|access'` prints 0. If the ID is nested, copy it from `/api/catalog` and URL-encode the `/`.
- [ ] Nothing in `~/results` changed: compare `ls -la --time-style=full-iso ~/results` before and after.

## 9. Clean up

- [ ] Ctrl+C the dashboard and close the tunnel.
- [ ] Keep the new result folders (they are evidence), or delete only the ones this test made.
- [ ] Paste the results table into the PR description.

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **`spt verify --rmi-registry-port`** — `spt verify` now takes `--rmi-registry-port`, with the default (1099), help text and range check of `spt run`'s flag. Verify's test engine serves its RMI registry on that port, and the port conflict check and bridge port mappings use it. With `--api-port` and `--rmi-registry-port`, a host can be verified beside a running engine that holds ports 1099 and 9999. `spt run` takes the same flag for every engine in a run.
- **`spt replay --rmi-registry-port`** — `spt replay` now takes `--rmi-registry-port`, with the default (1099), help text and range check of `spt run`'s flag. Every engine of the replay serves its RMI registry on that port: the local engine, a lone remote engine, or the entry and every worker of a distributed replay, which the entry reaches on it. A replay that shares hosts with another replay or an `spt run` needs its own `--api-port` and `--rmi-registry-port`, also when it has a local worker. Replay now refuses an `--api-port` equal to `--rmi-registry-port`, as `spt run` does, so it accepts `--api-port 1099` with another registry port. At the default port, every engine command is unchanged.
- **Mongoose versioning flag** — The engine accepts the Mongoose 3.x flag `--storage-net-http-versioning` as a synonym for `storage.object.versioning` with the S3 drivers. **Compatibility:** with `s3` and `s3-rdma`, a command that already passes it now checks the bucket's versioning on first use, enables it if it is off, leaves it on, and writes `key~versionId` names. With `s3-aws` it only splits `key~versionId` input names. With `s3-tables` it has no effect. See the [S3 driver's versioning guide](engine/extensions/storage-drivers/implementations/s3/README.md#434-mongoose-versioning-flag).

### Changed

- **RMI object range no longer published or checked** — `spt run`, `spt replay` and `spt verify` no longer publish or port-check the RMI object range: the engine never used it, since it serves its RMI services on the registry port. `--rmi-port-start` and `--rmi-port-count` are still accepted and recorded in `spt_run_params.json`, and have no effect. A port in use in 40000-40009 no longer fails a run's preflight, and a local worker or a bridge-mode verify container publishes only the API and RMI registry ports.
- **Partial-read performance wording** — The partial-read docs now describe the release performance check instead of a pending qualification: ordinary READ and WRITE runs showed no regression in the v5.15.2 check against v5.14.2, the last release before partial reads. No partial-read performance figures are published.
- **Known issue: default S3 auth version** — A direct-engine run (`java -jar`, `run.sh` or the image run by hand) with the `s3`, `s3-rdma` or `s3-tables` driver that passes no `--storage-auth-version` signs with V2 or V4 depending on the order in which the host lists the engine's `ext/` directory, so the same command can sign differently on two hosts. Pass `--storage-auth-version` explicitly: `4` for SigV4, or `2` for a legacy target that rejects SigV4. `spt run` always passes a version and is not affected. A later release makes V4 the single default.
- **Engine that never becomes ready** — When an engine never becomes ready, `spt run` and `spt verify` now show, after the wait failure and for each host whose engine API never answered, its container state (running, exited with its exit code, or missing) and its last 20 log lines, stdout and stderr together: in `spt run`'s API readiness failures, under the start failure of a remote worker or entry, and under `spt verify`'s `Ports Accessible` check. A local single-host `spt run` adds the container state to its startup diagnostics, which it already showed with the container's last 120 log lines, and now prints the failure line once, ahead of them. In those log lines, the values of `S3_ACCESS_KEY` and `S3_SECRET_KEY`, and each endpoint's host and host:port from `S3_ENDPOINT` and `S3_ENDPOINTS` (in any letter case), are masked as `***` wherever they appear as a whole token, as are credentials passed as key=value or flag arguments. A state or log that cannot be read is reported as such. Exit codes and the output of a successful wait are unchanged.
- **`spt run --rmi-registry-port`** — `spt run --rmi-registry-port` now works for remote and distributed runs; it previously applied only to local single-host runs. Every engine in the run serves its RMI registry on that port: a lone remote engine, and the entry and every worker of a distributed run. The entry reaches each worker on it, and the port conflict check covers it on every host. Because every engine serves both ports, `spt run`, `spt verify` and `spt replay` now refuse an `--api-port` equal to the registry port.
- **SSH keepalives** — Every SSH command and copy SPT runs now gives up about 60 seconds after its host stops answering, even where the SSH config sets a longer `ServerAliveInterval`, unless it runs over a shared connection opened outside SPT. A network gap longer than that, such as a VPN renegotiation, now ends the command instead of waiting for the connection to recover.
- **`SPT_JAVA_OPTS` with the bundle launchers** — `SPT_JAVA_OPTS` now adds to the bundle launchers' default JVM options, as it does in the SPT image, instead of replacing them. Results of `run.sh` and `run.bat` runs that set `SPT_JAVA_OPTS` can differ from 5.17.0. **Compatibility:** to pick another garbage collector, turn ZGC off first: `SPT_JAVA_OPTS="-XX:-UseZGC -XX:+UseG1GC"`; G1 tuning options without `-XX:+UseG1GC` are accepted and do nothing under ZGC. A maximum heap below 1 GiB needs a matching `-Xms`, because the default `-Xms1g` stays: `SPT_JAVA_OPTS="-Xms512m -Xmx512m"`. `-XX:MaxRAMPercentage` has no effect while the default `-Xmx4g` is on the line; give `-Xmx` instead. To run without the defaults, call `java <options> -jar spt.jar` directly.
- **Bucket in op traces of manifest-driven steps** — The `ItemPath` column of `op.trace.csv` now holds `/<bucket>/<key>` for steps that take their objects from a `bucket,key,size,version_id` manifest, as other steps' rows do: write-verify's verify and `--cleanup` steps, `read-verify`, `read --items-file` with a manifest, and a `mixed` DELETE from a manifest `--delete-items-file`. The path is the one the `s3` driver requests, with one slash between the bucket and the key whatever slashes the manifest's bucket and key carry. A CREATE that takes its objects from a manifest names its destination, the output path, not the manifest's bucket. These rows held the key alone, so a run over several buckets couldn't tell which bucket each object was in. Traces are off by default (engine `output.metrics.trace.persist`), and other outputs are unchanged.
- **`spt run --output-dir` deprecated** — `spt run --output-dir` (`-O`) never had an effect: results have always gone under `--results-dir` (default `./results`). It is now deprecated, hidden from help, and warns when used; pass `--results-dir` instead. `spt replay -O` is unchanged.

### Fixed

- **Early stop of a duration DELETE** — A duration DELETE stopped by its failed-object budget names the budget as the cause, instead of reporting that its inventory ran out and advising `--seed-objects`. A duration DELETE whose engine or step is stopped under it before the deadline now reports that it was stopped before the requested duration, not that its inventory ran out. Both kinds of stopped run still fail. A run that truly runs out of inventory before the deadline still fails with the exhaustion message and the `--seed-objects` advice.
- **Local workers and bridge-mode verify with an advertised hostname** — A local worker of a distributed run with a remote entry, and a remote `spt verify --network-mode bridge`, can now start their engines. A bridge-mode engine whose `java.rmi.server.hostname` is the host's own address bound its RMI services through that address, which from inside the container comes back from the bridge gateway, and the registry refused it (`Registry.rebind disallowed; origin /172.17.0.1 is non-local host`), so the engine exited. Engines now bind and unbind their RMI services through the local registry on the loopback address and still advertise the configured hostname to callers. Host-mode engines behave as before, unless a host firewall drops loopback traffic to the registry port, where an engine now waits at start instead of binding.
- **`spt run` and `spt replay` `--network-mode` check** — `spt run` and `spt replay` refuse a `--network-mode` other than exactly `host` or `bridge`, as `spt verify` does, with verify's message `network-mode must be 'bridge' or 'host', got: <value>` and exit status 1, before any port check, archive fetch, container or host connection. A typo such as `hots` used to start the engines in bridge mode without a warning. `host`, `bridge` and the default behave as before. **Compatibility:** the value is now case-sensitive and takes no surrounding spaces. `Host`, `HOST` and ` host` used to run in host mode and are now refused, as are an explicit empty value and a bad value given with `--generate-only`.
- **Port checker engine detection** — The CLI's port checker now recognises a running engine by its own API responses, not only by the Jetty error page: its `/metrics/json` probe accepts the current `metrics_schema` payload, and its `/run` probe accepts the engine's "Invalid run ID format" answer. It used to match a current engine only through the "Powered by Jetty" footer (and on Jetty 9.4 the servlet row) of the error page, so an engine on a newer Jetty, or one that hides its Jetty version there, would not have been recognised as SPT. Older engines are still detected.
- **`spt run --api-port` on distributed runs** — `spt run --api-port` now works for distributed runs; the engines previously started on 9999 and the run timed out waiting for the requested port. The entry and every worker now serve the requested port, and so does a distributed `spt replay`.
- **`spt run` and `spt replay` `--api-port` range** — `spt run` and `spt replay` refuse an `--api-port` that is not a whole number from 1 to 65535 before starting any engine, with `--api-port must be between 1 and 65535, got <value>` and exit 1; such a value used to start the engine and fail after the 60 s API wait. The value must be plain decimal digits, so a signed value such as `+20041` is refused too; a leading zero is accepted, and `09999` runs on 9999. `--generate-only` now checks the value as well.
- **Concurrent runs on one remote host** — Two runs that started the same role (engine, worker or entry) on the same remote host in the same second got the same container name, and the second `docker run` failed. Remote container names now carry the time to the nanosecond.
- **Runs on one remote engine** — An `spt run` or `spt replay` that runs on a single remote engine (one `--test-hosts` entry, or several where only the first connects and `--min-hosts` allows it) now stores its driver statistics in `spt_run_params.json`; 5.17.0 recorded a `driverStatsError` there instead. A standalone DELETE there, seeded or from `--items-file`, now exits 0 after a clean delete; from 5.15.0 it exited 1 with "metric contributor identity is unavailable before execution starts". A real failure there no longer carries that reason as an extra cause.
- **`spt verify --api-port`** — `spt verify` now starts its test engine on the `--api-port` port, and publishes that port in bridge mode, so verifying on a port other than 9999 can pass. Verify used to check a port its engine never served, and fail after 30 seconds with `Ports not accessible`. An `--api-port` outside 1-65535 is now rejected.
- **Concurrent `spt verify` runs on one host** — `spt verify` no longer stops another invocation's running verify container on the same host. Before it starts its own container, and again after it removes it, verify now removes only running verify containers that are at least 15 minutes old and that it did not start; it used to remove every running container labelled `spt.verify=true`. Verify containers now carry an `spt.verify.run` label with the id of the `spt verify` invocation that started them; a container from an older CLI, without that label, is judged by its age alone. Each removal's log line now gives the container's age.
- **`spt verify` port wait** — When the port wait times out, verify now names only the ports it last saw closed and lists any port it got no result for; when every port was seen open but never all in the same round, it says so. It used to report every port closed, including an RMI registry that was open.
- **`spt replay` exit status after a terminal failure** — `spt replay` now exits 1 when the engine ends its scenario FAILED, whichever step failed, and names the failed step and the engine's cause, as `spt run` does. A replay whose only step failed this way still left results for that step, so it printed `Results fetch encountered errors`, an empty phase table and `Create metrics unavailable (status: partial)`, and exited 0. A replay whose steps all finished still exits 0 when the results fetch is incomplete.
- **`spt replay` launch-script flags** — `spt replay` now warns once in its preflight about the `--storage-*` and `--load-*` engine flags on the archived launch script that it does not keep, naming them without their values, for example `replay does not keep these launch-script flags: --storage-targetBucket-provision, --storage-targetBucket-count`. The warning is also recorded in the `diagnostics` of `replay-metadata.json`. It used to drop them with no warning.
- **`spt replay` refusal of a JavaScript `storage.targetBucket`** — When an archived JavaScript scenario mentions `storage.targetBucket`, `spt replay` now says that covers a bucket count, a bucket list or the provisioning setting, and that you can replay a JSON archive or remove the setting from the archived scenario. It used to say "a bucket count or list", which was wrong for the `"provision": false` setting that `spt run` writes into most of its own scenarios. Replay still refuses these archives, with the same failure class.
- **SSH commands and copies that outlived their deadline** — A command or file copy over SSH now returns within a second of its deadline when a jump-host or helper process keeps its output open. On a ProxyJump route to a host that stopped answering, `spt run` and `spt verify` could wait until the connection itself timed out, which after a network loss can take many minutes, and a stalled `scp` could likewise wait past its deadline. A command that succeeded but left such a process behind now returns at most 1 second after it exits, and a remote log stream that ends cleanly no longer warns that it dropped and reconnects.
- **Large item files on remote hosts** — An item file from `--items-file`, `--read-items-file` or `--delete-items-file` no longer fails its copy to a remote host after 30 seconds, which stopped the run before it started whenever the list took longer than that to copy: the copy now runs as long as it needs, and `spt run` prints one line per host with the files' size before it starts. A stalled or stopped copy ends with a message that names the copy and host and says why, instead of `signal: killed`, and when the host still answers, the remote staging directory is then removed without a second `context deadline exceeded` error.
- **Failed starts with item files** — A start that fails while it copies item files to a remote host now rolls back once. It used to roll back twice, each time with its own 30-second limit, so a host that no longer answered could delay the error by a second pass, and each cleanup error was reported twice. A start with item files whose `mkdir` of the remote staging directory is cut off by its timeout or a cancel no longer leaves an empty `/tmp/spt-items-*` directory on the host: the rollback now removes it.
- **Failed remote starts that left a container or directories behind** — A remote engine start that fails after it has created something on a host now has it removed, by the start's rollback, or at the end of the run when the run goes on without that host. A worker or entry whose container started but failed its API readiness check used to keep running after `spt run` exited with the start error, and a worker that failed it in a run that went on with the other workers kept running after the run. With `SPT_JAVA_OPTS` set, a failed start could leave its `/tmp/spt-items-*` staging directory and its `/tmp/spt-diagnostics/<run>/<host>/<role>` directory on the host; both are now removed, and the empty `<run>/<host>` directories above it stay, as after a successful run. Three cases still leave something behind: a `docker run` cut off after Docker created the container, whose ID the CLI never got; a directory whose `mkdir` ran on the host but whose SSH command failed for another reason before the start's time limit, which the CLI treats as never created; and a host that no longer answers, where the removal fails and is reported.
- **Count-limited runs on several engines** — A count-limited run no longer leaves an engine without a count, which that engine read as no limit. When the count is at least the number of engines but the split left one of them at 0, the count is now split evenly: `--object-count 4` on three engines runs 2, 1 and 1 operations, where the third engine used to run without a limit until stopped. Splits that already gave every engine a count are unchanged. These count-limited runs now fail before the step starts, with exit 1 and a reason that names the fix:
  - A count below the number of engines, such as `--object-count 1` or `--seed-objects 1` on two `--test-hosts`. One engine used to run without a limit, and an `spt run list` exited 0 having listed a different number of objects than it asked for.
  - An `spt run tables` on two or more engines that provisions its table, or that runs the `compaction` test vector, whose poll is a step of one operation. These runs already hung, because the second engine ran its one-operation step without a limit. A `tps` run with `--no-provision` runs only for a duration and is unaffected, and a `catalog` run with `--no-provision` fails only when `--namespace-count` times `--tables-per-ns` is below the number of engines.
  - A count-based read from an items file with fewer items than engines. It used to hang.
  - A scenario run directly with a count below the number of engines, including one that used to finish with a different number of operations than it asked for. A weighted or pipeline step checks each sub-step's count and any items file the sub-step reads, and a mixed step the count and items file it runs with; a count or items file that no operation uses is split as before.

  Duration runs are unchanged: a `--duration` read from an items file with fewer items than engines still exits 0. A count-based read whose item input path lists no objects still hangs, as it did before.

- **Failed DELETE error** — The final `Error:` of a failed DELETE run states the failed step and the DELETE verdict once each, on one line when auto-results are on (the default). It could repeat the step's reason and the verdict after `run also failed:`, over two lines, when the CLI's status check saw the failure first.
- **Traced DELETE** — `spt run delete` with op tracing on (`output.metrics.trace.persist=true`, from `--engine-override` or `SPT_ENGINE_OVERRIDES`) now fails before it starts, saying tracing isn't supported for standalone DELETE. It failed with only "failed to start metadata-mode step slice 0", after a seeded DELETE had written its seed objects.
- **`--help` on the engine** — `--help` and `-h` on the engine, the image and `run.sh` now print the engine's options, with their value types and aliases, to standard output and exit 0, without starting anything or writing files. They used to start the default scenario, on the mock driver or on the target the other arguments named, and run until stopped.
- **Invalid engine arguments** — An engine argument that cannot be applied now stops startup with exit status 2 before any load: an unknown option such as `--dry-run`, an argument without `--`, a value of the wrong type or not a number, and an option the engine no longer takes. The engine used to log the error and run anyway with the settings applied before it, which could send load to a configured target. `--storage-driver-remote` is refused the same way, with an explanation: SPT has no remote driver services, so start each `--storage-driver-addrs` host as a worker engine (`run.sh --run-node=true`); the engine you start also runs its share of the load. The getting-started, recycling, item naming and S3 versioning docs used invalid flags and are corrected. **Compatibility:** a command with an invalid flag used to run, wrongly, and now fails with exit 2. This includes Mongoose 3.x options that SPT has no name for, such as `--load-limit-count` (SPT's is `--load-op-limit-count`) and `--storage-net-http-namespace`; `--storage-driver-remote`; `--load-op-recycle=true` (use `--load-op-recycle-mode=true`); and the old aliases `--load-circular` and `--load-generator-recycle-enabled`, whose target is not a setting. A worker started with `--restart unless-stopped` restarts in a loop on such a flag.
- **Boolean values other than true or false** — A boolean engine setting now takes only `true` or `false`, in any case. Any other value, such as `yes`, `1` or an empty value, is refused: on the command line with exit 2, in a scenario's `Load.config(...)` or `.append(...)` as a scenario failure, and as a string in a defaults file or in the defaults of a run request, which the node answers with 400 and a message naming the setting. Such a value used to read as false without a word. A boolean option given with no value, such as `--run-node`, still means true. **Compatibility:** a command, scenario or defaults file that set a boolean to anything else now fails. This includes `spt run --engine-override load.op.shuffle=no`: the CLI sends `yes`, `no`, `on` and `off` as text, so `yes` read as false, and the run now fails to start with `status 400` and that message; give `true` or `false`. Unquoted `yes`, `no`, `on` and `off` written in YAML the engine reads, such as an extension's defaults file, still read as booleans.
- **Unlimited steps on several engines** — An engine running a step with neither a count nor a time limit, such as an `spt run list` without `--duration`, no longer reports `test_state` 2 (Completed) in `/metrics/json`, `/metrics/cluster/json` and `/metrics/fleet/json` during a momentary gap between operations. Such a step reports 1 (Running) while it is active and 2 only once it has ended. A multi-host unlimited step can therefore no longer be ended by the first engine that goes idle; with `--auto-results=false` that could stop the other engines mid-step. Time- and count-limited steps are unchanged, and so is the `test_state` gauge on the Prometheus endpoint `/metrics`. **Compatibility:** a tool of your own that reads `test_state` from these JSON endpoints no longer sees 2 for an unlimited step while it runs; after the step ends, 2 comes from its retained terminal row for `api.linger.sec` (default 20 s), so with `--api-linger-sec=0` an unlimited step never reports 2.
- **Exit status after a start or scenario failure** — The engine now exits 1 when its node fails to start (`Run node failure`, for example when the API port is taken), when the scenario file is missing or cannot be read, when the scenario has a syntax or configuration error, and when the defaults fail to load. It used to log the error and exit 0. A run that ends normally still exits 0. The engine's exit statuses are now 0 for success, 1 for a run, start or scenario failure and 2 for an argument error; `--help` lists them. **Compatibility:** a script or check that took exit 0 as success now sees these failures. A node logs its scenario's failure and keeps running. Its `/status` reports a typed terminal failure as `FAILED` and, as before, any other scenario failure as `COMPLETED`, including a boolean refused in the scenario.
- **The `linux/arm64` image and arm64 JDKs** — The `linux/arm64` SPT image, and `run.sh` and `run.bat` with an arm64 JDK, now start: `-XX:UseAVX=2` is passed only to x86 JVMs. From 5.5.0 to 5.17.0 an arm64 JVM refused the option and exited.
- **Bundle launcher options** — `run.bat` now passes `-XX:+UseNUMA` and the RMI response timeout, as `run.sh` and the SPT image do. `run.sh` sets `GLIBC_TUNABLES=glibc.malloc.hugetlb=1` unless the caller has set `GLIBC_TUNABLES`, as the image does.
- **Signals sent to `run.sh`** — `run.sh` now replaces itself with the JVM, as the SPT image's entrypoint does, so a signal sent to `run.sh` stops the engine. **Compatibility:** the process now shows as `java ... spt.jar`; match `spt.jar`, not `run.sh`, to find it.
- **The bundle's Docker Compose example** — The `spt-weighted` service of the engine bundle's Docker Compose example no longer selects G1 on top of the image's ZGC, which stopped its JVM with `Multiple garbage collectors selected`.
- **`netty-mock` driver under load steps** — The netty-mock driver no longer overflows the stack under load steps: time-limited steps end on time, and count-limited steps no longer hang.
- **Driver close on an interrupted thread** — A storage driver closed on an interrupted thread now completes its close, instead of leaving its event loops or I/O workers running; from 5.15.0 such a close threw an `InterruptedException` partway through. The close takes no longer than a close on an uninterrupted thread, and the thread stays interrupted.
- **A `mixed` step of only writes and deletes** — A `mixed` step with GET and STAT weights of 0 no longer needs seed items to start. Without an items file it failed at start with `Seed items list must be non-null and non-empty`, from 5.8.0. A mix with GET or STAT still needs at least one seed item on each engine.
- **Warnings in `errors.log`** — Warnings in `errors.log` are no longer dropped when debug lines reach the rate limit. Debug and info messages keep their own limit of 1,000 messages per 10 s, so `errors.log` can grow up to twice as fast when both limits are reached.
- **Paged path item input on the `s3` driver** — A path item input over more than one listing page no longer stops after the first page or repeats it, on the `s3` driver and on the drivers that inherit its listing, such as `s3-rdma`. Each next page started after the last item's whole name, `<bucket>/<key>`, instead of its key, so a copy-mode step could copy only the first page and exit 0, or list the first page again without end. Only an `item-input-path` engine override or a direct scenario reaches it.
- **`s3-aws` copy mode** — An `s3-aws` copy-mode step whose item input path is a bucket, such as `/src`, now copies each listed object, instead of failing every copy with not found. Each copy read one object named after the source bucket, in the driver's own bucket, instead of the listed object in the source bucket. A copy keeps its key in the target bucket, where the `s3` driver puts it under `<source bucket>/`; see [Copy Mode](engine/core/spt-base/doc/design/modes/copy_mode/README.md#231-s3-objects-copying). A source path that names an object, such as `/src/k0`, still copies that one object to every item. Only an `item-input-path` engine override or a direct scenario reaches it.
- **A false error line for every multipart write** — The HTTP-based S3 drivers no longer log "Request is finished (…) after the response is started (…)" in `errors.log` for each multipart upload and each retried part. Successful requests are timed as before; only failed ones change. In `op.trace.csv`, a multipart completion, abort or part retry that was never sent, for example because its connection failed, now has a blank start and latency and an epoch-sized duration, like any other request that never started, where it carried the earlier exchange's start and latency; it now also logs the DEBUG "Response is finished while not started" line in `errors.log` that other unsent requests log. In the multipart log (`parts.upload.csv`, fetched as `<step>.multipart.csv`), a completion or abort that had a connection but no response now has a latency of 0 instead of a stale value, usually negative. That log has no row for an unsent completion or abort, or for a failed part attempt, so those show only in `op.trace.csv`.
- **`s3-aws` multipart part retries** — A failed part of an `s3-aws` multipart upload was never really retried: no retry was ever sent. Before 5.15.0 the retry was dropped, with an ERROR "I/O worker failure". From 5.15.0 each of its 3 retries failed at once with `FAIL_UNKNOWN`, the step logged an ERROR "I/O operation invocation failure", and the upload was aborted. The retries are now sent. An upload whose part fails once now completes, and that part gives one failure row instead of four. A part that keeps failing now sends its 3 retries to the target, so an upload against a failing target takes longer to abort.
- **S3 drivers kept in memory after they close** — The `s3` and `s3-rdma` drivers and the `s3-tables` control plane no longer stay in memory after they close when they signed requests on a long-lived engine thread, such as the scenario thread that checks target buckets. With the image's default JVM settings, each such driver kept about 24 MB, and a step can keep more than one: one per distinct storage configuration, plus any driver that listed input items or discovered prefixes. Long multi-step scenarios and engines reused for many runs could fill the default 4 GB heap after about 170 kept drivers, which is fewer steps when each step keeps several.
- **SigV4 signing with more than one secret** — S3 requests signed with SigV4 now use the right key when one driver uses more than one secret, for example several entries in `storage.auth.file`. Each signing thread used the first secret's key that day for every secret, so a target that checks signatures rejected the other secrets' requests, and a target-bucket check with another credential reported the bucket as not accessible. For extension authors: a function installed in `signingKeyCreateFunctionCache` is still memoized by date in `signingKeyCache`, as before, and `signingKeyCache` now holds only such functions' keys, not those of SPT's own signing function; a key put straight into `signingKeyCache` is used only by such a function, no longer by SPT's own.
- **Multipart counts when a part fails** — A multipart upload or multi-range read is now counted once, by its own outcome. A failed part attempt no longer takes a `load.op.limit.count` slot or counts as a failure, so a count-limited step no longer ends before its last uploads complete. A failed upload now counts as one failure, whether its initiation, a part or its completion failed. An upload whose initiation failed used to count nothing, so a count-limited step ran to its time limit. The `load.op.limit.fail` count and rate limits now count a failed upload once, not each failed part attempt. Failed part attempts still show in the errors log, the operation trace and `operation.lifecycle.csv`. Bytes that a failed or retried upload's successful parts sent still count in the byte totals. At concurrency above 1, a step whose upload was aborted can still wait up to `load.op.wait.limit` (30 s) at its end for that upload's other parts, as before.
- **Multipart writes with `load.op.retry=true`** — A multipart write with `load.op.retry=true` no longer stalls after a part fails. A failed upload is now retried as a whole, from a new initiation with all its parts, as documented. The part used to be retried as an extra part of its upload, which then never completed and held the step until its stop. An upload aborted after a part ran out of retries now ends with the status of the first part that did, including in the operation trace, whether the abort is answered, refused or cannot connect, so it is retried only when the part's failure is transient: a part the target refuses for good, for example with 403, fails its upload once. It used to end with `FAIL_UNKNOWN` after an abort the target answered (on `s3`, also one it refused), and otherwise with the abort's own failure, such as `FAIL_IO`. On `s3`, a failed initiation now keeps its own status, so a throttled one (503) is retried too; on a connection's first initiation it used to be reported as `RESP_FAIL_NOT_FOUND`, which is never retried.
- **Credentials in the engine's `cli.args.log`** — The engine's `cli.args.log` now masks as `***` the values of `--storage-auth-uid`, `--storage-auth-secret` and `--storage-auth-token` (and its alias `--atmos-subtenant`), and of the `Authorization`, `Proxy-Authorization`, `Cookie`, `x-amz-security-token` and `X-Auth-Token` headers set with `--storage-net-http-headers-<name>`, matched in any case. The file used to hold every argument as given. A node also serves that file at `GET /logs/<step_id>/Cli`. Argument names, their order and every other value are unchanged, an empty value or an argument without `=` is logged as given, and the run still uses the real values. Values in `storage.net.http.uri.args` are not masked. Only arguments on the engine's own command line reach this file (the jar, `run.sh`, `docker run <image> ...`); `spt run` passes no credentials there. The step `config.yaml` now also masks those five header values under `storage.net.http.headers`, wherever they were set (command line, scenario `config` block, posted defaults or a defaults file) and for every run, `spt run` included; it already masked the credentials.
- **Credentials in a node's `GET /config`** — A node's `GET /config` now shows the storage credentials (`storage.auth.uid`, `.secret` and `.token`) and the values of the `Authorization`, `Proxy-Authorization`, `Cookie`, `x-amz-security-token` and `X-Auth-Token` headers under `storage.net.http.headers` as `***`, in YAML and JSON, as the step `config.yaml` does. It used to return them in clear. An unset or empty value and every other value are unchanged, the node keeps the real values, and `GET /config/schema` is unchanged. SPT itself does not read `GET /config`. A client that posts a `GET /config` response back to `POST /run` as `defaults` now sends `***` as those credentials and headers, so it must set them itself.
- **Completion of multi-host steps with several operation types or differing limits** — On more than one host, the CLI now shows a step with several operation types as completed, and prints "Test COMPLETED", only once every operation type has completed on at least one engine. It used to do so once any one operation type had. Every `spt run` mixed workload on several hosts is such a step. Separately, when the engines of one step report different limit types (for example one engine an operation count and another no limit), the CLI now treats the step as having no limit and waits until every engine has completed it. It used to take the limit of whichever engine it read first, so it could show the step as completed while an engine was still running. Only weighted, pipeline and appended mixed scenarios run straight on the engines can report different limits; the scenarios `spt run` generates never do.
- **Shipped example scenarios** — The example scenarios shipped in the image under `/opt/spt/scenarios/js`: `types/weighted.js`, used by the Compose example's `spt-weighted` service, `rampup.js`, `parameterized/s3-multiuser.js`, `system/circular_append_test.js`, `system/circular_read_limit_by_time_test.js`, `system/read_verification_after_circular_update_test.js`, `system/weighted_load_test.js` and `system/tls_read_using_input_file_test.js` no longer stop on an invalid configuration path or a syntax error. `backward_compatibility.js` no longer stops on an invalid configuration path either: it now uses current configuration names, since the Mongoose 3/4 names it used are not aliased.
- **`s3-aws` UPDATE** — An `s3-aws` UPDATE writes the object again, as before 5.9.0, instead of failing every operation with a copy from a missing object. Each UPDATE of an item named `/<bucket>/<key>`, as an `s3-aws` write's items file records it, or of an object listed from an item input path, was sent as a copy from an object named after the bucket in the driver's own bucket, and failed with not found. It is now one PUT of the whole object with the item's current content, counted as the item's size, which matches the `s3` driver's default UPDATE. Configured ranges (`item.data.ranges.random` or `item.data.ranges.fixed`) are ignored, as they were before 5.9.0. No `spt run` workload runs an UPDATE; an archived `UpdateLoad` in `spt replay`, an engine `--load-op-type=update` or a direct scenario reaches it.

## [5.17.0] - 2026-11-11

### Added

- **Per-bucket metrics** — A step with more than one target bucket now writes `metrics.bucket.total.csv` and `metrics.bucket.steady.total.csv`, with counts, rates and timings per bucket and operation type. `--bucket-metrics=false` (engine `output.metrics.bucket.enabled`) turns them off.
- **Driver statistics in the metrics JSON** — Rows of `/metrics/json` and `/metrics/fleet/json` gain an optional `driver_stats` object with the counters and gauges a storage driver reports about itself, summed over nodes in fleet rows (metrics schema 5). The `s3-rdma` driver reports its RDMA and HTTP data path counts, buffer registrations and pool levels. See `cli/docs/S3_RDMA.md`.
- **S3-RDMA data path in the run summary** — For each step in which the `s3-rdma` driver sent payload requests, the run summary shows the transfers made over RDMA and those sent over HTTP, and warns when a step moved no payload over RDMA. Each step's final statistics are stored in `spt_run_params.json` under `driverStats`.
- **Driver statistics on the Prometheus endpoint** — `/metrics` on the entry node now exports the fleet totals of each running step's driver statistics, as `spt_driver_<name>_total` counters and `spt_driver_<name>` gauges.
- **Bucket sets** — `spt run --bucket-count N` spreads the objects a run creates over `N` buckets named `<bucket>-0` to `<bucket>-<N-1>`, and `--buckets`, `--buckets-file` or `S3_BUCKETS` names the buckets instead. Bucket sets work with `write`, `write-verify`, seeded `read` and `delete`, and `mixed` on the `s3`, `s3-aws` and `s3-rdma` drivers; other workloads reject a set of more than one bucket. See [Target Buckets](cli/docs/TARGET_BUCKETS.md#bucket-sets).
- **Steady-state metrics** — Each step is now also measured over its steady-state window, from the end of warm-up to just before the drain, in `metrics.steady.total.csv` with the window's start, turnovers, trend, variation and status; existing outputs are unchanged. The run summary shows IOPS Steady and Bandwidth Steady beside the averages, and the JSON metrics API (`metrics_schema` 5) adds a `steady` object. A distributed run needs the same engine version on every node.
- **NIC preview in `spt verify`** — `spt verify --interfaces auto`, or a list of interfaces, addresses or RDMA devices, previews on each host the NICs and Paths a run would use for its S3 endpoints, and fails a host whose plan has a failure. See [`spt verify`](cli/docs/SPT_SYNTAX.md#nics-and-the-nic-plan).
- **NIC list in `spt verify`** — With an SPT 5.17 or later image, each host's verify report now ends with its NICs: state, address, speed, MTU, NUMA node and RDMA device. The image adds `/opt/spt/spt-nic-plan`, which prints a host's NIC inventory and plan as JSON.
- **NIC sets in the engine** — The `s3` driver can now spread its requests over chosen client NICs, set with the engine setting `storage.net.interfaces` (`auto` or a list). Each engine plans the NIC set on its own host, binds its connections to each NIC, splits the concurrency limit across the NICs, and skips a failing NIC while others are working. Control requests, such as bucket checks and multipart aborts, keep the kernel's route.
- **Per-path NIC evidence** — A step whose `s3` driver has a NIC set now writes `nic.paths.csv`, with each NIC and endpoint pair's request attempts, how they ended, and payload bytes. The CLI fetches the files and names them in the run summary. See the engine's [per-path NIC evidence](engine/core/spt-base/README.md#per-path-nic-evidence).
- **NIC sets in `spt run` and `spt replay`** — `spt run --interfaces auto`, or a list of interfaces, addresses or RDMA devices (also `SPT_INTERFACES`), gives every engine of the run a NIC set for the `s3` and `s3-rdma` drivers, and `spt replay --interfaces` does the same. Each step also writes `nic.set.json`, and the run summary shows the interfaces and event loops in use. See [Network Interfaces](cli/docs/NETWORK_INTERFACES.md).
- **Admission barrier for NIC sets** — A step in which any engine has a NIC set now starts operations on every engine at once, after all have prepared, and fails if one engine fails or `load.step.preparation.timeout` (default `10m`) passes. Its duration and rates count from that moment, so they exclude preparation.
- **Host overrides for NIC sets** — `spt run`, `spt replay` and `spt verify` now take `--host-interfaces <host>=<value>` (also `SPT_HOST_INTERFACES`), so hosts with different NICs can each have their own value, or `none` to turn NICs off on one host.
- **NIC sets for the `s3-rdma` driver in the engine** — With `storage.net.interfaces`, the `s3-rdma` driver now opens one RDMA context per NIC on that NIC's device, and each operation keeps its requests on one NIC. Each NIC has its own buffer pool, preparation and copy threads, and by default `max(4, hardware threads / 48)` event loops, so a slow NIC delays only itself. See [S3-RDMA with a NIC set](cli/docs/S3_RDMA.md#nic-sets).
- **RDMA pool check and pre-warm with a NIC set** — A step whose `s3-rdma` driver has a NIC set now checks, before any object operation, that each NIC's buffer pool fits in direct memory, then pre-warms the pools so the load starts warm. The new engine setting `storage.rdma.poolShortfall` decides whether a short pool fails the step (`fail`, the default) or only warns (`warn`).
- **RDMA columns in `nic.paths.csv`** — The per-path CSV moves to schema 2, which adds `rdma_succeeded` and `http_succeeded` columns and per-NIC buffer pool counters for the `s3-rdma` driver. A reader that goes by column name reads both versions.
- **`spt run --interfaces` for the `s3-rdma` driver, and sized direct memory** — `--interfaces` and `--host-interfaces` now work with `--s3-driver rdma`, and `--rdma-pool-shortfall fail|warn` sets the pool check. With a NIC set, the CLI sizes each container's `-XX:MaxDirectMemorySize` from that host's NIC plan when `SPT_JAVA_OPTS` doesn't set it, and `spt verify --use-rdma` previews the sizing. See [S3-RDMA with a NIC set](cli/docs/S3_RDMA.md#nic-sets).
- **Event loops in every run's results** — Every engine now writes `event.loops.json` for each step, with each driver's event loops and what sized them, and the run summary shows an `Event loops` line. See [Event loops](cli/docs/EVENT_LOOPS.md).

### Changed

- **Fewer default event loops on large clients** — With `storage.driver.threads` unset, Netty-based drivers on the default route, including `s3` and `s3-rdma` without a NIC set, now run `max(4, hardware threads / 24)` event loops instead of `max(4, hardware threads / 8)`; clients with fewer than 40 hardware threads keep 4. On a client with 192 hardware threads, 10 KiB GETs carried 16% to 29% more for about half the client CPU per operation. Set `storage.driver.threads` for the earlier count; see [Event loops](cli/docs/EVENT_LOOPS.md#default).
- **Huge pages in the image** — The SPT image now sets `GLIBC_TUNABLES=glibc.malloc.hugetlb=1`, so malloc asks for huge pages where the host's transparent huge pages mode is `madvise`. This roughly halved RDMA pool pre-warm time, with no change measured under load.
- **Driver extension methods** — `NettyStorageDriverBase.releaseTransport(Channel)`, `CoopStorageDriverBase.hasAvailableDispatchCapacity()` and `CoopStorageDriverBase.dispatchAttemptLimit(int)` are now protected. A driver extension that declares a method with one of these signatures now overrides it, or no longer compiles if it declares it with weaker access; rename that method.
- **Dependency updates** — The CLI is built with Go 1.27.4 (previously 1.25.14), which building it from source now needs, and the engine image uses Eclipse Temurin JRE 25.0.5. Updated Netty to 4.1.140, AWS SDK/CRT to 2.57.11/0.50.0, Jackson to 2.22.4, and several Go modules.

### Fixed

- **Spurious ERROR at the end of every `mixed` run** — A `mixed` step that stops normally no longer logs `MixedLoadGenerator error in doWork` with `java.io.EOFException`. It didn't affect results.
- **Outdated libraries inside extension JARs** — The `mixed`, `pipeline` and `weighted` extensions no longer bundle outdated copies of Jackson, SnakeYAML and Log4j that image scanners flagged; they use the engine's. The `s3-tables` extension now bundles Parquet 1.18.1, fixing CVE-2025-30065.
- **`NullPointerException` in every step's errors log** — A step's errors log no longer records a `NullPointerException` from closing the driver that checks target buckets. Present since 5.16.2.
- **`spt run delete --cleanup` with objects left to clean** — A seeded DELETE whose cleanup removed objects now exits by its measured result. It used to exit 1 with "unexpected current-run DELETE step" or "missing authoritative terminal DELETE step".
- **Ordinary runs that failed outright** — A `read`, `write`, `mixed` or `tables` run now exits 1 if every request of a phase failed, instead of 0, and a `list` run that found no objects and had failed requests also exits 1. The summary has a new `Failed` column. Partial failures still exit 0.
- **Steady windows across an outage** — The summary now warns when a `valid` steady window's rate didn't hold (a trend of -50% or lower, or a CV of 0.5 or higher). The window stays `valid`, and the cutoffs are uncalibrated.
- **A `written.csv` manifest as `--read-items-file`** — A `mixed` run reading from a `written.csv` manifest now reads each row's object. Every READ used to fail with 404, because the row's bucket was lost.
- **Bad items input and failed starts** — A blank or invalid line in an items file, an unreadable or empty items file, a listing that fails part-way, or an engine that fails to start now fails the run with exit 1 and a named reason, instead of passing with a wrong count. A scenario run with `--run-scenario` now exits 1 on any terminal failure.
- **`spt replay` exit code** — A replay whose scenario fails after its first step now exits 1 instead of 0.
- **`spt run delete --duration`** — A duration DELETE now runs for its requested duration. It used to stop almost at once and exit 1, with most of its inventory unattempted.
- **`--delete-items-file` in a distributed `mixed` run** — Each engine now deletes its own share of the file's rows. Every engine used to delete every row, which multiplied the reported DELETE count and rate by the number of engines.
- **Large `spt run delete` runs** — A DELETE whose step takes a moment to finish no longer exits 1 with "engine terminal state is RUNNING" after deleting everything. A failed DELETE step's error now leads with the step's own reason.
- **`spt run read-verify` on an empty bucket** — With no `--prefix` and more than one thread, `read-verify` on an empty bucket no longer hangs. It exits 1 with "selected zero objects", or 0 with `--allow-empty-selection`.
- **`spt run delete` across buckets** — A DELETE with `--delete-batch-size=1` over several buckets now works on all of them at once, instead of one bucket after another.
- **`spt run delete --validate-inventory`** — A count-based DELETE with `--validate-inventory` now runs its post-delete verification and finishes, instead of exiting 1 after correct requests.
- **`spt run delete --label`** — A standalone DELETE with `--label` no longer exits 1 with "planned DELETE step identity ... has no stable ordinal" after correct requests.
- **`spt run delete --items-file`** — A DELETE from an items file now exits by its result. It used to exit 1 with "failed to retain DELETE frozen selection evidence" after correct requests.
- **A multipart part that loses its connection before any response** — When a multipart part lost its connection before any response and its retry succeeded, the HTTP-based S3 drivers never completed or aborted the upload, which could stall the step until its time limit. The upload now completes after the retry.
- **A multipart completion or abort that cannot connect** — A multipart upload whose completion request cannot connect is now aborted, instead of staying open on the target with its parts. The operation still fails, and an integrity run's `multipart.lifecycle.csv` now records such uploads.
- **A multi-range read whose range runs out of retries** — When one range of a multi-range read failed on every retry, the read could be counted as a success or not at all, and could stop the driver sending requests. It now finishes its other ranges and counts as failed.
- **Object names that start with the bucket name** — The HTTP-based S3 drivers treated an object name that merely started with the bucket's name as already holding its bucket, so `--bucket abc --prefix abcdef/` wrote into bucket `abcdef` and `--prefix abc/` lost the prefix, with exit 0. A name now holds its bucket only when the match ends at a `/`. **Compatibility:** objects that earlier versions wrote this way stay where they were written, and items files and manifests from those runs still name the old keys.
- **Rate-limited runs start paced** — With `load.op.limit.rate` set, each node sent up to one batch at full speed before pacing began, so short runs reported rates well above the limit. Each node now paces from its first operation.
- **Mixed cleanup on several workers** — `spt run mixed --cleanup` on more than one worker left objects behind, because cleanup read only the entry engine's list. Cleanup now collects every worker's list first, and the run's `put-remaining.csv` covers every worker.
- **Collecting a worker's item list survives a failed read** — A failed read while collecting a worker's item list is now retried up to 3 times. A list that still can't be read is kept on that worker with an error, instead of being deleted.
- **The `dummy-mock` driver stops like the real drivers** — The `dummy-mock` driver now refuses operations once a stopping step closes admission, so no operation completes after the step's outputs have closed.
- **A step's final values come from its own last read** — A metrics read still in progress as a step ended, such as a CLI poll or a Prometheus scrape, could replace the step's final totals with earlier counts. Final values now always come from the step's own last read.
- **Endpoint slicing when the node count does not divide the endpoint count** — With `--slice-endpoints`, when the node count did not divide the endpoint count, the later nodes targeted every endpoint instead of their own group. Each node now targets the group the log lists for it.
- **RDMA readiness in `spt verify` covers every device** — `spt verify --use-rdma` now checks the port of every RDMA device in the container, not just the first, and warns about each inactive one.
- **A JavaScript array in a scenario step's configuration** — A list setting written as a JavaScript array in a scenario's `.config(...)` or `.append(...)` now works like the same comma-separated string, instead of failing with `Scenario syntax error`. Three shipped example scenarios that could not run are corrected.
- **Pipeline steps hand over every item** — In a `PipelineLoad` step, a sub-step after the first could end before processing every result of the sub-step before it, and the handoff could stall the step past its time limit. Each sub-step now waits for and processes every result, so `spt run mock --cleanup` deletes while it creates and its create phase can run about 10% slower.
- **In-flight counts of `mixed` steps** — A `mixed` operation that finished before the step had counted it as sent stayed counted as in flight, so `ConcurrencyCurr` and `ConcurrencyMean` could read too high. Requests, rates and latencies were not affected.

## [5.16.2] - 2026-10-22

### Added

- **Concurrent local runs** — Added `spt run --rmi-registry-port` (default 1099) to set the local engine's RMI registry port, so several `spt run` instances can run on one host at once, each with its own `--api-port` and registry port. Before, a second instance failed to start with `Port already in use: 1099`. Remote and distributed runs keep port 1099 and reject any other value.

### Changed

- **Target-bucket provisioning** — Before each phase of an `s3`, `s3-rdma` or `s3-aws` workload starts, the engine on the entry node now checks the phase's target buckets: the bucket it writes to, the bucket it reads or lists, and every bucket its items file names. It creates a missing bucket only for the phase that writes the run's objects (the write or seed phase), and only after the target answers 404. `spt run --provision-target-bucket=false` (engine `storage.targetBucket.provision`, default `true`) still checks every bucket but never creates one. A bucket that is missing or not accessible ends the run before any object operation, with the phase and the cause in the console and exit code 1. Before, each worker checked and created the bucket on the first object it wrote, distributed workers could race to create it, and a failure only failed operations. Existing buckets are used as they are; SPT never clears, deletes or reconfigures a bucket. See [Target Buckets](cli/docs/TARGET_BUCKETS.md).
- **S3 region** — The `s3-aws` driver now picks its region in the same order as the other S3 drivers: `--region` or `storage.region`, then the region of a standard AWS endpoint (`s3.<region>.amazonaws.com`), then `us-east-1`. Before, it used `eu-west-2` when no region was set. `spt run` now also reads `S3_REGION` when `--region` is not given. A CreateBucket request to a standard AWS endpoint outside `us-east-1` carries the region's location constraint.
- **Bucket names in custom scenarios** — A per-operation expression (`${...}`, `#{...}`) in the bucket segment of `item.output.path` now fails the step at start with a configuration error, because the bucket must be known before the phase starts; keep such expressions after the bucket, as in `/bucket/${...}`. An initialization expression (`%{...}`) in the bucket segment, including the engine's default timestamped output path, is now evaluated once by the entry node, so every node of a distributed run writes to the same bucket instead of one per node.
- **`--create-prefix` deprecated** — The flag never had an effect. It is now hidden, prints a deprecation warning, and will be removed in the next minor release.
- **S3-AWS workers** — With `storage.driver.threads` unset, the `s3-aws` driver now runs one I/O worker per concurrency permit instead of at most one per hardware thread. Every request blocks its worker, so a run with `--threads` above the node's CPU count previously kept only as many requests in flight as the node had CPUs and queued the rest behind busy workers; it now keeps the configured number in flight. A worker blocked in a request no longer takes queued permits with it: new operations go to idle workers first.

### Fixed

- **Bucket checks taken for a missing bucket** — The `s3` and `s3-rdma` drivers treated any failed bucket check, an authorization failure (401, 403) or a server error included, as a missing bucket and tried to create it. Only a 404 now counts as missing; any other answer is reported as the bucket being inaccessible, and no create request is sent. Worker logs no longer print the response body of a failed bucket check.
- **Runs that fail during setup** — A run that fails before any phase slice starts now ends with an engine `FAILED` status that carries the step, the category and the message, and `spt run` exits 1 for every workload. The console shows that cause rather than a missing-metrics error, and a distributed run ends instead of waiting for metrics that never come.
- **S3-AWS lost requests** — A request the AWS client lost with no connection (so that `storage.crt.socketTimeoutMs` could not see it) stayed in flight for the rest of the run and, through the worker it blocked, stalled every permit queued behind it. The driver now puts a deadline on each request with `storage.crt.requestTimeoutMs` (default 300000, `0` for no bound), from the moment it makes it until the response has been read in full, a GET's body included: a request that outlives it is cancelled and fails as a timeout, counted and logged like any other.
- **S3-AWS stop with a lost request** — A step that stopped with such a request in flight waited for the whole of `load.op.wait.limit`, could not be stopped within the engine's budget, and ended with the operation still active. Once the step has closed admission, the driver now gives the requests in flight `storage.crt.stopTimeoutMs` (default 5000, half of the engine's budget for stopping a run; `0` to turn off) and then cancels them, a GET still reading its body and a request made after the grace included, so they end as `INTERRUPTED` and the stop completes within the budget. A driver stopped and started again gets a fresh grace for each stop.

## [5.16.1] - 2026-10-19

### Fixed

- **S3-AWS stalled requests** — The `s3-aws` driver now applies `storage.crt.connectionTimeoutMs` (default 5000) and `storage.crt.socketTimeoutMs` (default 30000) to all of its S3 clients. Before, a request that stopped making progress stayed in flight indefinitely and could keep the step from stopping. Now such a request fails as a timeout once its connection has moved no data for `socketTimeoutMs`, possibly after the AWS client retries it. `0` turns this off; any other value must be at least 2000. Requests still in flight when the driver stops are now cancelled.
- **S3-AWS small PUT failures** — `s3-aws` PUTs of 8 KiB or less no longer fail with "Unexpected read size" when the object's data runs past the end of the generated data buffer (`item.data.input.layer.size`, default 4MB) and continues from its start. With the default `item.data.dedupable: true`, this failed about one 4 KiB PUT in 1,000 before sending it.

## [5.16.0] - 2026-10-13

### Added

- **S3 region selection** — Added `spt run --region` and engine YAML `storage.region` support to explicitly select the AWS region for S3 requests. Omitting the setting preserves each driver’s existing region selection.
- **S3-RDMA data-path summary** — The `s3-rdma` driver now logs one end-of-step summary in `messages.log` showing how many data operations actually moved over RDMA, were declined by the server, failed, or used HTTP (RDMA unavailable, below threshold, oversize, ineligible, or preparation fallback).
- **Automatic RDMA local address** — When `--rdma-local-ip` is not set and `--rdma-device` is `auto`, each worker now selects the local address it routes toward the first S3 endpoint for RoCE GID selection, so distributed runs no longer need one shared address. Leave the flag unset for multi-host runs; a named `--rdma-device` keeps device selection authoritative.
- **S3-RDMA payload copy threads** — Added `spt run --rdma-copy-threads` (env `RDMA_COPY_THREADS`, engine `storage.rdma.copyThreads`) to copy each RDMA PUT payload of 2 MiB or more into its buffer on several threads instead of on the driver's dispatcher thread alone. The default of 1 keeps the previous behavior.
- **Legacy RDMA byte-count option** — Added `spt run --rdma-allow-missing-bytes-header` (env `RDMA_ALLOW_MISSING_BYTES_HEADER`, engine `storage.rdma.allowMissingBytesHeader`) for servers that report RDMA GET success without `x-amz-rdma-bytes-transferred`; it counts the requested size instead of failing the read.
- **S3 endpoint selection** — Added opt-in `--endpoint-selection` for the Netty S3 driver to choose the connect destination of every request attempt: `round-robin` over the `--endpoints` addresses, or `per-request-dns`, a fresh DNS lookup of the endpoint hostname. The default mode is unchanged; see `cli/docs/ENDPOINT_SELECTION.md`.

### Changed

- **S3-RDMA buffer reuse** — The `s3-rdma` driver now keeps registered buffers and reuses them across operations instead of allocating, zero-filling, and registering a new buffer for every PUT and GET, which limited RDMA throughput. A buffer is reused only after the server's final response to its request, and a buffer used by an integrity-verified GET only after that GET verified; buffers of requests that time out, lose their connection, or fail are deregistered. Pooled buffers stay within half of the JVM's direct-memory limit and are released when a per-operation buffer would otherwise not fit; transfers above 1 GiB are not pooled. `spt run --rdma-buffer-pool=false` (env `RDMA_BUFFER_POOL`, engine `storage.rdma.bufferPool`) restores per-operation buffers. The RDMA data-path summary in `messages.log` adds pool counters.
- **Remote entry-node log relay** — Entry-node output now arrives over one `docker logs --follow` SSH session instead of a new SSH session twice a second, reducing CPU load on the entry host. The console still shows only the engine's stdout; its stderr (such as JVM and logging warnings) is kept only in the CLI log, and only with `--log-level debug`.
- **Dependency updates** — The CLI is now built with Go 1.25.14 (previously 1.25.10). Updated Netty to 4.1.139 (netty-tcnative 2.0.84), AWS SDK/CRT to 2.55.14/0.48.5, Log4j to 2.26.1, Jackson to 2.22.3, Jetty to 9.4.58, Commons Codec to 1.23.0, Commons Lang to 3.21.0, Guava to 33.7.2, Avro to 1.11.5, Disruptor to 3.4.4, SLF4J to 1.7.36, Bouncy Castle TLS to 1.86.2, and Docker Go connections to 0.8.2.

### Fixed

- **S3-RDMA reply validation** — RDMA PUT and GET now honor the server's `x-amz-rdma-reply` and `x-amz-rdma-bytes-transferred` headers. A server that declines RDMA (HTTP 200 with reply 501, or no reply) previously produced "successful" writes counted at full object size although nothing was stored; declined PUTs now fail. Declined GETs are counted once over HTTP when `--rdma-fallback` is enabled and fail otherwise. GET bytes come from the server's reported transfer size, and a success without that header, a byte-count mismatch, or any HTTP body (including chunked bodies) on an RDMA success fails as corrupt.
- **S3-RDMA multipart uploads** — The multipart initiate request no longer allocates, copies, and registers the entire object; only upload parts use RDMA, each with a part-sized buffer. Parts that fail RDMA preparation or time out now settle their upload's part accounting exactly once, so retries and abort proceed.
- **S3-RDMA ranged reads** — Reads with fixed or random byte ranges now use HTTP instead of an RDMA buffer sized for the whole object, which previously miscounted or failed valid range responses.
- **S3-RDMA memory exhaustion** — An RDMA buffer allocation that runs out of JVM direct memory, or a PUT payload copy that runs out of memory, now fails the operation, or sends it over HTTP with `--rdma-fallback`, instead of escaping the driver and stopping its dispatch of further operations.
- **S3-RDMA fallback setting** — With `--rdma-fallback` disabled, RDMA buffer-registration or token failures now fail the operation instead of silently sending it over HTTP.
- **S3-RDMA fallback payload** — With `--rdma-fallback`, a PUT whose RDMA preparation failed after its payload was copied (for example, when no RDMA token could be generated) was sent over HTTP without its body and failed. The fallback request now carries the whole payload, and the same failure with `--rdma-fallback` disabled no longer reports the object's bytes as written.
- **Partial-read retries at stop** — A partial read whose retry had not yet sent its request when a run stopped now delivers its earlier failure exactly once. Previously, the failure was counted but its result was never delivered, and the run ended with “Range result delivery failed.”
- **DELETE failure-budget reporting** — Duration-based DELETE runs now propagate a failure-budget exception consistently when the budget is exceeded during worker startup, matching failures detected during active waiting.
- **Composite expression evaluation** — Give each expression segment its own evaluation context to prevent intermittent method-resolution failures when synchronous and asynchronous segments run together.

- **Write prefixes and saved manifests** — `spt run write` now honors `--prefix` before any shard directories, preserving complete object keys in saved manifests for subsequent reads and cleanup. Combining `--save-items` with `--cleanup` retains the manifest after object deletion. Workloads that do not support `--prefix` now reject it explicitly instead of silently ignoring it.
- **Run labels on result artifacts** — `spt run --label` now applies the same sanitized label to generated step IDs and their result filenames as to the results directory. Runs without a label retain the `mt` prefix.
- **Distributed launch messages** — Ordinary distributed workloads now report “distributed run” instead of the misleading “distributed replay.”
- **Verification service startup** — `spt verify` now polls the required service ports for up to 30 seconds instead of checking once after a fixed ten-second sleep. Healthy engines that start more slowly can pass, already-ready engines proceed immediately, and unavailable ports still cause a bounded failure followed by container cleanup.
- **False metrics-schema warning at run start** — The TUI no longer reports “Incompatible metrics JSON … requires metrics_schema >= 2” while engines are reachable but have not yet published metrics for the current run, such as during the pre-step stabilization pause. Genuinely incompatible metrics payloads still produce the warning, including when they first appear after startup.
- **Credentials in step config logs** — The engine now masks storage credentials in each step's logged `config.yaml`, which single-host runs keep under `.node-home` in the results folder.

## [5.15.2] - 2026-09-21

### Fixed

- **Large-object SigV4 signing** — Sign S3 requests with payload lengths above 2 GiB without rejecting their 64-bit `Content-Length` values.

## [5.15.1] - 2026-09-16

### Fixed

- **Distributed TUI metrics** — Fixed frozen graphs and zero-valued progress during distributed runs by preserving the effective run and cluster identity in live and terminal metrics. Worker metrics now use the submitted step identity after startup while retaining safe fallback behavior for missing or legacy identity configuration.

## [5.15.0] - 2026-09-15

### Added

- **Partial S3 reads** — Added opt-in `--range-size`, `--range-offset`, and `--range-align` flags for fixed or random single-range READs with the Netty S3 driver. Strict response validation, retained retries, `range.read.csv`, and reconciled result summaries account for logical reads and transport attempts separately. Explicit operation traces include local selection and transport failures. AWS and S3-RDMA support are deferred; release-baseline READ/WRITE performance qualification remains pending.

- **Standalone S3 DELETE benchmarking** — Added public `spt run delete` support with safe seeded inventories, explicit canonical manifests, and guarded existing-prefix selection; single- and batched requests retain exact-version identities and report logical-request and object-outcome units separately. Optional full inventory validation and post-delete absence verification distinguish accepted API outcomes from confirmed absence. Qualification covers real-constructor Netty and AWS S3 canaries, hardware-free inherited-HTTP S3 RDMA contracts, lifecycle and distributed-routing safety, and versioned result and recovery artifacts.

- **Engine Build Information** — Added immutable engine provenance through `GET /version`, engine-local `engine.build.json`, and the combined CLI result artifact `engine.info.json`. Managed runs and replays check participating engine builds before scenario submission by default and show engine provenance in result summaries.

### Changed

- **Dependency updates** — Updated Jackson to 2.22.2, Commons Codec to 1.22.1, Guava to 33.7.1, Javassist to 3.33.0-GA, AWS SDK/CRT to 2.54.18/0.48.4, Bouncy Castle to 1.86/1.86.1, and Docker Go connections to 0.8.1.

- **Engine build mismatch policy** — `spt run` and `spt replay` reject known engine build mismatches by default. In addition to supported port-conflict handling, `--force` now permits a known build mismatch and records a prominent warning and forced-mismatch evidence; it cannot bypass malformed build information or collection failures.

### Fixed

- **Worker startup timeout** — Isolated advertised-IP detection's 10-second timeout from container startup and API readiness, preventing premature worker failures while preserving caller cancellation.

- **`spt replay` exit after completion** — `spt replay` could finish its workload, fetch results, stop containers, and print the summary, then never exit. The auto-results completion tracker was not bound to the replay run session, so the launcher was only released by the metrics-based completion heuristic, which does not fire for small distributed op-count replays. Replay now shares the authoritative terminal signal with `spt run`.

## [5.14.2] - 2026-08-13

### Changed

- **Recycled read dispatch** — Disabled the low-concurrency fast-recycle bypass introduced in v5.7.0 because directly resubmitting the same in-flight operations could restrict a duration-based read to a working set equal to concurrency. Completed operations now return through shared generator circulation, trading some low-concurrency dispatch efficiency for complete manifest coverage.

### Fixed

- **Recycled read working-set coverage** — Low-concurrency duration reads now circulate through the complete input manifest before repeating objects.

## [5.14.1] - 2026-08-11

### Added

- **Historical S3 version verification** — Added `read-verify --versions=current|all`; all-version discovery preserves exact `(bucket,key,version_id)` identities across every `ListObjectVersions` page, excludes and reports delete markers, and records `excluded_delete_marker_count` in completion evidence, the console summary, and `index.json`.

### Changed

- **Integrity completion schema v2** — Engine-produced completion records now use schema v2 to account explicitly for excluded delete markers; strict readers continue to accept legacy v1 records where that count is necessarily zero.
- **Root documentation** — Reorganized the README around a focused quick start and capability guide, clarified distributed Docker and SSH prerequisites and `spt verify`, documented version-matched engine images, and added a distributed TUI preview.
- **Dependency updates** — Updated the GitHub Actions Java setup action used by CI, artifact, and release workflows.

### Fixed

- **Multi-page verification discovery** — Recycled LIST discovery previously omitted intermediate pages and emitted the final page twice, so large `read-verify` prefix selections could verify only the final page. Discovery now publishes every page exactly once.
- **Failed verification discovery** — A failed LIST page could previously publish a valid-looking partial selection. Engine publication and CLI finalization now reject discovery evidence whenever any LIST operation failed.

## [5.14.0] - 2026-08-05

### Added

- **S3 persisted-data integrity verification** — Added `write-verify` and `read-verify` correctness workloads with v1 SHA-256 user metadata, full-object verification through current-version LIST discovery or exact-version manifests, corruption-specific exit code `20`, explicit empty-selection policy, and `--defer-verification` for CREATE-only seeding followed by later verification campaigns.
- **Integrity evidence and resumability** — Added canonical written/input/verified manifests and completion records, `verify-remaining.csv`, corruption diagnostics, digest-cost telemetry, multipart lifecycle evidence, structured step lifecycle, and distributed immutable-image or payload identity checks.
- **Direct-JAR integrity scenarios** — Added runnable `spt.jar` examples for write-and-read verification, CREATE-only seeding, and later verification from a QA-maintained item file.
- **Verification and maintenance helpers** — Added a repeatable `write-verify` test script and an engine line-count utility for local development and qualification work.

### Changed

- **Versioned integrity identity** — Metadata-mode reads carry S3 version IDs separately from object keys, while ordinary mode retains the legacy Netty `key~version` result naming for compatibility.
- **Verification run lifecycle** — Verification launch, artifact collection, finalization, shutdown, and summary generation now have explicit ownership and separately bounded cleanup so interrupted distributed runs retain attributable lifecycle evidence.

### Fixed

- **Legacy deterministic verification prefixes** — Parse only the generated leaf ID after configured prefixes and shards, and fail explicitly when it cannot be decoded instead of silently using offset zero.
- **S3 LIST integrity discovery** — Stream and validate LIST responses without buffering an unbounded page, distinguish root-level `Prefix` from `CommonPrefixes`, reject malformed or incomplete entries, and canonicalize discovered identities before verification.
- **Short-run rate metrics** — Preserve sub-second elapsed time when calculating IOPS and bandwidth so short phases no longer report zero rates.
- **Verification credential exposure** — Redact credential-bearing engine overrides from generated metadata, errors, dry-run output, trace output, and environment diagnostics while preserving exact values only for in-memory launch configuration.
- **Distributed interruption cleanup** — Stop the owned entry-node run before worker services, preserve STOPPED and downstream `not_started` outcomes, salvage partial evidence, and remove managed containers after interrupted verification runs.

## [5.13.1] - 2026-08-04

### Changed

- **Dependency updates** — Updated the GitHub Actions Java setup action.

### Fixed

- **Local saved-items reuse** — Local Docker runs now stage restrictive `items.csv` inputs as container-readable, read-only copies, allowing root-owned `0600` artifacts produced by `--save-items` to be reused with `--items-file`, `--read-items-file`, or `--delete-items-file` without changing the original file permissions.

## [5.13.0] - 2026-07-27

### Added

- **Prefix-sharded object naming** — Generated write, mixed, and read-seed keys now use concurrency-derived prefix directories by default; `--prefix-shards` selects an exact count or restores flat naming.
- **Read-phase settle control** — Added `--read-phase-pause-seconds` to configure the delay between seed, read, and cleanup phases.

### Changed

- **Engine scheduling efficiency** — Reused dispatch scratch state and moved recurring service tasks to dedicated platform threads to reduce client CPU overhead.
- **Dependency updates** — Updated Jackson, fastutil, Docker Go connections, and GitHub setup actions.
- **Netty failure diagnostics** — Added operation context to response-handler failure logs.

### Fixed

- **Sharded S3 lifecycle handling** — Cached bucket initialization across prefix paths to avoid redundant initialization during generated read workflows.

## [5.12.2] - 2026-07-16

### Fixed

- **IEC byte-size input** — CLI and engine size settings such as `--object-size` and `--item-data-size` now accept IEC suffixes (`KiB`, `MiB`, `GiB`, `TiB`, `PiB`, and `EiB`) while preserving legacy binary aliases such as `KB` and `MB`.

## [5.12.1] - 2026-07-14

### Changed

- **Dependency updates** — Updated `golang.org/x/tools`.

### Fixed

- **S3 write dispatch throughput** — Avoided repeated full-list scans during ordinary writes after composite-read batching.
- **Netty completion cleanup** — Cleared completed channel operations before reuse and released undispatched batch permits during shutdown.
- **Partial socket writes** — Corrected non-TLS file-region progress accounting for partial writes.
- **Remote execution routing** — Routed single remote-host runs through remote orchestration instead of local Docker.
- **Virtual-thread configuration** — Applied `load.service.threads` when engine containers start so workers use the requested scheduler parallelism.

## [5.12.0] - 2026-07-09

### Added

- **Bounded retries** — `load-op-retry` now retries only transient failures, uses exponential backoff, and is limited by the new `load-op-retryLimit` setting (default `10`; set `0` to disable retries).

### Changed

- **Binary byte-unit labels** — SPT now labels 1024-based byte sizes and bandwidth with IEC units (`KiB`, `MiB`, `GiB`, `MiB/s`, XML `MiBps`) instead of decimal-style labels (`KB`, `MB`, `GB`, `MB/s`, XML `MBps`). New result CSV files use `BWAvg[MiB/s]` and `BWLast[MiB/s]`, XML result files use `bw_unit="MiBps"`, and XML `filesize` values now render IEC size labels, including ranges such as `1KiB-2KiB`. The current CLI can still read legacy CSV totals with `BWAvg[MB/s]` and `BWLast[MB/s]`, but older CLIs and external scripts or dashboards that key on the exact old labels should be updated.

### Fixed

- **Live/status bandwidth conversion** — live TUI metrics and `spt status` now divide engine byte rates by 1 MiB before displaying `MiB/s`; previously those paths divided by 1,000,000 while using a binary-style label.
- **Retry reliability** — fixed several `load-op-retry` edge cases that could hang finite workloads, abandon pending retries, bypass configured throttles, or carry retry counts across successful recycled operations.
- **Distributed coordination stalls** — Docker entrypoints now set an RMI response timeout so transient worker-coordination stalls fail fast instead of blocking a run indefinitely.
- **Netty stuck-operation detection** — the default Netty idle timeout is now 30 seconds instead of 5 minutes, making short runs more likely to record stuck operations as failures. Set `--storage-net-timeoutMilliSec` explicitly for workloads that need longer idle headroom.

## [5.11.2] - 2026-06-29

### Changed

- **Dependency updates** — bumped `golang.org/x/tools` in the Go dependency group.

### Fixed

- **Local auto-results node log capture** — single-host local container runs now persist engine-side logs under a writable results-backed `.node-home` bind mount, allowing `--save-items` and other log artifacts to be written successfully without requiring host/container UID alignment.

## [5.11.1] - 2026-06-26

### Added

- **Bounded read-phase shuffle controls** — `spt run read` now supports `--shuffle` and `--shuffle-batch-size` to widen read randomness within the `ReadLoad` phase without globally exposing unbounded `load.batch.size`. The batch override is read-only, defaults to a bounded window when omitted, and is capped to avoid pathological buffer growth.

### Fixed

- **External items files in Docker runs** — `--items-file`, mixed `--read-items-file`, and mixed `--delete-items-file` are now rewritten to stable container paths and mounted into local or remote Docker test containers, allowing saved item catalogs to be reused by later read/delete workflows.
- **Large auto-results artifacts** — CLI artifact retrieval now downloads engine log artifacts larger than the engine log page size with HTTP range requests, preserving large `items.csv` files instead of truncating them at the first page.

## [5.11.0] - 2026-06-25

### Added

- **Archived workload replay** — added `spt replay --from <archive-folder-url>` to import archived SPT or legacy Mongoose workload artifacts, convert archived JSON or JavaScript scenarios, sanitize credentials, remap target environments, apply supported safe file-preparation transforms, generate equivalent S3 workloads, and launch single-host, remote-host, or distributed replay runs with trace capture and auto-results.
- **Linux arm64 CLI release binaries** — official GitHub releases now publish `spt-<version>-linux-arm64.gz` alongside the existing CLI assets, with checksum coverage for every packaged CLI binary.
- **SPT CLI self-update command** — added `spt update` for checking GitHub Releases and installing verified CLI release binaries. The command supports lightweight `--check` probes with distinct exit codes, prerelease selection, `--output` downloads, checksum verification, archive hardening, pre-download replace-access checks, and hardened GitHub transport. Windows running-binary replacement is intentionally disabled for now; use `--output` to download a verified Windows binary.
- **READ/LIST TTFB reporting** — Time to First Byte is now tracked for body-returning READ and LIST operations, including local filesystem reads and AWS SDK S3 LIST responses. Live/headless metrics and final summaries report TTFB only when samples are available, avoiding misleading mixed-workload or non-body-operation TTFB values.

### Changed

- **Mixed workload reporting** — mixed runs now render per-operation breakdowns in console/TUI final summaries, correct mixed interval/total CSV row shapes, suppress false artifact-health warnings when seed steps intentionally skip summary persistence, and emit final auto-results summaries after shutdown completes.
- **Release artifact gating** — GitHub release publication now waits for the published multi-architecture engine image before attaching release artifacts, and prerelease Docker tag validation now handles prerelease tags correctly.
- **Dependency updates** — refreshed Go indirect modules, GitHub Actions dependencies, and JVM dependencies for the 5.11.0 release prep, including Jackson Core/Databind/YAML 2.22.0 with Jackson Annotations 2.22, AWS SDK 2.46.17, AWS CRT 0.47.1, Bouncy Castle 1.84, JUnit Jupiter 5.14.4, SnakeYAML 2.6, commons-codec 1.22.0, Javassist 3.32.0-GA, hadoop-shaded-guava 1.5.0, and stax2-api 4.3.0.

## [5.10.4] - 2026-06-01

### Changed

- **Version-pinned default engine image** — the CLI now defaults to the matching versioned engine image instead of the moving `latest` tag, making CLI/engine behavior reproducible across runs. Use `--spt-image` to select a different engine image explicitly.

### Fixed

- **Headless completion detection** — fixed premature `--headless` completion when transient aggregate metrics briefly reported `Completed`, while preserving running state through temporary concurrency gaps. (PR #123)
- **Headless auto-results shutdown** — fixed a shutdown race that could prevent headless runs from reliably collecting final auto-results artifacts. (PR #125)
- **Headless multi-step read runs** — fixed multi-step completion detection so seeded read workflows wait for the actual read step instead of treating the seed step as the terminal workload. (PR #126)

## [5.10.3] - 2026-05-23

### Fixed

- **MPU completion race handling** — fixed race condition in multipart upload completion logic that could cause incorrect finalization timing. (PR #121)
- **CLI lint issues** — resolved linting warnings in CLI code. (PR #121)
- **CLI MPU batch override removal** — removed unnecessary CLI-level MPU batch override that conflicted with engine-level configuration. (PR #121)
- **MPU completion timing** — corrected timing logic in MPU completion operations to ensure accurate metrics reporting. (PR #121)
- **Terminal metrics refresh** — fixed TUI metrics display refresh to ensure real-time updates are rendered correctly. (PR #121)
- **Composite read batch dispatch** — fixed dispatch logic for composite read operations to prevent batch handling errors. (PR #121)
- **MPU scheduling and finalization** — improved MPU task scheduling and finalization to handle edge cases in distributed runs. (PR #121)
- **Jar-based MPU test helpers** — updated test helper utilities for jar-based MPU testing to improve reliability. (PR #121)
- **Multi-host headless completion hangs** — fixed issue where multi-host headless runs would hang after completion instead of exiting cleanly. (PR #120)

## [5.10.2] - 2026-05-19

### Fixed

- **Config YAML load.op key confusion** — config.yaml dumps now trim irrelevant `load.op.weight` keys based on step type. `MixedLoad` steps show only `load.op.weights` (distribution map); non-mixed steps (CreateLoad, ReadLoad, etc.) show only scalar `load.op.weight`. This eliminates the confusing dual-key output that made it impossible to distinguish step types from config dumps alone. (PR #118)

## [5.10.1] - 2026-05-08

### Fixed

- **S3 MPU object URI mismatch with trailing output paths** — fixed HTTP data URI joining when `item-output-path` ends with `/` and generated item names are bare. MPU init, part upload, complete, and abort requests now use the same object URI, avoiding backend-specific `404 Not Found` failures on storage systems that treat repeated slashes as significant object key characters. (PR #115)

## [5.10.0] - 2026-05-07

### Added

- **Real-time latency percentiles** — latency is now tracked in memory and reported as percentiles (`p50`, `p90`, `p99`, `p99.9`) through the Metrics API and final CLI/TUI summaries. The TUI now shows `p50` in place of mean.
- **Time-to-first-byte tracking** — TTFB is now recorded for body-returning operations, including LIST, and included in final reporting when available.

### Changed

- **Mixed workload distribution config key** — renamed the mixed workload engine config from `load.op.weight.{get,put,delete,stat}` to `load.op.weights.{get,put,delete,stat}` to avoid colliding with `WeightedLoad`'s scalar `load.op.weight` key. CLI-generated `spt run mixed` scenarios use the new key automatically; hand-written `MixedLoad` scenarios should be updated.
- **Trace output capture** — trace runs now record invocation details and attach trace output to run metadata and auto-results.

### Fixed

- **Mixed workload weighting and TTFB edge cases** — corrected mixed workload weighting behavior and tightened TTFB start timing so valid zero-length body-returning operations are not dropped.

## [5.9.2] - 2026-05-05

### Fixed

- **S3 multipart NPE on error responses** — `S3ResponseHandler` no longer throws `NullPointerException` when an MPU part PUT returns a non-2xx response without an `ETag` header. The handler previously attempted to record a null ETag into the parent task's context map (a `ConcurrentHashMap`, which rejects null values), surfacing as misleading "Premature channel closure" warnings that masked the real upstream 4xx/5xx failure. (PR #105)
- **Item name corruption with versioning enabled** — when versioning was enabled and a response omitted the `x-amz-version-id` header, the literal string "null" was appended to the item name (compounding `~null~null~…` across retries). Both header reads now early-out cleanly when absent. (PR #105)

## [5.9.1] - 2026-05-01

### Added

- **CRC64-NVME checksum support** — added support for the `crc64-nvme` checksum algorithm across all S3 drivers (Netty, AWS SDK, and RDMA). The Netty driver now includes explicit checksum strategy metadata and multipart checksum wiring for CRC64-NVME. The CLI `--checksum crc64-nvme` flag is now accepted for all S3 drivers (previously AWS-only). Added RDMA checksum parity tests covering CRC32, CRC32C, and CRC64-NVME known vectors. (PRs #101, #102)

## [5.9.0] - 2026-04-29

### Added

- **Post-quantum TLS handshake support** — the Netty HTTPS driver now offers hybrid post-quantum key exchange groups (`X25519MLKEM768`, `x25519`, `secp256r1`) during TLS 1.3 handshakes via the Bouncy Castle JSSE provider. Enabled by default in `prefer` mode with automatic classical fallback. Covers the `default`, `rdma`, and S3 Tables drivers. Engine config keys: `storage.net.ssl.pqcMode` (`off`/`prefer`/`require`), `storage.net.ssl.jsseProvider`, `storage.net.ssl.namedGroups`. See [`cli/docs/PQC_TLS.md`](cli/docs/PQC_TLS.md).
- **Data compressibility control** (`--object-data-compressibility`) — set a target compressibility percentage (0-100) for generated object data. Each 4KB chunk is split into pseudo-random and zero-filled portions, forcing the storage system's compression to evaluate every block. Engine config key: `item.data.input.compressibility`. (env: `SPT_OBJECT_DATA_COMPRESSIBILITY`)
- **Anti-dedupe stamping** (`--object-data-dedupable=false`) — stamp every 4KB of generated data with a 16-byte header (64-bit object identifier + 64-bit stream offset) to practically eliminate inline deduplication on typical fixed/variable chunking systems. Stamps are deterministic and consistent across all transfer paths (Netty, SSL, AWS SDK, RDMA). Engine config key: `item.data.dedupable`. (env: `SPT_OBJECT_DATA_DEDUPABLE`)
- **MPU concurrency limits** — add `--mpu-object-concurrency` and `--mpu-part-concurrency` flags to control object-level and part-level concurrency for multipart uploads. Prevents resource exhaustion and improves throughput control. Engine config keys: `load.op.create.mpu.objectConcurrency` and `load.op.create.mpu.partConcurrency`.
- **AWS CRT driver optimization** — optimize s3-aws storage driver to use AWS CRT (Common Runtime) library for improved performance and efficiency.

### Fixed

- **Post-5.6.0 AWS Driver performance regression** — fix performance issue introduced after 5.6.0 in the s3-aws driver.
- **Mixed-mode generator correctness** — fix correctness issues in mixed workload generator.
- **headObject rebase fix** — fix headObject to use s3AsyncClient after rebase.
- **Async stamp buffer lifetime race** — fix race condition in async stamp buffer lifetime management.
- **MPU init condition bug** — resolve MPU init condition bug in complete method.
- **Operation timing fields reset** — reset operation timing fields when generating MPU complete/abort requests.
- **Prometheus metric family names** — stabilize prometheus metric family names for consistency.
- **Operation result timing snapshot test** — stabilize operation result timing snapshot test.

### Changed

- **MPU dispatch backpressure** — harden coop MPU dispatch backpressure for better flow control.
- **Multipart limit warning noise** — reduce multipart limit warning noise.
- **Deduplication stamping** — use 64-bit object id in dedupe stamps for larger object space.

## [5.8.0] - 2026-04-17

### Added

- **Mixed Workload Benchmarking Support** — added native support for testing mixed operations simultaneously (`READ`, `CREATE`, `DELETE`, `STAT`). The CLI now exposes a `spt run mixed` command and scenario builder, supporting dynamic percentage distributions (e.g., `GET=45%`, `PUT=15%`, `DELETE=10%`, `STAT=30%`).
- **MixedLoad Engine Extension** — new native load step extension. Implements a single-generator, multi-operation state machine. Employs a non-blocking `deleteQueue` directly fed by `CREATE` completions, bounded spin locks, and re-rolls empty queues uniformly to prevent starvation or blocking.

### Fixed

- **Netty IO Buffer Sizing for Mixed Workloads** — fixed severe packet fragmentation and high context switching during mixed-object payloads. `MixedLoadStepLocal` now properly initializes socket buffers for `OpType.NOOP`, bringing `spt` mixed performance on par with competitive tools (e.g. Warp).

### Changed

- **Engine Threading Performance** — replaced busy `parkNanos` waits with cooperative thread `yield` across core engine queues and thread pools to reduce CPU waste during I/O stalls.
- **Dependency Updates** — bumped `netty` to `4.1.132.Final` (addressing 5 CVEs), `guava` to `33.6.0-jre`, and rolled out safe minor upgrades to Go CLI packages.

## [5.7.4] - 2026-04-16

### Fixed

- **Netty connection pool exhaustion failure** — resolved a `ConnectException: Connection is not active` bug where the `s3` (Netty) driver aggressively failed entire load operations if handed a stale/idle connection by the pool. The driver now automatically closes the inactive channel and loops to lease a fresh active connection without disrupting the request stream.

## [5.7.3] - 2026-04-13

### Fixed

- **`result.xml` well-formedness fix** — replace log4j2 header/footer with explicit wrapper-tag management in `MetricsManagerImpl`. The engine now logs `<result>` and `</result>` tags at step lifecycle boundaries, with safety cleanup in `doClose()` for abnormal shutdowns. The CLI's artifact fetcher includes a `normalizeResultXML()` safety net that strips stale wrappers and re-wraps entries. This eliminates the timing bug where log4j2's async rollover wrote `</result>` before the entries, producing malformed XML.

## [5.7.2] - 2026-04-13

### Fixed

- **`result.xml` format restored** — revert incorrect 5.7.1 change that collapsed per-entry `<result>` wrappers into a single document-level wrapper. Each metrics entry is again wrapped individually as `<result><result .../></result>`, matching the original Mongoose v3-compatible format.

## [5.7.1] - 2026-04-13

### Added

- **`--checksum` CLI flag** — enable per-object (and per-part for MPU) checksum validation with `crc32`, `crc32c`, `sha1`, or `sha256`. Environment variable: `SPT_CHECKSUM`. Works with both the Netty and AWS SDK S3 drivers. (PR #80)
- **S3-AWS driver checksum support** — the `s3-aws` driver now reads `storage.checksum.enabled` / `storage.checksum.algorithm` from config and sets the corresponding `ChecksumAlgorithm` on `PutObjectRequest` via the AWS SDK flexible checksum API. Supported algorithms: CRC32, CRC32C, SHA1, SHA256. (PR #80)
- **Netty S3 driver MPU checksum completeness** — multipart upload init requests now include the `x-amz-checksum-algorithm` header, the response handler captures per-part checksum headers, and the complete-upload XML body includes per-part checksum elements. (PR #80)
- **`result.xml` output** — engine now generates a `result.xml` artifact with per-operation metrics in a format compatible with Mongoose v3, including duration, latency, concurrency, byte counts, and success/fail tallies. The CLI fetches `result.xml` and `result-threshold.xml` automatically. (PR #81)

### Fixed

- **`result.xml` well-formedness** — closing XML tags moved from the log4j2 pattern to the appender footer so the output is always valid XML. (PR #81)

### Changed

- **Bump `actions/github-script` from 8 to 9** in the `github-actions` dependency group. (PR #82)

## [5.7.0] - 2026-04-09

### Changed

- **Docker runtime upgraded from JDK 21 to JDK 25** — enables compact object headers (JEP 519) and improved virtual thread scheduling for better small-object throughput. (PR #76)
- **Read throughput optimization** — replace `ArrayBlockingQueue` with lock-free `ConcurrentLinkedQueue` for operation recycling, reducing contention at high concurrency. (PR #73)
- **Fast-recycle dispatch** — short-circuit dispatch path for low-concurrency workloads; gate VT quiesce on concurrency level to avoid T8/T32 regression. (PR #74)
- **Write throughput optimization** — replace timed dispatch awaits with untimed to reduce VT-unparker overhead, eliminate per-request `Matcher` allocation in S3 header normalization, add JVM tuning flag `-XX:-DoJVMTIVirtualThreadTransitions`. (PR #75)

### Fixed

- **Backpressure signal-loss race** — fix `await()` signal-loss race condition in dispatch task that could cause stalls under sustained load. (PR #75)
- **Connection failure permit leak** — fix semaphore permit leak when Netty connections fail during setup. (PR #75)
- **RDMA native library not packaged in release artifacts** — release workflow was missing RDMA build dependencies, so `libspt_rdma.so` was never compiled into the shadow JAR. Added build-time logging when the native build is skipped. (PR #79)

### Tests

- **Netty and HTTP driver test coverage** — add unit tests covering HTTP response status mapping, data URI path generation, range-header serialization, exception-to-status mapping, and chunked NIO streams. HTTP module line coverage 23% to 43%, Netty module 30% to 38%. (PR #77)

## [5.6.0] - 2026-04-04

### Added

- **AWS SDK S3 storage driver (`s3-aws`)** — new pluggable driver built on AWS SDK for Java v2 with the Apache HTTP client. Supports CREATE, READ, UPDATE, DELETE, and LIST operations. Select with `--s3-driver aws`. (PR #61)
- **`--s3-driver` CLI flag** — choose between `default` (Netty), `aws` (AWS SDK), or `rdma` at runtime. Environment variable: `SPT_S3_DRIVER`. (PR #67)
- **S3 Multipart Upload enhancements** — per-part checksum verification, per-part retry (up to 3 attempts), automatic abort on failure, and MPU ABORT operation. Enable with `--part-size`. (PR #63)
- **Parallel range-read** for composite READ operations and per-phase latency breakdown in MPU metrics. (PR #63)
- **`--save-items` / `--items-file`** — decoupled write-then-read workflows: save item lists from write benchmarks and replay them in independent read passes. (PR #60)
- **`--s3-driver` support in test scripts** — `testread.sh`, `testlocal.sh`, and `testrdma.sh` accept `--s3-driver` for driver selection. (PR #67)
- **`--image` / `--skip-image-pull` in test scripts** — `testread.sh`, `testlocal.sh`, and `testmock.sh` now support Docker image override and pull control via flags or environment variables (`SPT_IMAGE`, `SPT_SKIP_IMAGE_PULL`).
- **Storage Drivers section in README** — root README now documents the available S3 drivers and the `--s3-driver` flag.

### Fixed

- **s3-aws error classification** — `classifyFailure()` now maps HTTP 504 to `FAIL_TIMEOUT` and 507 to `RESP_FAIL_SPACE`, matching the Netty driver's behavior. (PR #69)
- **s3-aws error logging** — exceptions in `invokeNio` are now logged at DEBUG/TRACE level instead of being silently swallowed. `resolveBucketName` errors are also logged. (PR #69)
- **s3-aws recycle fix** — strip `dstPath` prefix from mutated item names during operation recycling to prevent path corruption. (PR #61)
- **s3-aws endpoint resolution** — read port and SSL settings from standard config paths instead of hardcoded defaults. (PR #61)
- **Netty exclusion from s3-aws shadow JAR** — prevents Netty 4.1.x / 4.2.x version conflicts at runtime. (PR #61)
- **NioStorageDriverBase semaphore permit leak** — fix permit leak that could stall the driver under sustained load. (PR #65)
- **JVM performance args** — add tuned JVM settings to the bundled `run.sh` launch command. (PR #64)
- **items.csv path** — use engine `home_dir` via ThreadContext for correct artifact placement. (PR #62)
- **DataItemInputStream mark/reset** — save and restore `doneSize` counter so the stream can be re-read after reset, required by the upgraded AWS SDK's retry mechanism. (PR #71)
- **LoadStepClient test regression** — fix 10-minute test hang caused by remote node retries. (PR #57)
- **Docker publish CI gate** — verify all CI checks pass before publishing images. (PR #55)
- **README quickstart** — fix download instructions for pre-built binaries. (PR #56)

### Changed

- **AWS SDK upgraded from 2.21.29 to 2.42.28** — ~2.4 years of improvements; s3-aws driver updated to use `DataItemInputStream` (supports mark/reset) and `RequestBody.fromFile()` for compatibility. (PR #71)
- **Dependency updates** — Scala 2.13.18, Jackson 2.21.2, Guava 33.5.0-jre, Mockito 5.23.0, commons-codec 1.21.0, commons-lang 3.20.0, fastutil 8.5.18, and others. AWS SDK dependencies centralized in `libs.versions.toml`. Added `com.github.ben-manes.versions` Gradle plugin for dependency management. (PR #68)
- **JitPack removed** — akurilov JARs vendored locally, eliminating the JitPack repository dependency. (PR #58)
- **Gradle Actions** — bumped `gradle/actions` from 5 to 6. (PR #59)

## [5.5.0] - 2026-02-21

public release with CLI/TUI, engine orchestration, write/read/list/mock workloads, S3-RDMA acceleration, S3 Tables benchmarking, distributed multi-node runs, and headless CI mode.

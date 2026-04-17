# Tools

- `jartest.mixed.sh`: Runs a mixed workload (GET/PUT/DELETE/STAT) directly against the engine JAR using a generated JavaScript scenario. Configurable distribution weights, seed count, and optional cleanup via environment variables.
- `monitor-entry-metrics.sh`: Polls `/metrics/json` (and optional verbose/cluster variants) on a loop, logging raw payloads with timestamps. Useful during distributed runs to confirm entry-node totals remain cached in the pause window.

# Branch 1K AWS smoke + final (single 2m)

Run root: `/tmp/spt-branch-1k-aws-20260428_132301`

## Final vs preserved main baseline (comparison_20260404_181517, median of 3 runs)

| Operation | Threads | Branch TP (op/s) | Main baseline median (op/s) | Ratio | Fail | Latency avg (ms) | Verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| read | 1 | 488.587 | 1095.000 | 44.620% | 1 | 1.929 | **LOW** |
| read | 4 | 1797.843 | 4769.000 | 37.699% | 4 | 1.887 | **LOW** |
| read | 8 | 0.281 | 6456.000 | 0.004% | 6 | 8.142 | **HORRIBLE** |
| read | 32 | 0.058 | 3710.000 | 0.002% | 6 | 17167.601 | **HORRIBLE** |
| write | 1 | 476.975 | 557.000 | 85.633% | 15 | 1.995 | **ok** |
| write | 4 | 2042.198 | 2213.000 | 92.282% | 65 | 1.863 | **ok** |
| write | 8 | 0.405 | 3388.000 | 0.012% | 0 | 14668.061 | **HORRIBLE** |
| write | 32 | 0.273 | 3460.000 | 0.008% | 1 | 21787.492 | **HORRIBLE** |

## Smoke observations

- T1/T4 looked stable and proportionally similar to final (lower because 30s window).
- T8/T32 were already pathological in smoke, and stayed pathological in final.

## Error signals (final)

| Operation | Threads | FAIL_IO | FAIL_UNKNOWN | Metrics-context warnings |
|---|---:|---:|---:|---:|
| read | 1 | 3 | 1 | 2 |
| read | 4 | 0 | 4 | 2 |
| read | 8 | 2 | 6 | 2 |
| read | 32 | 0 | 6 | 2 |
| write | 1 | 15 | 0 | 1 |
| write | 4 | 65 | 0 | 1 |
| write | 8 | 0 | 0 | 1 |
| write | 32 | 0 | 1 | 1 |

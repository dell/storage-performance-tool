# S3-AWS Performance Comparison Results

**Test Date:** Thu 07 May 2026 05:56:13 PM UTC
**S3 Endpoint:** http://10.246.190.64:8333
**Bucket:** spttest
**Test Duration:** 1 minutes per test

## Test Configuration

- **Object Sizes:** 10K 1M
- **Operations:** create read
- **Concurrency Levels:** 1 8 32
- **Total Tests:** 12

## Test Approach

**Phase 1:** Data population with create operations (32 threads, deterministic serial naming, save object lists to item files)
**Phase 2:** Full test matrix for all operations and concurrency levels (read uses saved item files with recycle mode)

## Results Summary

### Raw Results

```tsv
operation\tobject_size\tconcurrency\tthroughput_ops\tmb_per_sec\tlatency_mean_ms\tlatency_p50_ms\tlatency_p75_ms\tlatency_p95_ms\ttotal_ops\ttotal_errors\tduration_seconds
create	10K	32	2770.7049180327867	27.057665215163933	2.779	2.301	2.750	0	169013	0	61.021
create	1M	32	513.5737704918033	513.5737704918033	15.052	13.764	15.483	0	31328	0	61.019
create	10K	1	369.59016393442624	3.609278944672131	2.484	2.320	2.567	0	22545	0	61.018
create	10K	8	2484.967213114754	24.26725794057377	2.652	2.231	2.634	0	151583	0	61.018
create	10K	32	2794.409836065574	27.28915855532787	2.766	2.289	2.745	0	170459	0	61.025
read	10K	1	369.0	3.603515625	2.530	2.422	2.634	0	22509	1	61.024
read	10K	8	2097.2295081967213	20.480756915983605	3.298	2.790	3.345	0	127931	8	61.022
read	10K	32	2316.9836065573772	22.626793032786885	3.350	2.781	3.345	0	141336	8	61.027
create	1M	1	65.52459016393442	65.52459016393442	14.617	13.806	15.962	0	3997	0	61.021
create	1M	8	419.327868852459	419.327868852459	15.697	14.499	16.378	0	25579	0	61.025
create	1M	32	505.2295081967213	505.2295081967213	15.294	13.988	15.777	0	30819	0	61.02
read	1M	1	175.5737704918033	175.5737704918033	5.410	4.822	5.876	0	10710	1	61.035
read	1M	8	740.1967213114754	740.1967213114754	8.616	8.047	9.408	0	45152	6	61.028
read	1M	32	802.8852459016393	802.8852459016393	9.306	8.526	10.056	0	48976	8	61.03
```

### Performance by Object Size

#### 10K Objects (Small Object Workload)

| Operation | Concurrency | Throughput (ops/s) | Bandwidth (MB/s) | Mean Latency (ms) | P50 Latency (ms) | P75 Latency (ms) | P95 Latency (ms) |
|-----------|-------------|-------------------|-----------------|------------------|-----------------|-----------------|-----------------|
| create | 32 | 2770.7049180327867 | 27.057665215163933 | 2.779 | 2.301 | 2.750 | 0 |
| create | 1 | 369.59016393442624 | 3.609278944672131 | 2.484 | 2.320 | 2.567 | 0 |
| create | 8 | 2484.967213114754 | 24.26725794057377 | 2.652 | 2.231 | 2.634 | 0 |
| create | 32 | 2794.409836065574 | 27.28915855532787 | 2.766 | 2.289 | 2.745 | 0 |
| read | 1 | 369.0 | 3.603515625 | 2.530 | 2.422 | 2.634 | 0 |
| read | 8 | 2097.2295081967213 | 20.480756915983605 | 3.298 | 2.790 | 3.345 | 0 |
| read | 32 | 2316.9836065573772 | 22.626793032786885 | 3.350 | 2.781 | 3.345 | 0 |

#### 1M Objects (Large Object Workload)

| Operation | Concurrency | Throughput (ops/s) | Bandwidth (MB/s) | Mean Latency (ms) | P50 Latency (ms) | P75 Latency (ms) | P95 Latency (ms) |
|-----------|-------------|-------------------|-----------------|------------------|-----------------|-----------------|-----------------|
| create | 32 | 513.5737704918033 | 513.5737704918033 | 15.052 | 13.764 | 15.483 | 0 |
| create | 1 | 65.52459016393442 | 65.52459016393442 | 14.617 | 13.806 | 15.962 | 0 |
| create | 8 | 419.327868852459 | 419.327868852459 | 15.697 | 14.499 | 16.378 | 0 |
| create | 32 | 505.2295081967213 | 505.2295081967213 | 15.294 | 13.988 | 15.777 | 0 |
| read | 1 | 175.5737704918033 | 175.5737704918033 | 5.410 | 4.822 | 5.876 | 0 |
| read | 8 | 740.1967213114754 | 740.1967213114754 | 8.616 | 8.047 | 9.408 | 0 |
| read | 32 | 802.8852459016393 | 802.8852459016393 | 9.306 | 8.526 | 10.056 | 0 |

### Performance by Operation

#### Create Operations

| Object Size | Concurrency | Throughput (ops/s) | Bandwidth (MB/s) | Mean Latency (ms) |
|-------------|-------------|-------------------|-----------------|------------------|
| 10K | 32 | 2770.7049180327867 | 27.057665215163933 | 2.779 |
| 1M | 32 | 513.5737704918033 | 513.5737704918033 | 15.052 |
| 10K | 1 | 369.59016393442624 | 3.609278944672131 | 2.484 |
| 10K | 8 | 2484.967213114754 | 24.26725794057377 | 2.652 |
| 10K | 32 | 2794.409836065574 | 27.28915855532787 | 2.766 |
| 1M | 1 | 65.52459016393442 | 65.52459016393442 | 14.617 |
| 1M | 8 | 419.327868852459 | 419.327868852459 | 15.697 |
| 1M | 32 | 505.2295081967213 | 505.2295081967213 | 15.294 |

#### Read Operations

| Object Size | Concurrency | Throughput (ops/s) | Bandwidth (MB/s) | Mean Latency (ms) |
|-------------|-------------|-------------------|-----------------|------------------|
| 10K | 1 | 369.0 | 3.603515625 | 2.530 |
| 10K | 8 | 2097.2295081967213 | 20.480756915983605 | 3.298 |
| 10K | 32 | 2316.9836065573772 | 22.626793032786885 | 3.350 |
| 1M | 1 | 175.5737704918033 | 175.5737704918033 | 5.410 |
| 1M | 8 | 740.1967213114754 | 740.1967213114754 | 8.616 |
| 1M | 32 | 802.8852459016393 | 802.8852459016393 | 9.306 |

#### Mixed Operations

| Object Size | Concurrency | Throughput (ops/s) | Bandwidth (MB/s) | Mean Latency (ms) |
|-------------|-------------|-------------------|-----------------|------------------|


### Key Observations

<!-- Add manual analysis here based on the results -->

### Test Environment

- **SPT Version:** v5.9.2-1-g7d65855
- **Branch:** task-optimize-perf-aws-sdk
- **Commit:** 7d65855
- **Test Start:** Thu 07 May 2026 05:40:43 PM UTC
- **Test End:** Thu 07 May 2026 05:56:13 PM UTC
- **Total Duration:** 15 minutes

### Raw Data Files

- Full results: `results.tsv`
- Individual test logs: `*_raw.log`


export type RunMode = "guided" | "expert";
export type Options = {
  mode?: RunMode;
  object_count: number;
  object_size: string;
  threads: number;
  max_runtime_seconds: number;
  // "s3" (default): write + verify on the configured bucket. "mock": SPT's mock driver, no S3.
  driver?: Driver;
};
export type Driver = "s3" | "mock";
export type Row = {
  timestamp?: string;
  op_type?: string;
  phase?: string;
  step_id?: string;
  source_file?: string;
  elapsed_seconds?: number | null;
  duration_seconds?: number | null;
  cumulative_ops?: number | null;
  successful_ops?: number | null;
  count_failed?: number | null;
  failed_ops?: number | null;
  average_iops?: number | null;
  interval_iops?: number | null;
  iops?: number | null;
  bandwidth_mib_s?: number | null;
  p50_latency_us?: number | null;
  completion_percent?: number | null;
  bytes_processed?: number | null;
  concurrency?: number | null;
  concurrency_mean?: number | null;
  node_count?: number | null;
  corrupt_ops?: number | null;
  latency_mean_us?: number | null;
  latency_min_us?: number | null;
  latency_max_us?: number | null;
  latency_p99_us?: number | null;
};
export type Metrics = {
  run_id: string;
  updated_at?: string;
  source_file?: string;
  workload_type?: string;
  is_active: boolean;
  is_aggregate: boolean;
  phase?: string;
  progress_percent?: number | null;
  samples: Row[];
  chart_samples?: Row[];
  phase_summaries?: Row[];
  total_failed?: number | null;
  spt_summary_report?: string;
  nodes?: NodeRow[];
};
export type Job = {
  run_id: string;
  status: string;
  started_at?: string;
  finished_at?: string;
  elapsed_seconds?: number;
  phase?: string;
  message?: string;
  returncode?: number | null;
  prefix?: string;
  options?: Options;
};
export type State = {
  connection?: {
    status: "connected" | "disconnected";
    mode?: "attached" | "local";
    host?: string;
    run_id?: string;
    read_only?: boolean;
  };
  target: {
    ready: boolean;
    reasons?: string[];
    mock_ready?: boolean;
    mock_reasons?: string[];
    endpoint?: string;
    endpoint_label?: string;
    bucket?: string;
    credentials_configured?: boolean;
    endpoint_source?: "env" | "dashboard";
    hosts?: string[];
  };
  job: Job | null;
  metrics: Metrics | null;
  storage?: Storage;
  limits?: RunLimits;
  branding?: { product_name: string; environment_label: string };
};
export type Range = { min: number; max: number };
// Published by the server; it enforces the same values on POST /api/runs.
export type RunLimits = {
  guided: {
    object_counts: number[];
    object_sizes: string[];
    threads: number[];
    max_runtime_seconds: number[];
  };
  expert: {
    object_count: Range;
    object_size_bytes: Range;
    threads: Range;
    max_runtime_seconds: Range;
    max_total_bytes: number;
  };
};
export type CheckStatus = "pending" | "passed" | "failed";
export type VerifyNode = {
  host: string;
  status: CheckStatus;
  message: string;
  checks: { name: string; status: "pass" | "fail" | "warn"; message: string }[];
};
export type Verification = {
  status: "idle" | "running" | "passed" | "failed";
  started_at?: string;
  finished_at?: string;
  summary: string;
  nodes: VerifyNode[];
  endpoint: { label: string; status: CheckStatus; message: string };
};
export type Storage = {
  hosts: string[];
  endpoint: string;
  max_hosts: number;
  verification: Verification;
};
export type HistoryRun = {
  run_id: string;
  started_at?: string;
  status: string;
  options?: Partial<Options>;
  phase_summaries: Row[];
};
export type Sample = {
  timestamp: string;
  elapsed: number;
  throughput: number | null;
  bandwidth: number | null;
  p50: number | null;
  p99: number | null;
  row: Row;
};
export type NodeRow = {
  host: string;
  node_id?: string;
  role?: string;
  status: "reporting" | "unavailable" | string;
  op_type?: string;
  sample_ts?: string;
  successful_ops?: number | null;
  failed_ops?: number | null;
  corrupt_ops?: number | null;
  iops?: number | null;
  bandwidth_mib_s?: number | null;
  p50_latency_us?: number | null;
  p99_latency_us?: number | null;
};
export type CatalogPhase = {
  key: string;
  step: string;
  op_type: string;
  source_file: string;
  successful_ops: number;
  failed_ops: number;
  corrupt_ops: number | null;
  iops: number | null;
  bandwidth_mib_s: number | null;
  p50_latency_us: number | null;
  p99_latency_us: number | null;
  latency_mean_us?: number | null;
  latency_max_us?: number | null;
  duration_seconds?: number | null;
  bytes_processed?: number | null;
  node_count?: number | null;
};
export type CatalogRun = {
  id: string;
  name: string;
  source: "dashboard" | "cli";
  status: "complete" | "partial" | "malformed";
  integrity: "passed" | "failed" | "incomplete" | "not_recorded";
  started_at?: string;
  workload?: string;
  label?: string;
  object_size?: string;
  object_count?: number;
  threads?: number;
  duration?: string;
  s3_driver?: string;
  host_count?: number;
  endpoint_host?: string;
  cli_version?: string;
  engine_version?: string;
  phases: CatalogPhase[];
  issues: string[];
  summary_file?: string;
  modified_at: string;
};
export type CatalogList = {
  root: string;
  runs: CatalogRun[];
  total: number;
  truncated: boolean;
  read_only: boolean;
};
export type SeriesPoint = {
  t: number;
  iops: number | null;
  bandwidth_mib_s: number | null;
  p50_latency_us: number | null;
  p99_latency_us: number | null;
};
export type CompareFlag =
  | "regressed"
  | "improved"
  | "within"
  | "insufficient"
  | "no_data"
  | "not_comparable";
export type CompareDelta = {
  run_id: string;
  phase: string;
  metric: string;
  label: string;
  baseline: number | null;
  value: number | null;
  delta_pct: number | null;
  flag: CompareFlag;
  reason?: string;
};
export type CompareResult = {
  baseline: string;
  runs: CatalogRun[];
  series: Record<string, Record<string, SeriesPoint[]>>;
  deltas: CompareDelta[];
  settings_differences: Record<string, string[]>;
  rule: {
    label: string;
    threshold_pct: number;
    latency_floor_us: number;
    min_ops: number;
    description: string;
  };
};
export type LogLevel = "all" | "warn" | "error";
export type LogLine = { file: string; level: string; text: string };
export type LogResponse = {
  run_id: string;
  source: "console" | "results";
  level: LogLevel;
  interim: boolean;
  truncated: boolean;
  lines: LogLine[];
};

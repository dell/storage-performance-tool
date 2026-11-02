import { useEffect, useState } from "react";
import type { CatalogList, CatalogRun } from "../types";
import { Status } from "./Status";
import { RunLog } from "./RunLog";
import { formatNumber as fmt } from "../utils/metrics";
import {
  catalogQuery,
  maxCompare,
  runSettings,
  toggleSelection,
  type CatalogFilters,
} from "../utils/catalog";

const integrityText: Record<CatalogRun["integrity"], string> = {
  passed: "Integrity passed",
  failed: "Integrity failed",
  incomplete: "Integrity incomplete",
  not_recorded: "Integrity not recorded",
};

export function Catalog({
  onCompare,
}: {
  onCompare: (ids: string[], baseline: string) => void;
}) {
  const [filters, setFilters] = useState<CatalogFilters>({
    q: "",
    source: "",
    status: "",
  });
  const [data, setData] = useState<CatalogList | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<string[]>([]);
  const [baseline, setBaseline] = useState("");
  const [open, setOpen] = useState("");
  const [version, setVersion] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    const timer = setTimeout(() => {
      setLoading(true);
      fetch(catalogQuery(filters), {
        cache: "no-store",
        signal: controller.signal,
      })
        .then(async (r) => {
          const body = await r.json();
          if (!r.ok) throw new Error(body.error ?? "Could not read results.");
          setData(body as CatalogList);
          setError("");
        })
        .catch((e) => {
          if (!controller.signal.aborted)
            setError(
              e instanceof Error ? e.message : "Could not read results.",
            );
        })
        .finally(() => {
          if (!controller.signal.aborted) setLoading(false);
        });
    }, 200);
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [filters, version]);
  const effectiveBaseline = selected.includes(baseline)
    ? baseline
    : (selected[0] ?? "");
  const runs = data?.runs ?? [];
  return (
    <>
      <div className="page-heading">
        <div>
          <span className="eyebrow">RESULTS CATALOG · READ ONLY</span>
          <h1>Results catalog</h1>
          <p>
            Dashboard and CLI runs found under{" "}
            {data?.root ?? "the results folder"}. Nothing here changes or
            deletes results.
          </p>
        </div>
        <div className="actions">
          <button onClick={() => setVersion((v) => v + 1)} disabled={loading}>
            Rescan
          </button>
          <button
            className="primary"
            disabled={selected.length < 2}
            onClick={() => onCompare(selected, effectiveBaseline)}
          >
            Compare {selected.length || ""} selected
          </button>
        </div>
      </div>
      <section className="panel catalog-filters" aria-label="Filters">
        <label>
          Search
          <input
            type="search"
            value={filters.q}
            placeholder="Run name, label, size, endpoint host"
            onChange={(e) => setFilters({ ...filters, q: e.target.value })}
          />
        </label>
        <label>
          Source
          <select
            value={filters.source}
            onChange={(e) =>
              setFilters({
                ...filters,
                source: e.target.value as CatalogFilters["source"],
              })
            }
          >
            <option value="">All sources</option>
            <option value="dashboard">Dashboard</option>
            <option value="cli">CLI</option>
          </select>
        </label>
        <label>
          Status
          <select
            value={filters.status}
            onChange={(e) =>
              setFilters({
                ...filters,
                status: e.target.value as CatalogFilters["status"],
              })
            }
          >
            <option value="">All statuses</option>
            <option value="complete">Complete</option>
            <option value="partial">Partial</option>
            <option value="malformed">Malformed</option>
            <option value="failed">Integrity failed</option>
          </select>
        </label>
        <p className="form-help">
          Select 2–{maxCompare} runs to compare; the baseline is the run the
          others are checked against.
        </p>
      </section>
      {error && (
        <div className="error" role="alert">
          {error}
        </div>
      )}
      {data?.truncated && (
        <div className="notice notice-warn">
          The results folder is large; only part of it was scanned.
        </div>
      )}
      <section className="panel run-history-panel">
        {loading && !data ? (
          <p className="history-empty">Scanning results…</p>
        ) : runs.length ? (
          <div className="table-wrap">
            <table className="run-history-table catalog-table">
              <thead>
                <tr>
                  <th>Compare</th>
                  <th>Baseline</th>
                  <th>Run</th>
                  <th>Started · UTC</th>
                  <th>Settings</th>
                  <th>Status</th>
                  <th>Phases · IOPS</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {runs.map((run) => (
                  <CatalogRow
                    key={run.id}
                    run={run}
                    checked={selected.includes(run.id)}
                    full={selected.length >= maxCompare}
                    isBaseline={effectiveBaseline === run.id}
                    open={open === run.id}
                    onToggle={() =>
                      setSelected((s) => toggleSelection(s, run.id))
                    }
                    onBaseline={() => setBaseline(run.id)}
                    onOpen={() => setOpen(open === run.id ? "" : run.id)}
                  />
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="history-empty">
            <h2>No runs found</h2>
            <p>
              {filters.q || filters.source || filters.status
                ? "No runs match these filters."
                : "SPT result folders will appear here after a run."}
            </p>
          </div>
        )}
        <p className="history-footnote">
          {data ? `${runs.length} shown of ${data.total} found.` : ""} Malformed
          or partial folders are listed with what is missing.
        </p>
      </section>
    </>
  );
}

function CatalogRow({
  run,
  checked,
  full,
  isBaseline,
  open,
  onToggle,
  onBaseline,
  onOpen,
}: {
  run: CatalogRun;
  checked: boolean;
  full: boolean;
  isBaseline: boolean;
  open: boolean;
  onToggle: () => void;
  onBaseline: () => void;
  onOpen: () => void;
}) {
  return (
    <>
      <tr className={run.status === "malformed" ? "catalog-malformed" : ""}>
        <td>
          <input
            type="checkbox"
            aria-label={`Compare ${run.id}`}
            checked={checked}
            disabled={!checked && (full || run.status === "malformed")}
            onChange={onToggle}
          />
        </td>
        <td>
          <input
            type="radio"
            name="baseline"
            aria-label={`Use ${run.id} as baseline`}
            checked={isBaseline}
            disabled={!checked}
            onChange={onBaseline}
          />
        </td>
        <td className="history-run-id">
          {run.id}
          <small>
            {run.source === "dashboard" ? "Dashboard" : "CLI"}
            {run.label ? ` · ${run.label}` : ""}
          </small>
        </td>
        <td>
          {run.started_at
            ? run.started_at
                .replace("T", " ")
                .replace(/\.\d+/, "")
                .replace("Z", "")
            : "—"}
        </td>
        <td className="catalog-settings">{runSettings(run)}</td>
        <td>
          <Status status={run.status} />
          <small>{integrityText[run.integrity]}</small>
        </td>
        <td>
          {run.phases.length ? (
            run.phases.map((p) => (
              <span className="history-phase" key={p.key}>
                {p.key} {fmt(p.iops, 1)}
              </span>
            ))
          ) : (
            <span className="subtle">No phase totals</span>
          )}
        </td>
        <td>
          <button aria-expanded={open} onClick={onOpen}>
            {open ? "Hide" : "Details"}
          </button>
        </td>
      </tr>
      {open && (
        <tr className="catalog-detail">
          <td colSpan={8}>
            {run.issues.length > 0 && (
              <ul className="catalog-issues">
                {run.issues.map((i) => (
                  <li key={i}>{i}</li>
                ))}
              </ul>
            )}
            <dl className="record-grid">
              <div>
                <dt>Endpoint host</dt>
                <dd>{run.endpoint_host ?? "—"}</dd>
              </div>
              <div>
                <dt>S3 driver</dt>
                <dd>{run.s3_driver ?? "—"}</dd>
              </div>
              <div>
                <dt>CLI / engine</dt>
                <dd>
                  {run.cli_version ?? "—"} / {run.engine_version ?? "—"}
                </dd>
              </div>
              <div>
                <dt>Summary file</dt>
                <dd>{run.summary_file ?? "—"}</dd>
              </div>
            </dl>
            {run.phases.length > 0 && (
              <div className="table-wrap">
                <table className="run-history-table">
                  <thead>
                    <tr>
                      <th>Phase</th>
                      <th>Successful</th>
                      <th>Failed</th>
                      <th>Corrupt</th>
                      <th>IOPS</th>
                      <th>MiB/s</th>
                      <th>P50 µs</th>
                      <th>P99 µs</th>
                    </tr>
                  </thead>
                  <tbody>
                    {run.phases.map((p) => (
                      <tr key={p.key}>
                        <td>{p.key}</td>
                        <td>{fmt(p.successful_ops)}</td>
                        <td>{fmt(p.failed_ops)}</td>
                        <td>{fmt(p.corrupt_ops)}</td>
                        <td>{fmt(p.iops, 2)}</td>
                        <td>{fmt(p.bandwidth_mib_s, 2)}</td>
                        <td>{fmt(p.p50_latency_us, 1)}</td>
                        <td>{fmt(p.p99_latency_us, 1)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            {run.status !== "malformed" && (
              <RunLog
                base={`/api/catalog/log?id=${encodeURIComponent(run.id)}`}
                title="Engine logs"
              />
            )}
          </td>
        </tr>
      )}
    </>
  );
}

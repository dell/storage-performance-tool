import { useEffect, useState } from "react";
import type { LogLevel, LogResponse } from "../types";
import { logURL } from "../utils/catalog";

const levels: { value: LogLevel; label: string }[] = [
  { value: "all", label: "All" },
  { value: "warn", label: "WARN+" },
  { value: "error", label: "ERROR" },
];

/**
 * Read-only log viewer. `base` is /api/catalog/log?id=… or /api/runs/{id}/log.
 * Dashboard runs can switch between the console capture (interim, while SPT runs)
 * and the engine's results logs.
 */
export function RunLog({
  base,
  sources,
  title = "Run log",
  refreshMs = 0,
  collapsed = false,
}: {
  base: string;
  sources?: ("console" | "results")[];
  title?: string;
  refreshMs?: number;
  /** Render inside a closed <details> so long traces stay hidden until asked for. */
  collapsed?: boolean;
}) {
  const [open, setOpen] = useState(!collapsed);
  const [level, setLevel] = useState<LogLevel>("all");
  const [source, setSource] = useState(sources?.[0] ?? "");
  const [data, setData] = useState<LogResponse | null>(null);
  const [error, setError] = useState("");
  const [version, setVersion] = useState(0);
  useEffect(() => {
    if (!open) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const load = async () => {
      try {
        const r = await fetch(
          logURL(base, level, 200, source ? { source } : {}),
          { cache: "no-store", signal: controller.signal },
        );
        const body = await r.json();
        if (!r.ok) throw new Error(body.error ?? "Could not read the log.");
        setData(body as LogResponse);
        setError("");
      } catch (e) {
        if (!controller.signal.aborted) {
          setData(null);
          setError(e instanceof Error ? e.message : "Could not read the log.");
        }
      }
      if (refreshMs > 0 && !controller.signal.aborted)
        timer = setTimeout(load, refreshMs);
    };
    void load();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [base, level, source, refreshMs, version, open]);
  const files = [...new Set(data?.lines.map((l) => l.file) ?? [])];
  const body = (
    <>
      <div className="run-log-heading">
        {!collapsed && <h3>{title}</h3>}
        <div className="mode-switch" role="group" aria-label="Log level">
          {levels.map((l) => (
            <button
              key={l.value}
              className={level === l.value ? "active" : ""}
              aria-pressed={level === l.value}
              onClick={() => setLevel(l.value)}
            >
              {l.label}
            </button>
          ))}
        </div>
        {sources && sources.length > 1 && (
          <div className="mode-switch" role="group" aria-label="Log source">
            {sources.map((s) => (
              <button
                key={s}
                className={source === s ? "active" : ""}
                aria-pressed={source === s}
                onClick={() => setSource(s)}
              >
                {s === "console" ? "Console (interim)" : "Results logs"}
              </button>
            ))}
          </div>
        )}
        <button onClick={() => setVersion((v) => v + 1)}>Reload</button>
      </div>
      {data?.interim && (
        <p className="form-help">
          Interim console output captured by the dashboard. The engine’s results
          logs are the record once the run finishes.
        </p>
      )}
      {error ? (
        <p className="subtle">{error}</p>
      ) : !data ? (
        <p className="subtle">Loading log…</p>
      ) : data.lines.length === 0 ? (
        <p className="subtle">
          {level === "all"
            ? "No log lines recorded."
            : `No ${level === "warn" ? "warning or error" : "error"} lines recorded.`}
        </p>
      ) : (
        <pre className="log-lines" tabIndex={0}>
          {data.lines.map((l, i) => (
            <span key={i} className={`log-line log-${l.level}`}>
              {l.text}
              {"\n"}
            </span>
          ))}
        </pre>
      )}
      {data?.truncated && (
        <p className="form-help">
          Showing the end of a large log; older lines were not read.
        </p>
      )}
      {files.length > 0 && (
        <p className="form-help">From: {files.join(", ")}</p>
      )}
      <p className="form-help">
        Credential-looking values are redacted. Showing up to the last 200
        matching lines.
      </p>
    </>
  );
  return collapsed ? (
    <details
      className="panel run-log"
      aria-label={title}
      onToggle={(e) => setOpen((e.currentTarget as HTMLDetailsElement).open)}
    >
      <summary>{title}</summary>
      {open && body}
    </details>
  ) : (
    <section className="panel run-log" aria-label={title}>
      {body}
    </section>
  );
}

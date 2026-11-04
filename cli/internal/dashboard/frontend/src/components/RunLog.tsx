import { Button, Text, Title } from "@mantine/core";
import { useState } from "react";
import type { LogLevel, LogResponse } from "../types";
import { useLog } from "../api/queries";

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
  const [source, setSource] = useState<LogResponse["source"] | "">(
    sources?.[0] ?? "",
  );
  // Unmount the query observer on close so its in-flight request is cancelled.
  const body = open ? (
    <LogContents
      base={base}
      sources={sources}
      title={title}
      refreshMs={refreshMs}
      collapsed={collapsed}
      level={level}
      source={source}
      setLevel={setLevel}
      setSource={setSource}
    />
  ) : null;
  return collapsed ? (
    <details
      data-spt-legacy=""
      className="panel run-log"
      aria-label={title}
      onToggle={(e) => setOpen((e.currentTarget as HTMLDetailsElement).open)}
    >
      <summary data-spt-legacy="">{title}</summary>
      {open && body}
    </details>
  ) : (
    <section data-spt-legacy="" className="panel run-log" aria-label={title}>
      {body}
    </section>
  );
}

function LogContents({
  base,
  sources,
  title,
  refreshMs,
  collapsed,
  level,
  source,
  setLevel,
  setSource,
}: {
  base: string;
  sources?: LogResponse["source"][];
  title: string;
  refreshMs: number;
  collapsed: boolean;
  level: LogLevel;
  source: LogResponse["source"] | "";
  setLevel: (level: LogLevel) => void;
  setSource: (source: LogResponse["source"] | "") => void;
}) {
  const log = useLog(base, level, source, refreshMs);
  const data = log.isError ? undefined : log.data;
  const error = log.error?.message ?? "";
  const files = [...new Set(data?.lines.map((l) => l.file) ?? [])];
  return (
    <>
      <div data-spt-legacy="" className="run-log-heading">
        {!collapsed && (
          <Title order={3} className="flex-auto">
            {title}
          </Title>
        )}
        <div
          data-spt-legacy=""
          className="mode-switch"
          role="group"
          aria-label="Log level"
        >
          {levels.map((l) => (
            <button
              data-spt-legacy=""
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
          <div
            data-spt-legacy=""
            className="mode-switch"
            role="group"
            aria-label="Log source"
          >
            {sources.map((s) => (
              <button
                data-spt-legacy=""
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
        <Button onClick={() => void log.refetch()}>Reload</Button>
      </div>
      {data?.interim && (
        <Text size="sm" className="my-comfortable">
          Interim console output captured by the dashboard. The engine’s results
          logs are the record once the run finishes.
        </Text>
      )}
      {error ? (
        <Text size="xs" c="var(--spt-text-subtle)">
          {error}
        </Text>
      ) : !data ? (
        <Text size="xs" c="var(--spt-text-subtle)">
          Loading log…
        </Text>
      ) : data.lines.length === 0 ? (
        <Text size="xs" c="var(--spt-text-subtle)">
          {level === "all"
            ? "No log lines recorded."
            : `No ${level === "warn" ? "warning or error" : "error"} lines recorded.`}
        </Text>
      ) : (
        <pre data-spt-legacy="" className="log-lines" tabIndex={0}>
          {data.lines.map((l, i) => (
            <span
              data-spt-legacy=""
              key={i}
              className={`log-line log-${l.level}`}
            >
              {l.text}
              {"\n"}
            </span>
          ))}
        </pre>
      )}
      {data?.truncated && (
        <Text size="sm" className="my-comfortable">
          Showing the end of a large log; older lines were not read.
        </Text>
      )}
      {files.length > 0 && (
        <Text size="sm" className="my-comfortable">
          From: {files.join(", ")}
        </Text>
      )}
      <Text size="sm" className="my-comfortable">
        Credential-looking values are redacted. Showing up to the last 200
        matching lines.
      </Text>
    </>
  );
}

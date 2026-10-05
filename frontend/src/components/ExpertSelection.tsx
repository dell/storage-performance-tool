import { useState } from "react";
export const expertOptions = [
  { id: "throughput", label: "Throughput KPI" },
  { id: "bandwidth", label: "Bandwidth KPI" },
  { id: "latency", label: "P99 / P50 latency KPI" },
  { id: "failures", label: "Failure KPI" },
  { id: "concurrency", label: "Concurrency KPI" },
  { id: "success", label: "Success count KPI" },
  { id: "core", label: "Core KPIs per operation / step" },
  { id: "fleet", label: "Fleet telemetry and chart controls" },
  { id: "clients", label: "Client comparison" },
  {
    id: "timings",
    label: "Full timing distributions, concurrency and integrity failures",
  },
  { id: "delete", label: "Complete DELETE metrics" },
  { id: "mixed", label: "Mixed configured / observed shares" },
  { id: "discovery", label: "LIST / shard discovery metrics" },
  { id: "raw", label: "Raw normalized snapshot" },
] as const;
export type ExpertField = (typeof expertOptions)[number]["id"];
export const allExpertFields: ExpertField[] = expertOptions.map((o) => o.id);
export function ExpertSelection({
  initial,
  onApply,
}: {
  initial: ExpertField[];
  onApply: (fields: ExpertField[]) => void;
}) {
  const [draft, setDraft] = useState(initial);
  return (
    <section className="panel expert-selection">
      <span className="eyebrow">EXPERT VIEW</span>
      <h1>Choose the data you want to see</h1>
      <p>
        Select metrics and sections, then apply your selection. DELETE, Mixed
        and LIST sections appear only when relevant data exists.
      </p>
      <div className="selection-actions">
        <button onClick={() => setDraft([...allExpertFields])}>
          Select all
        </button>
        <button onClick={() => setDraft([])}>Clear</button>
      </div>
      <div className="expert-check-grid">
        {expertOptions.map((o) => (
          <label className="check-label" key={o.id}>
            <input
              type="checkbox"
              checked={draft.includes(o.id)}
              onChange={(e) =>
                setDraft(
                  e.target.checked
                    ? [...draft, o.id]
                    : draft.filter((id) => id !== o.id),
                )
              }
            />
            {o.label}
          </label>
        ))}
      </div>
      <button
        className="primary"
        disabled={!draft.length}
        onClick={() => onApply(draft)}
      >
        Apply selection · {draft.length} selected
      </button>
    </section>
  );
}

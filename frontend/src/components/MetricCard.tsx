export function MetricCard({
  label,
  value,
  unit,
  detail,
  accent = "blue",
}: {
  label: string;
  value: string;
  unit?: string;
  detail: string;
  accent?: string;
}) {
  return (
    <article className={`metric-card ${accent}`}>
      <span className="metric-label">{label}</span>
      <div className="metric-value">
        {value}
        <small>{unit}</small>
      </div>
      <p>{detail}</p>
    </article>
  );
}

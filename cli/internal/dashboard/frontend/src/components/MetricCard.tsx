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
    <article data-spt-legacy="" className={`metric-card ${accent}`}>
      <span data-spt-legacy="" className="metric-label">
        {label}
      </span>
      <div data-spt-legacy="" className="metric-value">
        {value}
        <small data-spt-legacy="">{unit}</small>
      </div>
      <p data-spt-legacy="">{detail}</p>
    </article>
  );
}

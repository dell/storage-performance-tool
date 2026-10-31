import type { CheckStatus, Verification } from "../types";

const fill: Record<CheckStatus, string> = {
  pending: "#ffffff",
  passed: "#2a9d74",
  failed: "#d0503c",
};
const word: Record<CheckStatus, string> = {
  pending: "Checking…",
  passed: "Passed",
  failed: "Failed",
};
const short = (text: string) =>
  text.length > 30 ? `${text.slice(0, 29)}…` : text;

function Node({
  x,
  y,
  label,
  status,
}: {
  x: number;
  y: number;
  label: string;
  status: CheckStatus;
}) {
  return (
    <g data-spt-legacy="">
      <title data-spt-legacy="">{`${label}: ${word[status]}`}</title>
      <circle
        data-spt-legacy=""
        cx={x}
        cy={y}
        r="13"
        fill={fill[status]}
        stroke={status === "pending" ? "#b7c3d3" : fill[status]}
        strokeWidth="2"
      />
      {status !== "pending" && (
        <text
          data-spt-legacy=""
          x={x}
          y={y + 4}
          textAnchor="middle"
          className="graph-mark"
        >
          {status === "passed" ? "✓" : "✕"}
        </text>
      )}
      <text
        data-spt-legacy=""
        x={x}
        y={y + 30}
        textAnchor="middle"
        className="graph-label"
      >
        {short(label)}
      </text>
      <text
        data-spt-legacy=""
        x={x}
        y={y + 43}
        textAnchor="middle"
        className="graph-status"
      >
        {word[status]}
      </text>
    </g>
  );
}

export function ConnectionGraph({
  verification: v,
}: {
  verification: Verification;
}) {
  const row = 66,
    height = Math.max(1, v.nodes.length) * row + 14,
    middle = height / 2 - 14,
    left = 90,
    center = 360,
    right = 630;
  const y = (i: number) => 24 + i * row;
  const passed = v.nodes.filter((n) => n.status === "passed").length;
  return (
    <figure data-spt-legacy="" className="connection-graph">
      <svg
        data-spt-legacy=""
        viewBox={`0 0 720 ${height}`}
        role="img"
        aria-label={`Connection check: ${passed} of ${v.nodes.length} client machines passed; S3 endpoint ${word[v.endpoint.status].toLowerCase()}`}
      >
        {v.nodes.map((n, i) => (
          <g data-spt-legacy="" key={n.host} className="graph-line">
            <line
              data-spt-legacy=""
              x1={left}
              y1={middle}
              x2={center}
              y2={y(i)}
            />
            <line
              data-spt-legacy=""
              x1={center}
              y1={y(i)}
              x2={right}
              y2={middle}
            />
          </g>
        ))}
        <g data-spt-legacy="">
          <title data-spt-legacy="">Dashboard host</title>
          <circle
            data-spt-legacy=""
            cx={left}
            cy={middle}
            r="13"
            fill="#eaf2fd"
            stroke="#176aca"
            strokeWidth="2"
          />
          <text
            data-spt-legacy=""
            x={left}
            y={middle + 30}
            textAnchor="middle"
            className="graph-label"
          >
            Dashboard host
          </text>
        </g>
        {v.nodes.map((n, i) => (
          <Node
            key={n.host}
            x={center}
            y={y(i)}
            label={n.host}
            status={n.status}
          />
        ))}
        <Node
          x={right}
          y={middle}
          label={v.endpoint.label || "S3 endpoint"}
          status={v.endpoint.status}
        />
      </svg>
      <figcaption data-spt-legacy="">
        Client machines are checked by SPT. The S3 endpoint is checked for
        reachability from the dashboard host.
      </figcaption>
    </figure>
  );
}

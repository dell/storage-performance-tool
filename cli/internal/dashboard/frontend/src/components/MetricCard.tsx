import { Box, Paper, Text } from "@mantine/core";

const accentColors: Record<string, string> = {
  blue: "var(--spt-chart-throughput)",
  teal: "var(--spt-chart-bandwidth)",
  purple: "var(--spt-chart-latency-p50)",
  orange: "#d18930",
};

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
    <Paper
      component="article"
      className="relative overflow-hidden p-[21px] [@media(max-width:1150px)]:p-md [@media(max-width:760px)]:p-comfortable"
    >
      <Box
        component="span"
        aria-hidden="true"
        bg={accentColors[accent] ?? accentColors.blue}
        className="absolute top-[23px] left-0 h-[30px] w-[3px]"
      />
      <Text
        component="span"
        size="caption"
        c="var(--spt-text-muted)"
        lh="normal"
      >
        {label}
      </Text>
      <Text
        component="div"
        fw="var(--mantine-font-weight-heading)"
        className="mt-[14px] mb-[11px] text-metric tracking-[-1px] whitespace-nowrap tabular-nums [@media(min-width:1500px)]:text-metric-wide [@media(max-width:1150px)]:text-metric-compact [@media(max-width:760px)]:text-metric-mobile [@media(max-width:760px)]:whitespace-normal"
        c="var(--spt-text-primary)"
        lh="normal"
      >
        {value}
        <Text
          component="small"
          size="sm"
          c="var(--spt-text-subtle)"
          className="ml-[7px]"
          lh="normal"
        >
          {unit}
        </Text>
      </Text>
      <Text size="xs" className="[@media(max-width:760px)]:text-caption">
        {detail}
      </Text>
    </Paper>
  );
}

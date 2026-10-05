export function failurePercent(
  success: number,
  failures: number,
): number | null {
  const total = success + failures;
  return total === 0 ? null : (failures / total) * 100;
}
export function microsecondsToMilliseconds(value: number): number {
  return value / 1000;
}
export function formatNumber(value: number | null, digits = 0): string {
  return value === null
    ? "—"
    : new Intl.NumberFormat("en-US", { maximumFractionDigits: digits }).format(
        value,
      );
}
export function formatElapsed(seconds: number): string {
  return `${Math.floor(seconds / 60)
    .toString()
    .padStart(2, "0")}:${Math.floor(seconds % 60)
    .toString()
    .padStart(2, "0")}`;
}

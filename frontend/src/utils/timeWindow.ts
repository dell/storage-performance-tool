export function clampWindow(
  center: number,
  width: number,
  min: number,
  max: number,
): [number, number] {
  const available = Math.max(0, max - min),
    size = Math.min(available, Math.max(Math.min(1, available), width));
  const start = Math.max(min, Math.min(center - size / 2, max - size));
  return [start, start + size];
}
export function zoomWindow(
  current: [number, number],
  factor: number,
  min: number,
  max: number,
): [number, number] {
  return clampWindow(
    (current[0] + current[1]) / 2,
    (current[1] - current[0]) * factor,
    min,
    max,
  );
}
export function panWindow(
  current: [number, number],
  fraction: number,
  min: number,
  max: number,
): [number, number] {
  return clampWindow(
    (current[0] + current[1]) / 2 + (current[1] - current[0]) * fraction,
    current[1] - current[0],
    min,
    max,
  );
}

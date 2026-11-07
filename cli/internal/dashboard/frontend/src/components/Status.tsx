export function Status({ status }: { status: string }) {
  return (
    <span
      data-spt-legacy=""
      className={`status ${["completed", "complete", "ready", "passed"].includes(status) ? "good" : "warn"}`}
    >
      <i data-spt-legacy="" />
      {status}
    </span>
  );
}

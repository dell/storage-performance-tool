export function Status({ status }: { status: string }) {
  return (
    <span
      className={`status ${["completed", "complete", "ready", "passed"].includes(status) ? "good" : "warn"}`}
    >
      <i />
      {status}
    </span>
  );
}

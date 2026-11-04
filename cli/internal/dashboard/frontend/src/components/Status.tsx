import { Badge } from "@mantine/core";

export function Status({ status }: { status: string }) {
  const good = ["completed", "complete", "ready", "passed"].includes(status);
  return (
    <Badge
      color={good ? "teal" : "yellow"}
      leftSection={
        <span
          className="size-[5px] rounded-full bg-current"
          aria-hidden="true"
        />
      }
    >
      {status}
    </Badge>
  );
}

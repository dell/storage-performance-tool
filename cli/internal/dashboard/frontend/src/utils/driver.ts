import type { Driver, State } from "../types";

/**
 * Whether the selected driver may start. S3 needs the full storage check to pass;
 * a mock run never contacts S3, so only every client machine must have passed.
 */
export function driverReadiness(
  driver: Driver,
  state: State | null,
  storageDirty: boolean,
): { verified: boolean; targetReady: boolean } {
  const v = state?.storage?.verification;
  const verified =
    !storageDirty &&
    (driver === "mock"
      ? !!v?.nodes.length &&
        v.status !== "running" &&
        v.nodes.every((n) => n.status === "passed")
      : v?.status === "passed");
  const targetReady =
    driver === "mock" ? !!state?.target.mock_ready : !!state?.target.ready;
  return { verified, targetReady };
}

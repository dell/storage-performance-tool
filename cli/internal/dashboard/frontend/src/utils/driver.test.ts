import { describe, expect, it } from "vitest";
import type { State, Verification } from "../types";
import { driverReadiness } from "./driver";

const node = (status: "passed" | "failed") => ({
  host: "n",
  status,
  message: "",
  checks: [],
});
const state = (
  v: Partial<Verification>,
  target: Partial<State["target"]> = {},
) =>
  ({
    target: { ready: false, mock_ready: true, ...target },
    storage: {
      hosts: ["n"],
      endpoint: "",
      max_hosts: 8,
      verification: {
        status: "failed",
        summary: "",
        nodes: [node("passed")],
        endpoint: { label: "", status: "failed", message: "" },
        ...v,
      },
    },
  }) as unknown as State;

describe("driverReadiness", () => {
  it("lets a mock run start when nodes pass but the S3 endpoint failed", () => {
    expect(driverReadiness("mock", state({}), false)).toEqual({
      verified: true,
      targetReady: true,
    });
    expect(driverReadiness("s3", state({}), false)).toEqual({
      verified: false,
      targetReady: false,
    });
  });
  it("blocks a mock run when any node failed, none ran, or a check is running", () => {
    expect(
      driverReadiness(
        "mock",
        state({ nodes: [node("passed"), node("failed")] }),
        false,
      ).verified,
    ).toBe(false);
    expect(driverReadiness("mock", state({ nodes: [] }), false).verified).toBe(
      false,
    );
    expect(
      driverReadiness("mock", state({ status: "running" }), false).verified,
    ).toBe(false);
  });
  it("requires re-verification after the storage form changes", () => {
    expect(
      driverReadiness("s3", state({ status: "passed" }, { ready: true }), true)
        .verified,
    ).toBe(false);
    expect(
      driverReadiness(
        "s3",
        state({ status: "passed" }, { ready: true }),
        false,
      ),
    ).toEqual({
      verified: true,
      targetReady: true,
    });
  });
  it("uses mock_ready for the mock target", () => {
    expect(
      driverReadiness("mock", state({}, { mock_ready: false }), false)
        .targetReady,
    ).toBe(false);
    expect(driverReadiness("mock", null, false)).toEqual({
      verified: false,
      targetReady: false,
    });
  });
});

import { describe, expect, it } from "vitest";
import { formatMetric, formatNumber } from "./metrics";

describe("formatMetric", () => {
  it("keeps measured zero distinct from no data and delayed", () => {
    expect(formatMetric(0, 0, "present")).toBe("0");
    expect(formatMetric(0, 0)).toBe("0");
    expect(formatMetric(null, 0, "no_data")).toBe("No data");
    expect(formatMetric(undefined, 0, "no_data")).toBe("No data");
    expect(formatMetric(null, 0, "delayed")).toBe("Delayed");
    expect(formatMetric(12.5, 1, "delayed")).toBe("12.5 (delayed)");
    expect(formatNumber(null)).toBe("—");
  });

  it("still marks delayed when an old measurement value exists", () => {
    // Live cards pass data_status whenever is_active, even if a stale row remains.
    const rowValue = 12.5;
    const isActive = true;
    const dataStatus = "delayed" as const;
    const status = isActive ? dataStatus : undefined;
    expect(formatMetric(rowValue, 1, status)).toBe("12.5 (delayed)");
  });
});

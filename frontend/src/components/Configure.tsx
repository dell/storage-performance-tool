import { useState } from "react";
import type { Operation, RunConfig } from "../types";
import { defaultDeleteOptions } from "../data/deleteMock";
import type { DeleteOptions } from "../types";
import { isMock } from "../services";
export function Configure({
  initial,
  busy,
  onStart,
}: {
  initial: RunConfig;
  busy: boolean;
  onStart: (config: RunConfig) => Promise<void>;
}) {
  const [config, setConfig] = useState(initial),
    [review, setReview] = useState(false);
  return (
    <section className="panel configure">
      <span className="eyebrow">RUN SETUP</span>
      <h1>Configure a benchmark</h1>
      <p>Choose the workload. Review the settings before starting.</p>
      <form
        onSubmit={(e) => {
          e.preventDefault();
          if (review) void onStart(config);
          else setReview(true);
        }}
      >
        <div className="form-grid">
          <label>
            Run name
            <input
              required
              maxLength={100}
              value={config.name}
              onChange={(e) => {
                setReview(false);
                setConfig({ ...config, name: e.target.value });
              }}
            />
          </label>
          <label>
            Operation
            <select
              value={config.operation}
              onChange={(e) => {
                setReview(false);
                setConfig({
                  ...config,
                  operation: e.target.value as Operation,
                  unbounded:
                    e.target.value === "DELETE" ? false : config.unbounded,
                });
              }}
            >
              {["CREATE", "READ", "STAT", "DELETE", "MIXED"].map((o) => (
                <option key={o}>{o}</option>
              ))}
            </select>
          </label>
          {(
            [
              {
                key: "durationSeconds",
                label: "Duration · seconds",
                min: 30,
                max: 3600,
              },
              {
                key: "threadsPerClient",
                label: "Threads per client",
                min: 1,
                max: 512,
              },
              { key: "expectedNodes", label: "Clients", min: 1, max: 12 },
              {
                key: "objectSizeMiB",
                label: "Object size · MiB",
                min: 1,
                max: 64,
              },
            ] as const
          ).map((f) => (
            <label key={f.key}>
              {f.label}
              <input
                type="number"
                required
                min={f.min}
                max={f.max}
                step="1"
                value={config[f.key]}
                onChange={(e) => {
                  setReview(false);
                  setConfig({ ...config, [f.key]: Number(e.target.value) });
                }}
              />
            </label>
          ))}
        </div>
        {config.operation !== "DELETE" && (
          <label className="check-label unbounded-option">
            <input
              type="checkbox"
              checked={config.unbounded ?? false}
              onChange={(e) => {
                setReview(false);
                setConfig({ ...config, unbounded: e.target.checked });
              }}
            />
            Unbounded run (no completion target)
          </label>
        )}
        <fieldset className="delete-config">
          <legend>Shard discovery metrics</legend>
          <label className="check-label">
            <input
              type="checkbox"
              checked={config.shardDiscovery?.enabled ?? false}
              onChange={(e) => {
                setReview(false);
                setConfig({
                  ...config,
                  shardDiscovery: {
                    enabled: e.target.checked,
                    simulateStall:
                      config.shardDiscovery?.simulateStall ?? false,
                  },
                });
              }}
            />
            Enable LIST / shard discovery metrics
          </label>
          {config.shardDiscovery?.enabled && (
            <div className="discovery-config">
              <label className="check-label">
                <input
                  type="checkbox"
                  checked={config.shardDiscovery.simulateStall}
                  onChange={(e) => {
                    setReview(false);
                    setConfig({
                      ...config,
                      shardDiscovery: {
                        enabled: true,
                        simulateStall: e.target.checked,
                      },
                    });
                  }}
                />
                Simulate a stalled prefix (mock only)
              </label>
              <p>
                Collecting is simulated. The mock discovery dataset is
                independent from the selected benchmark workload.
              </p>
            </div>
          )}
        </fieldset>
        {config.operation === "MIXED" && (
          <div className="review">
            <h3>Mixed workload · prototype</h3>
            <p>
              Fixed configured shares: READ 60% · CREATE 25% · STAT 10% · DELETE
              5%.
            </p>
            <p>
              This prototype compares cumulative attempted operation counts.
              Editing shares and executing real mixed workloads are not
              implemented.
            </p>
          </div>
        )}
        {config.operation === "DELETE" && (
          <fieldset className="delete-config">
            <legend>DELETE options</legend>
            <div className="form-grid">
              {(
                [
                  {
                    key: "batchSize",
                    label: "Configured batch size",
                    min: 1,
                    max: 1000,
                    step: 1,
                  },
                  {
                    key: "bucketCount",
                    label: "Buckets in mock dataset",
                    min: 1,
                    max: 500,
                    step: 1,
                  },
                  {
                    key: "allowedFailurePercent",
                    label: "Allowed object failure · %",
                    min: 0,
                    max: 100,
                    step: 0.01,
                  },
                ] as const
              ).map((f) => (
                <label key={f.key}>
                  {f.label}
                  <input
                    required
                    type="number"
                    min={f.min}
                    max={f.max}
                    step={f.step}
                    value={
                      (config.deleteOptions ?? defaultDeleteOptions)[f.key]
                    }
                    onChange={(e) => {
                      setReview(false);
                      setConfig({
                        ...config,
                        deleteOptions: {
                          ...defaultDeleteOptions,
                          ...config.deleteOptions,
                          [f.key]: Number(e.target.value),
                        },
                      });
                    }}
                  />
                </label>
              ))}
              <label>
                Mock scenario
                <select
                  value={
                    config.deleteOptions?.scenario ??
                    defaultDeleteOptions.scenario
                  }
                  onChange={(e) => {
                    setReview(false);
                    setConfig({
                      ...config,
                      deleteOptions: {
                        ...defaultDeleteOptions,
                        ...config.deleteOptions,
                        scenario: e.target.value as DeleteOptions["scenario"],
                      },
                    });
                  }}
                >
                  <option value="clean">Clean deletion</option>
                  <option value="within-budget">
                    Some failures · default budget allows them
                  </option>
                  <option value="over-budget">
                    Many failures · default budget exceeded
                  </option>
                  <option value="verification-unresolved">
                    Unresolved verification
                  </option>
                </select>
              </label>
            </div>
            <p>Scenario and bucket count configure synthetic data only.</p>
          </fieldset>
        )}
        {review && (
          <div className="review">
            <h3>Review run</h3>
            <p>
              {config.operation} · {config.expectedNodes} clients ×{" "}
              {config.threadsPerClient} threads ·{" "}
              {config.unbounded ? "unbounded" : `${config.durationSeconds}s`}
            </p>
            <p>
              {isMock
                ? "This starts a synthetic local dataset. No command is executed and no storage is contacted."
                : "This sends a start request to your configured backend."}
            </p>
          </div>
        )}
        <button className="primary" disabled={busy}>
          {busy
            ? "Starting…"
            : review
              ? isMock
                ? "Start mock run"
                : "Start run"
              : "Review settings"}
        </button>
      </form>
    </section>
  );
}

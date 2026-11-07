import { useState } from "react";
import type { State } from "../types";
import { readJSON } from "../utils/validation";

export function ConnectRun({
  state,
  offline,
  onConnected,
}: {
  state: State | null;
  offline: boolean;
  onConnected: () => Promise<void>;
}) {
  const hosts = state?.storage?.hosts ?? [];
  const attached = state?.connection?.mode === "attached";
  const [host, setHost] = useState(hosts[0] ?? "");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const nextHost =
    attached && state?.connection?.host
      ? state.connection.host
      : !hosts.includes(host) && hosts.length
        ? hosts[0]
        : host;
  if (host !== nextHost) setHost(nextHost);

  const connect = async () => {
    setError("");
    setBusy(true);
    try {
      const response = await fetch("/api/attach", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ host }),
      });
      const body = await readJSON(
        response,
        "Could not connect to the SPT run.",
      );
      if (!response.ok)
        throw new Error(body.error ?? "Could not connect to the SPT run.");
      await onConnected();
    } catch (reason) {
      setError(
        reason instanceof Error
          ? reason.message
          : "Could not connect to the SPT run. Check the API and retry.",
      );
    } finally {
      setBusy(false);
    }
  };

  const disconnect = async () => {
    setError("");
    setBusy(true);
    try {
      const response = await fetch("/api/attach", { method: "DELETE" });
      const body = await readJSON(
        response,
        "Could not disconnect from the run.",
      );
      if (!response.ok)
        throw new Error(body.error ?? "Could not disconnect from the run.");
      await onConnected();
    } catch (reason) {
      setError(
        reason instanceof Error ? reason.message : "Could not disconnect.",
      );
    } finally {
      setBusy(false);
    }
  };

  return (
    <>
      <div data-spt-legacy="" className="page-heading">
        <div data-spt-legacy="">
          <span data-spt-legacy="" className="eyebrow">
            READ ONLY MONITORING
          </span>
          <h1 data-spt-legacy="">Connect to an active run</h1>
          <p data-spt-legacy="">
            Follow a run started from the SPT CLI. This connection reads live
            status and measurements and does not start or stop the benchmark.
          </p>
        </div>
      </div>
      {error && (
        <div data-spt-legacy="" className="error" role="alert">
          <strong data-spt-legacy="">Could not connect. </strong>
          {error}
          {hosts.includes(host) && (
            <div
              data-spt-legacy=""
              className="actions"
              style={{ marginTop: 12 }}
            >
              <button
                data-spt-legacy=""
                onClick={() => void connect()}
                disabled={busy || offline}
              >
                Retry connection
              </button>
            </div>
          )}
        </div>
      )}
      <section data-spt-legacy="" className="panel connect-panel">
        {attached ? (
          <>
            <span data-spt-legacy="" className="eyebrow">
              CURRENT CONNECTION
            </span>
            <h2 data-spt-legacy="">Monitoring {state?.connection?.host}</h2>
            <p data-spt-legacy="">
              Run {state?.connection?.run_id ?? "—"} · Updates with the SPT
              status API and live metrics endpoint on port 9999.
            </p>
            <div data-spt-legacy="" className="connect-actions">
              <span data-spt-legacy="" className="connection-state">
                <i data-spt-legacy="" className="source-dot connected" />{" "}
                Connected · read only
              </span>
              <div data-spt-legacy="" className="actions">
                {offline && (
                  <button
                    data-spt-legacy=""
                    onClick={() => void connect()}
                    disabled={busy}
                  >
                    Retry connection
                  </button>
                )}
                <button
                  data-spt-legacy=""
                  onClick={() => void disconnect()}
                  disabled={busy}
                >
                  Disconnect
                </button>
              </div>
            </div>
          </>
        ) : (
          <>
            <span data-spt-legacy="" className="eyebrow">
              EXISTING SPT RUN
            </span>
            <h2 data-spt-legacy="">Choose its entry machine</h2>
            <p data-spt-legacy="">
              SPT’s entry machine serves the documented /status and
              /metrics/json APIs. Choose a client machine already saved in
              Storage configuration.
            </p>
            {hosts.length ? (
              <label data-spt-legacy="" className="connect-host">
                Entry machine
                <select
                  data-spt-legacy=""
                  value={host}
                  onChange={(event) => setHost(event.target.value)}
                  disabled={busy || offline}
                >
                  {hosts.map((item) => (
                    <option data-spt-legacy="" key={item} value={item}>
                      {item}
                    </option>
                  ))}
                </select>
              </label>
            ) : (
              <div data-spt-legacy="" className="review">
                <strong data-spt-legacy="">
                  No client machines are configured yet.
                </strong>
                <p data-spt-legacy="">
                  Add the SPT entry machine in Configure run → Storage
                  configuration, then return here.
                </p>
              </div>
            )}
            <div data-spt-legacy="" className="connect-actions">
              <span data-spt-legacy="" className="subtle">
                The run must already be active on the selected machine.
              </span>
              <button
                data-spt-legacy=""
                className="primary"
                onClick={() => void connect()}
                disabled={!hosts.includes(host) || busy || offline}
              >
                {busy ? "Connecting…" : "Connect to active run"}
              </button>
            </div>
          </>
        )}
      </section>
    </>
  );
}

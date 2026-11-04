import { Button, NativeSelect, Text, Title } from "@mantine/core";
import { useState } from "react";
import type { State } from "../types";
import { useAttachRun, useDetachRun } from "../api/mutations";

export function ConnectRun({
  state,
  offline,
  onConnected,
}: {
  state: State | null;
  offline: boolean;
  onConnected: () => void;
}) {
  const hosts = state?.storage?.hosts ?? [];
  const attached = state?.connection?.mode === "attached";
  const [host, setHost] = useState(hosts[0] ?? "");
  const attach = useAttachRun();
  const detach = useDetachRun();
  const busy = attach.isPending || detach.isPending;
  const error = attach.error?.message || detach.error?.message || "";
  const nextHost =
    attached && state?.connection?.host
      ? state.connection.host
      : !hosts.includes(host) && hosts.length
        ? hosts[0]
        : host;
  if (host !== nextHost) setHost(nextHost);

  const connect = () => {
    if (busy || !hosts.includes(host)) return;
    detach.reset();
    attach.mutate(host, { onSuccess: onConnected });
  };

  const disconnect = () => {
    if (busy) return;
    attach.reset();
    detach.mutate(undefined, { onSuccess: onConnected });
  };

  return (
    <>
      <div data-spt-legacy="" className="page-heading">
        <div data-spt-legacy="">
          <span data-spt-legacy="" className="eyebrow">
            READ ONLY MONITORING
          </span>
          <Title order={1}>Connect to an active run</Title>
          <Text size="sm" className="mt-xs wrap-anywhere">
            Follow a run started from the SPT CLI. This connection reads live
            status and measurements and does not start or stop the benchmark.
          </Text>
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
              <Button onClick={() => void connect()} disabled={busy || offline}>
                Retry connection
              </Button>
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
            <Title order={2} className="mt-compact">
              Monitoring {state?.connection?.host}
            </Title>
            <Text className="mt-xs max-w-[650px]">
              Run {state?.connection?.run_id ?? "—"} · Updates with the SPT
              status API and live metrics endpoint on port 9999.
            </Text>
            <div data-spt-legacy="" className="connect-actions">
              <span data-spt-legacy="" className="connection-state">
                <i data-spt-legacy="" className="source-dot connected" />{" "}
                Connected · read only
              </span>
              <div data-spt-legacy="" className="actions">
                {offline && (
                  <Button onClick={() => void connect()} disabled={busy}>
                    Retry connection
                  </Button>
                )}
                <Button onClick={() => void disconnect()} disabled={busy}>
                  Disconnect
                </Button>
              </div>
            </div>
          </>
        ) : (
          <>
            <span data-spt-legacy="" className="eyebrow">
              EXISTING SPT RUN
            </span>
            <Title order={2} className="mt-compact">
              Choose its entry machine
            </Title>
            <Text className="mt-xs max-w-[650px]">
              SPT’s entry machine serves the documented /status and
              /metrics/json APIs. Choose a client machine already saved in
              Storage configuration.
            </Text>
            {hosts.length ? (
              <NativeSelect
                label="Entry machine"
                className="my-[26px] max-w-[420px]"
                value={host}
                onChange={(event) => setHost(event.target.value)}
                disabled={busy || offline}
              >
                {hosts.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </NativeSelect>
            ) : (
              <div data-spt-legacy="" className="review">
                <strong data-spt-legacy="">
                  No client machines are configured yet.
                </strong>
                <Text size="sm" className="mt-xs">
                  Add the SPT entry machine in Configure run → Storage
                  configuration, then return here.
                </Text>
              </div>
            )}
            <div data-spt-legacy="" className="connect-actions">
              <span data-spt-legacy="" className="subtle">
                The run must already be active on the selected machine.
              </span>
              <Button
                variant="filled"
                loading={busy}
                onClick={() => void connect()}
                disabled={!hosts.includes(host) || busy || offline}
              >
                {busy ? "Connecting…" : "Connect to active run"}
              </Button>
            </div>
          </>
        )}
      </section>
    </>
  );
}

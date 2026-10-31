import { useEffect, useState } from "react";
import type { State } from "../types";
import { ConnectionGraph } from "./ConnectionGraph";
import { endpointError, hostErrors, readJSON } from "../utils/validation";

type Form = { hosts: string[]; endpoint: string };

export function StorageConfiguration({
  state,
  offline,
  onDirty,
  onRefresh,
}: {
  state: State | null;
  offline: boolean;
  onDirty: (dirty: boolean) => void;
  onRefresh: () => Promise<void>;
}) {
  const saved = state?.storage;
  const [form, setForm] = useState<Form | null>(null),
    [busy, setBusy] = useState(false),
    [error, setError] = useState("");
  // Copy the saved settings once; later polls must not overwrite what is being typed.
  if (saved && !form) {
    setForm({ hosts: [...saved.hosts], endpoint: saved.endpoint });
  }

  const entered = (form?.hosts ?? []).map((h) => h.trim()).filter(Boolean);
  const dirty =
    !!saved &&
    !!form &&
    (entered.join(",") !== saved.hosts.join(",") ||
      form.endpoint.trim() !== saved.endpoint);
  useEffect(() => {
    onDirty(dirty);
  }, [dirty, onDirty]);

  if (!saved || !form)
    return <p className="form-help">Waiting for the dashboard server…</p>;

  const v = saved.verification,
    running = v.status === "running",
    locked = busy || running,
    target = state!.target;
  const rowErrors = hostErrors(form.hosts, saved.max_hosts),
    urlError = endpointError(form.endpoint.trim()),
    invalid = rowErrors.some(Boolean) || !!urlError;
  const setHost = (index: number, value: string) =>
    setForm({
      ...form,
      hosts: form.hosts.map((h, i) => (i === index ? value : h)),
    });

  async function verify() {
    if (locked || !form) return;
    if (!entered.length) {
      setError("Add at least one client machine.");
      return;
    }
    if (invalid) {
      setError("Fix the highlighted fields before verifying.");
      return;
    }
    setBusy(true);
    setError("");
    try {
      const endpoint = form.endpoint.trim();
      const r = await fetch("/api/storage/verify", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ hosts: entered, endpoint }),
      });
      await readJSON(r, "Verification request failed.");
      setForm({ hosts: entered, endpoint });
      await onRefresh();
    } catch (e) {
      setError(
        e instanceof Error ? e.message : "Could not start verification.",
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <>
      <div className="target-summary">
        <div>
          <span>S3 endpoint in use</span>
          <strong>{target.endpoint_label || "Not configured"}</strong>
        </div>
        <div>
          <span>Bucket</span>
          <strong>{target.bucket || "Not reported"}</strong>
        </div>
        <span className={`status ${target.ready ? "good" : "warn"}`}>
          <i />
          {target.ready ? "ready" : "needs setup"}
        </span>
      </div>
      {!!target.reasons?.length && (
        <div className="notice">{target.reasons.join(" ")}</div>
      )}
      <form
        onSubmit={(e) => {
          e.preventDefault();
          void verify();
        }}
      >
        <fieldset className="plain-fieldset" disabled={locked}>
          <legend>Client machines</legend>
          <p className="form-help">
            The machines that generate load. Each needs Docker and key-based SSH
            access from the dashboard host. Enter <code>user@host</code>, or{" "}
            <code>127.0.0.1</code> for the dashboard host itself.
          </p>
          <ul className="host-list">
            {form.hosts.map((host, i) => (
              <li key={i}>
                <input
                  aria-label={`Client machine ${i + 1}`}
                  placeholder="user@hostname"
                  autoComplete="off"
                  spellCheck={false}
                  maxLength={300}
                  value={host}
                  aria-invalid={!!rowErrors[i]}
                  aria-describedby={
                    rowErrors[i] ? `host-error-${i}` : undefined
                  }
                  onChange={(e) => setHost(i, e.target.value)}
                />
                {rowErrors[i] && (
                  <small
                    id={`host-error-${i}`}
                    className="field-error host-error"
                  >
                    {rowErrors[i]}
                  </small>
                )}
                <button
                  type="button"
                  disabled={form.hosts.length === 1}
                  aria-label={`Remove client machine ${i + 1}`}
                  onClick={() =>
                    setForm({
                      ...form,
                      hosts: form.hosts.filter((_, j) => j !== i),
                    })
                  }
                >
                  Remove
                </button>
              </li>
            ))}
          </ul>
          <button
            type="button"
            disabled={form.hosts.length >= saved.max_hosts}
            onClick={() => setForm({ ...form, hosts: [...form.hosts, ""] })}
          >
            ＋ Add machine
          </button>
          <span className="subtle host-count">
            {form.hosts.length} of {saved.max_hosts}
          </span>
          <label className="endpoint-field">
            S3 endpoint (optional)
            <input
              type="url"
              placeholder={
                target.endpoint_source === "env" && target.endpoint
                  ? `${target.endpoint} (from the host)`
                  : "https://s3.example.com or http://127.0.0.1:9000"
              }
              autoComplete="off"
              spellCheck={false}
              maxLength={2048}
              value={form.endpoint}
              aria-invalid={!!urlError}
              aria-describedby={urlError ? "endpoint-error" : undefined}
              onChange={(e) => setForm({ ...form, endpoint: e.target.value })}
            />
            {urlError && (
              <small id="endpoint-error" className="field-error">
                {urlError}
              </small>
            )}
            <small>
              Use HTTP or HTTPS. Prefer HTTPS when the endpoint is outside a
              trusted private network. Leave blank to use the endpoint
              configured on the host. The bucket and access keys always come
              from the host.
            </small>
          </label>
        </fieldset>
        <div className="actions">
          <button
            className="primary"
            type="submit"
            disabled={
              locked || offline || invalid || state?.job?.status === "running"
            }
          >
            {locked ? "Verifying…" : "Verify connection"}
          </button>
          <span className="subtle" role="status">
            {running
              ? "Checking each machine. This can take a few minutes."
              : dirty
                ? "Settings changed. Verify to apply them."
                : v.status === "idle"
                  ? "Not verified yet."
                  : v.summary}
          </span>
        </div>
      </form>
      {error && (
        <div className="error run-error" role="alert">
          {error}
        </div>
      )}
      {v.status !== "idle" && !dirty && (
        <>
          <ConnectionGraph verification={v} />
          {[
            ...v.nodes,
            {
              host: v.endpoint.label || "S3 endpoint",
              status: v.endpoint.status,
              message: v.endpoint.message,
              checks: [],
            },
          ]
            .filter((n) => n.status === "failed")
            .map((n) => (
              <details className="verify-failure" key={n.host} open>
                <summary>
                  {n.host} failed{n.message ? `: ${n.message}` : ""}
                </summary>
                {n.checks.length > 0 && (
                  <ul>
                    {n.checks.map((c) => (
                      <li key={c.name} className={`check-${c.status}`}>
                        <strong>{c.name}</strong> {c.status.toUpperCase()} ·{" "}
                        {c.message}
                      </li>
                    ))}
                  </ul>
                )}
              </details>
            ))}
        </>
      )}
    </>
  );
}

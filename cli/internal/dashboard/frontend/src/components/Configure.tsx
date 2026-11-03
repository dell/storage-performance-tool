import { useState } from "react";
import type { Driver, Options, RunMode, State } from "../types";
import { useStartRun } from "../api/mutations";
import { useRuns } from "../api/queries";
import { defaults } from "../config";
import { StorageConfiguration } from "./StorageConfiguration";
import { formatNumber as fmt } from "../utils/metrics";
import { driverReadiness } from "../utils/driver";
import {
  fallbackLimits,
  formatBytes,
  parseSize,
  parseWhole,
  sizeUnits,
  splitSize,
  validateOptions,
  type OptionErrors,
  type SizeUnit,
} from "../utils/validation";

type ExpertDraft = {
  object_count: string;
  size_amount: string;
  size_unit: SizeUnit;
  threads: string;
  max_runtime_seconds: string;
};
const expertPresets: { label: string; options: Options }[] = [
  {
    label: "Quick check",
    options: {
      object_count: 100,
      object_size: "1MiB",
      threads: 2,
      max_runtime_seconds: 60,
    },
  },
  {
    label: "Medium",
    options: {
      object_count: 2000,
      object_size: "4MiB",
      threads: 8,
      max_runtime_seconds: 600,
    },
  },
  {
    label: "Sustained",
    options: {
      object_count: 5000,
      object_size: "16MiB",
      threads: 32,
      max_runtime_seconds: 1800,
    },
  },
];
function toDraft(o: Options): ExpertDraft {
  const { amount, unit } = splitSize(o.object_size);
  return {
    object_count: String(o.object_count),
    size_amount: amount,
    size_unit: unit,
    threads: String(o.threads),
    max_runtime_seconds: String(o.max_runtime_seconds),
  };
}
function fromDraft(d: ExpertDraft): Options {
  return {
    mode: "expert",
    object_count: parseWhole(d.object_count) ?? NaN,
    object_size: `${d.size_amount.trim()}${d.size_unit}`,
    threads: parseWhole(d.threads) ?? NaN,
    max_runtime_seconds: parseWhole(d.max_runtime_seconds) ?? NaN,
  };
}

type ExpertStart = "new" | "saved" | "previous" | "command";
type SavedExpertProfile = { name: string; options: Options };
const expertProfileStorageKey = "spt-dashboard-expert-profiles-v1";

function readExpertProfiles(): SavedExpertProfile[] {
  try {
    const parsed: unknown = JSON.parse(
      localStorage.getItem(expertProfileStorageKey) ?? "[]",
    );
    if (!Array.isArray(parsed)) return [];
    return parsed
      .filter(
        (item): item is SavedExpertProfile =>
          !!item &&
          typeof item === "object" &&
          typeof item.name === "string" &&
          !!item.options &&
          typeof item.options.object_count === "number" &&
          typeof item.options.object_size === "string" &&
          typeof item.options.threads === "number" &&
          typeof item.options.max_runtime_seconds === "number",
      )
      .slice(0, 12);
  } catch {
    return [];
  }
}

// Only parse the four settings currently accepted by POST /api/runs.
// Never execute user input or pass arbitrary CLI flags to the backend.
function parseExpertCommand(
  text: string,
  base: Options,
  limits: NonNullable<State["limits"]>,
): Options {
  const parts = text
    .replace(/\\\s*\r?\n/g, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  if (
    parts.length < 3 ||
    !["spt", "./spt"].includes(parts[0]) ||
    parts[1] !== "run" ||
    parts[2] !== "write-verify"
  ) {
    throw new Error("Start with: spt run write-verify");
  }
  const result: Options = { ...base, mode: "expert" };
  const map = {
    "--object-count": "object_count",
    "--object-size": "object_size",
    "--threads": "threads",
    "--auto-terminate-seconds": "max_runtime_seconds",
  } as const;
  const seen = new Set<string>();
  for (let i = 3; i < parts.length; i++) {
    const [flag, inline] = parts[i].split("=", 2);
    if (!(flag in map))
      throw new Error(
        `Unsupported flag: ${flag}. Only the four dashboard run settings can be imported.`,
      );
    if (seen.has(flag)) throw new Error(`Duplicate flag: ${flag}`);
    seen.add(flag);
    const value = inline ?? parts[++i];
    if (!value || value.startsWith("--"))
      throw new Error(`Missing value for ${flag}.`);
    const key = map[flag as keyof typeof map];
    if (key === "object_size") {
      result.object_size = value;
    } else {
      if (!/^\d+$/.test(value))
        throw new Error(`${flag} must be a whole number.`);
      result[key] = Number(value);
    }
  }
  if (seen.size === 0) throw new Error("Include at least one supported flag.");
  const errors = validateOptions(result, limits);
  const first = Object.values(errors).find(Boolean);
  if (first) throw new Error(first);
  return result;
}

function expertCommand(draft: ExpertDraft, driver: Driver): string {
  const opts = fromDraft(draft);
  const workload = driver === "mock" ? "mock" : "write-verify";
  return `spt run ${workload} --object-count ${opts.object_count} --object-size ${opts.object_size} --threads ${opts.threads} --auto-terminate-seconds ${opts.max_runtime_seconds}`;
}

function PreviousRunPicker({
  setDraft,
}: {
  setDraft: (draft: ExpertDraft) => void;
}) {
  const history = useRuns();
  const previousRuns = (
    Array.isArray(history.data?.runs) ? history.data.runs : []
  ).filter(
    (run) =>
      run.options &&
      run.status === "completed" &&
      typeof run.options.object_count === "number" &&
      typeof run.options.object_size === "string" &&
      typeof run.options.threads === "number" &&
      typeof run.options.max_runtime_seconds === "number",
  );
  const [chosenID, setChosenID] = useState("");
  const [message, setMessage] = useState("");
  const previousID = previousRuns.some((run) => run.run_id === chosenID)
    ? chosenID
    : (previousRuns[0]?.run_id ?? "");
  const historyMessage =
    history.error?.message ||
    message ||
    (history.isPending
      ? "Loading saved runs…"
      : previousRuns.length
        ? ""
        : "No completed runs with reusable settings found.");
  return (
    <div data-spt-legacy="" className="expert-inline">
      <label data-spt-legacy="">
        Completed benchmark
        <select
          data-spt-legacy=""
          value={previousID}
          onChange={(e) => setChosenID(e.target.value)}
        >
          {previousRuns.map((r) => (
            <option data-spt-legacy="" key={r.run_id} value={r.run_id}>
              {r.run_id}
            </option>
          ))}
          {!previousRuns.length && (
            <option data-spt-legacy="" value="">
              No runs available
            </option>
          )}
        </select>
      </label>
      <button
        data-spt-legacy=""
        type="button"
        disabled={!previousID}
        onClick={() => {
          const selected = previousRuns.find((r) => r.run_id === previousID);
          if (selected?.options) {
            setDraft(toDraft(selected.options as Options));
            setMessage(
              "Previous run settings loaded. Review them before running.",
            );
          }
        }}
      >
        Duplicate settings
      </button>
      {historyMessage && (
        <span data-spt-legacy="" role="status" className="expert-muted">
          {historyMessage}
        </span>
      )}
    </div>
  );
}

function ExpertWorkspace({
  draft,
  setDraft,
  errors,
  limits,
  verified,
  clients,
  driver,
}: {
  draft: ExpertDraft;
  setDraft: (d: ExpertDraft) => void;
  errors: OptionErrors;
  limits: NonNullable<State["limits"]>;
  verified: boolean;
  clients: number;
  driver: Driver;
}) {
  const workloadName = driver === "mock" ? "Mock driver" : "Write + verify";
  const [start, setStart] = useState<ExpertStart>("new");
  const [profiles, setProfiles] = useState(readExpertProfiles);
  const [chosenProfile, setChosenProfile] = useState("");
  const [profileName, setProfileName] = useState("");
  const [saveMessage, setSaveMessage] = useState("");
  const [commandText, setCommandText] = useState("");
  const [commandMessage, setCommandMessage] = useState("");
  const [importError, setImportError] = useState(false);
  const options = fromDraft(draft);
  const valid = Object.keys(errors).length === 0;
  const changed = [
    ["object_count", "Object count"],
    ["object_size", "Object size"],
    ["threads", "Threads"],
    ["max_runtime_seconds", "Safety limit"],
  ].filter(
    ([key]) => options[key as keyof Options] !== defaults[key as keyof Options],
  );
  const volume =
    (parseSize(options.object_size) ?? 0) *
    (Number.isFinite(options.object_count) ? options.object_count : 0);

  function saveProfile() {
    const name = profileName.trim().slice(0, 60);
    if (!name || !valid) {
      setSaveMessage(
        name
          ? "Correct invalid settings before saving."
          : "Enter a profile name first.",
      );
      return;
    }
    const next = [
      { name, options: { ...options, mode: "expert" as const } },
      ...profiles.filter((p) => p.name !== name),
    ].slice(0, 12);
    try {
      localStorage.setItem(expertProfileStorageKey, JSON.stringify(next));
      setProfiles(next);
      setChosenProfile(name);
      setProfileName("");
      setSaveMessage(
        "Saved in this browser. Storage credentials are not saved.",
      );
    } catch {
      setSaveMessage(
        "Browser storage is unavailable. Could not save this profile.",
      );
    }
  }

  function importCommand() {
    try {
      setDraft(toDraft(parseExpertCommand(commandText, options, limits)));
      setCommandMessage(
        "Supported settings imported. Review the fields and host limits below.",
      );
      setImportError(false);
    } catch (e) {
      setCommandMessage(
        e instanceof Error ? e.message : "Command could not be parsed.",
      );
      setImportError(true);
    }
  }

  return (
    <div data-spt-legacy="" className="expert-workspace">
      <div data-spt-legacy="" className="expert-workspace-main">
        <div data-spt-legacy="" className="expert-area-heading">
          <strong data-spt-legacy="">Start from</strong>
          <span data-spt-legacy="">
            Reuse a known configuration or start with the defaults.
          </span>
        </div>
        <div
          data-spt-legacy=""
          className="expert-start-grid"
          role="group"
          aria-label="Start expert configuration"
        >
          {(
            [
              ["new", "New configuration", "Use a simple starting point"],
              ["saved", "Saved profile", "Reuse your browser profile"],
              ["previous", "Previous run", "Duplicate completed run settings"],
              ["command", "CLI command", "Import supported SPT flags"],
            ] as const
          ).map(([key, name, detail]) => (
            <button
              data-spt-legacy=""
              type="button"
              key={key}
              className={`expert-start-option ${start === key ? "active" : ""}`}
              aria-pressed={start === key}
              onClick={() => setStart(key)}
            >
              <strong data-spt-legacy="">{name}</strong>
              <small data-spt-legacy="">{detail}</small>
            </button>
          ))}
        </div>
        <div data-spt-legacy="" className="expert-starter">
          {start === "new" && (
            <div data-spt-legacy="" className="expert-inline">
              <label data-spt-legacy="">
                Starting preset
                <select
                  data-spt-legacy=""
                  defaultValue="default"
                  onChange={(e) => {
                    const preset = expertPresets.find(
                      (p) => p.label === e.target.value,
                    );
                    setDraft(toDraft(preset?.options ?? defaults));
                  }}
                >
                  <option data-spt-legacy="" value="default">
                    Default · 100 objects, 64KiB
                  </option>
                  {expertPresets.map((p) => (
                    <option data-spt-legacy="" key={p.label} value={p.label}>
                      {p.label}
                    </option>
                  ))}
                </select>
              </label>
              <span data-spt-legacy="" className="expert-muted">
                Adjust any setting below before reviewing.
              </span>
            </div>
          )}
          {start === "saved" && (
            <div data-spt-legacy="" className="expert-inline">
              <label data-spt-legacy="">
                Saved configurations
                <select
                  data-spt-legacy=""
                  value={chosenProfile}
                  onChange={(e) => setChosenProfile(e.target.value)}
                >
                  <option data-spt-legacy="" value="">
                    Choose a saved profile
                  </option>
                  {profiles.map((p) => (
                    <option data-spt-legacy="" key={p.name} value={p.name}>
                      {p.name}
                    </option>
                  ))}
                </select>
              </label>
              <button
                data-spt-legacy=""
                type="button"
                disabled={!chosenProfile}
                onClick={() => {
                  const p = profiles.find(
                    (item) => item.name === chosenProfile,
                  );
                  if (p) setDraft(toDraft(p.options));
                }}
              >
                Load profile
              </button>
              {!profiles.length && (
                <span data-spt-legacy="" className="expert-muted">
                  No saved profiles yet. Save one in the summary panel.
                </span>
              )}
            </div>
          )}
          {start === "previous" && <PreviousRunPicker setDraft={setDraft} />}
          {start === "command" && (
            <div data-spt-legacy="" className="expert-command-import">
              <label data-spt-legacy="" htmlFor="expert-cli-input">
                Paste an SPT command
              </label>
              <textarea
                data-spt-legacy=""
                id="expert-cli-input"
                value={commandText}
                rows={3}
                spellCheck={false}
                placeholder="spt run write-verify --object-count 1000 --object-size 1MiB --threads 4 --auto-terminate-seconds 600"
                onChange={(e) => setCommandText(e.target.value)}
              />
              <div data-spt-legacy="" className="expert-inline">
                <button
                  data-spt-legacy=""
                  type="button"
                  onClick={importCommand}
                >
                  Import supported settings
                </button>
                <span data-spt-legacy="" className="expert-muted">
                  Import only; commands are never executed. Four flags
                  supported.
                </span>
              </div>
              {commandMessage && (
                <p
                  data-spt-legacy=""
                  className={
                    importError
                      ? "expert-feedback-error"
                      : "expert-feedback-good"
                  }
                  role="status"
                >
                  {commandMessage}
                </p>
              )}
            </div>
          )}
        </div>

        <div
          data-spt-legacy=""
          className="expert-area-heading expert-essentials-heading"
        >
          <strong data-spt-legacy="">
            {workloadName} · Essential settings
          </strong>
          <span data-spt-legacy="">
            The current dashboard API supports this workload and these four run
            settings.
          </span>
        </div>
        <ExpertFields
          draft={draft}
          setDraft={setDraft}
          errors={errors}
          limits={limits}
          showPresets={false}
        />
        <details data-spt-legacy="" className="expert-limits">
          <summary data-spt-legacy="">
            Host limits &amp; advanced settings
          </summary>
          <p data-spt-legacy="">
            Other SPT CLI flags are not available through this version of the
            Dashboard API. Defaults are enforced by the Go server.
          </p>
          <dl data-spt-legacy="">
            <div data-spt-legacy="">
              <dt data-spt-legacy="">Maximum objects</dt>
              <dd data-spt-legacy="">
                {limits.expert.object_count.max.toLocaleString("en-US")}
              </dd>
            </div>
            <div data-spt-legacy="">
              <dt data-spt-legacy="">Maximum threads / client</dt>
              <dd data-spt-legacy="">{limits.expert.threads.max}</dd>
            </div>
            <div data-spt-legacy="">
              <dt data-spt-legacy="">Maximum object size</dt>
              <dd data-spt-legacy="">
                {formatBytes(limits.expert.object_size_bytes.max)}
              </dd>
            </div>
            <div data-spt-legacy="">
              <dt data-spt-legacy="">Maximum runtime</dt>
              <dd data-spt-legacy="">
                {limits.expert.max_runtime_seconds.max}s
              </dd>
            </div>
            <div data-spt-legacy="">
              <dt data-spt-legacy="">Maximum data</dt>
              <dd data-spt-legacy="">
                {formatBytes(limits.expert.max_total_bytes)}
              </dd>
            </div>
          </dl>
        </details>
      </div>
      <aside
        data-spt-legacy=""
        className="expert-summary"
        aria-label="Expert configuration summary"
      >
        <span data-spt-legacy="" className="eyebrow">
          CONFIGURATION SUMMARY
        </span>
        <h3 data-spt-legacy="">{workloadName}</h3>
        <span data-spt-legacy="" className="expert-summary-sub">
          {clients} client machine{clients === 1 ? "" : "s"} ·{" "}
          {driver === "mock"
            ? "No storage used"
            : verified
              ? "Storage verified"
              : "Storage not verified"}
        </span>
        <dl data-spt-legacy="">
          <div data-spt-legacy="">
            <dt data-spt-legacy="">Maximum objects</dt>
            <dd data-spt-legacy="">{draft.object_count || "—"}</dd>
          </div>
          <div data-spt-legacy="">
            <dt data-spt-legacy="">Object size</dt>
            <dd data-spt-legacy="">
              {draft.size_amount
                ? `${draft.size_amount} ${draft.size_unit}`
                : "—"}
            </dd>
          </div>
          <div data-spt-legacy="">
            <dt data-spt-legacy="">Threads / client</dt>
            <dd data-spt-legacy="">{draft.threads || "—"}</dd>
          </div>
          <div data-spt-legacy="">
            <dt data-spt-legacy="">Safety limit</dt>
            <dd data-spt-legacy="">
              {draft.max_runtime_seconds
                ? `${draft.max_runtime_seconds}s`
                : "—"}
            </dd>
          </div>
        </dl>
        <div data-spt-legacy="" className="expert-data-footprint">
          <strong data-spt-legacy="">Maximum test data</strong>
          <span data-spt-legacy="">
            {Number.isFinite(volume) ? formatBytes(volume) : "—"}
          </span>
        </div>
        <div data-spt-legacy="" className="expert-changes">
          <strong data-spt-legacy="">Modified from defaults</strong>
          <div data-spt-legacy="" className="expert-chips">
            {changed.length ? (
              changed.map(([, label]) => (
                <span data-spt-legacy="" key={label}>
                  {label}
                </span>
              ))
            ) : (
              <span data-spt-legacy="" className="quiet">
                Using defaults
              </span>
            )}
          </div>
        </div>
        <details data-spt-legacy="" className="expert-cli-preview">
          <summary data-spt-legacy="">Preview CLI settings</summary>
          <pre data-spt-legacy="">
            {valid
              ? expertCommand(draft, driver)
              : "Correct the settings to generate a command preview."}
          </pre>
          <small data-spt-legacy="">
            {driver === "mock"
              ? "The Go backend adds the client machines, safe flags and the results directory. No storage credentials are passed."
              : "The Go backend adds storage credentials, safe flags and the results directory."}
          </small>
        </details>
        <div data-spt-legacy="" className="expert-save">
          <label data-spt-legacy="" htmlFor="expert-profile-name">
            Save this configuration
          </label>
          <input
            data-spt-legacy=""
            id="expert-profile-name"
            value={profileName}
            placeholder="Profile name"
            maxLength={60}
            onChange={(e) => setProfileName(e.target.value)}
          />
          <button
            data-spt-legacy=""
            type="button"
            disabled={!valid}
            onClick={saveProfile}
          >
            Save profile
          </button>
          {saveMessage && (
            <small data-spt-legacy="" role="status">
              {saveMessage}
            </small>
          )}
        </div>
      </aside>
    </div>
  );
}

export function Configure({
  state,
  mode,
  offline,
  onStarted,
}: {
  state: State | null;
  mode: RunMode;
  offline: boolean;
  onStarted: () => void;
}) {
  const [guided, setGuided] = useState<Options>({ ...defaults }),
    [draft, setDraft] = useState<ExpertDraft>(() => toDraft(defaults)),
    [review, setReview] = useState(false),
    [storageDirty, setStorageDirty] = useState(false),
    [driver, setDriver] = useState<Driver>("s3");
  const startMutation = useStartRun();
  const busy = startMutation.isPending;
  const error = startMutation.error?.message ?? "";
  const limits = state?.limits ?? fallbackLimits;
  const config: Options = {
    ...(mode === "expert" ? fromDraft(draft) : { ...guided, mode: "guided" }),
    ...(driver === "mock" ? { driver } : {}),
  };
  const errors: OptionErrors = validateOptions(config, limits);
  const valid = Object.keys(errors).length === 0;
  const totalBytes =
    (parseSize(config.object_size) ?? 0) *
    (Number.isFinite(config.object_count) ? config.object_count : 0);
  // Each mode keeps its own values; a mode change returns to editing.
  const [previousMode, setPreviousMode] = useState(mode);
  if (mode !== previousMode) {
    setPreviousMode(mode);
    setReview(false);
  }
  const clients = state?.storage?.hosts.length ?? 1;
  const { verified, targetReady } = driverReadiness(
    driver,
    state,
    storageDirty,
  );
  const ready =
    targetReady &&
    !offline &&
    state?.job?.status !== "running" &&
    verified &&
    valid;
  function start() {
    if (busy || !ready || !valid) return;
    startMutation.mutate(config, { onSuccess: onStarted });
  }
  return (
    <div
      data-spt-legacy=""
      className={`configure ${mode === "expert" ? "configure--expert" : ""}`}
    >
      <span data-spt-legacy="" className="eyebrow">
        NEW BENCHMARK
      </span>
      <h1 data-spt-legacy="">Configure a run</h1>
      <p data-spt-legacy="">
        Write test objects, read them back to verify integrity, then clean up
        the run’s objects.
      </p>
      {state?.job?.status === "running" && (
        <div data-spt-legacy="" className="notice">
          A run is already in progress. Its results are available in Live
          dashboard.
        </div>
      )}
      <section data-spt-legacy="" className="panel">
        <span data-spt-legacy="" className="eyebrow">
          STEP 1
        </span>
        <h2 data-spt-legacy="">Storage configuration</h2>
        <StorageConfiguration
          state={state}
          offline={offline}
          onDirty={setStorageDirty}
        />
      </section>
      <section data-spt-legacy="" className="panel">
        <span data-spt-legacy="" className="eyebrow">
          STEP 2
        </span>
        <h2 data-spt-legacy="">
          {review ? "Review your run" : "Workload configuration"}
        </h2>
        <div
          data-spt-legacy=""
          className="driver-switch"
          role="radiogroup"
          aria-label="Storage driver"
        >
          {(
            [
              [
                "s3",
                "S3 write + verify",
                "Writes, reads back, and cleans up objects in the configured bucket.",
              ],
              [
                "mock",
                "Mock driver",
                "Runs SPT's mock storage driver. No S3 endpoint or credentials are used; for smoke tests only.",
              ],
            ] as const
          ).map(([value, label, help]) => (
            <label data-spt-legacy="" key={value} className="check-label">
              <input
                data-spt-legacy=""
                type="radio"
                name="driver"
                value={value}
                checked={driver === value}
                disabled={busy}
                onChange={() => {
                  setDriver(value);
                  setReview(false);
                }}
              />
              <div data-spt-legacy="">
                <strong data-spt-legacy="">{label}</strong>
                <p data-spt-legacy="">{help}</p>
              </div>
            </label>
          ))}
        </div>
        {!verified && (
          <div data-spt-legacy="" className="notice">
            {driver === "mock"
              ? "Verify the client machines to unlock workload settings. The S3 endpoint check is not needed for a mock run."
              : "Verify the storage configuration to unlock workload settings."}
          </div>
        )}
        {driver === "mock" && !!state?.target.mock_reasons?.length && (
          <div data-spt-legacy="" className="notice notice-warn">
            {state.target.mock_reasons.join(" ")}
          </div>
        )}
        <fieldset
          data-spt-legacy=""
          className="plain-fieldset"
          disabled={!verified}
        >
          {review ? (
            <>
              <div data-spt-legacy="" className="review">
                <h2 data-spt-legacy="">
                  {driver === "mock" ? "Mock driver" : "Write + verify"} ·{" "}
                  {mode === "expert" ? "Expert" : "Guided"} · {clients} client
                  machine
                  {clients === 1 ? "" : "s"}
                </h2>
                <dl data-spt-legacy="" className="review-grid">
                  <div data-spt-legacy="">
                    <dt data-spt-legacy="">Maximum objects</dt>
                    <dd data-spt-legacy="">{config.object_count}</dd>
                  </div>
                  <div data-spt-legacy="">
                    <dt data-spt-legacy="">Object size</dt>
                    <dd data-spt-legacy="">{config.object_size}</dd>
                  </div>
                  <div data-spt-legacy="">
                    <dt data-spt-legacy="">Parallel threads</dt>
                    <dd data-spt-legacy="">{config.threads}</dd>
                  </div>
                  <div data-spt-legacy="">
                    <dt data-spt-legacy="">Safety time limit</dt>
                    <dd data-spt-legacy="">{config.max_runtime_seconds}s</dd>
                  </div>
                </dl>
                {driver === "mock" ? (
                  <p data-spt-legacy="">
                    Mock run: no data is written to S3. Results show SPT's mock
                    driver, not storage performance.
                  </p>
                ) : (
                  <p data-spt-legacy="">
                    Up to {fmt(totalBytes / 1048576, 2)} MiB of test data. A
                    unique prefix isolates this run. Cleanup is best effort.
                  </p>
                )}
                {mode === "expert" && (
                  <div data-spt-legacy="" className="expert-reviewed-command">
                    <strong data-spt-legacy="">CLI settings preview</strong>
                    <pre data-spt-legacy="">{expertCommand(draft, driver)}</pre>
                    <small data-spt-legacy="">
                      The server generates the complete command and validates
                      all options.
                    </small>
                  </div>
                )}
              </div>
              <div data-spt-legacy="" className="actions">
                <button
                  data-spt-legacy=""
                  disabled={busy}
                  onClick={() => setReview(false)}
                >
                  Edit settings
                </button>
                <button
                  data-spt-legacy=""
                  className="primary"
                  disabled={busy || !ready}
                  onClick={() => void start()}
                >
                  {busy ? "Starting…" : "Start run"}
                </button>
              </div>
              <p data-spt-legacy="" className="form-help">
                {driver === "mock"
                  ? "Starting runs SPT with the mock driver on the verified client machines."
                  : "Starting runs a real benchmark on the configured storage target."}
              </p>
            </>
          ) : (
            <form
              data-spt-legacy=""
              noValidate
              onSubmit={(e) => {
                e.preventDefault();
                if (valid) setReview(true);
              }}
            >
              <p data-spt-legacy="" className="form-help">
                {mode === "expert"
                  ? "Expert mode: enter any values within the server's limits."
                  : "Guided mode: choose from preset sizes for short, safe runs. Switch to Expert in the top bar for custom values."}
              </p>
              {mode === "expert" ? (
                <ExpertWorkspace
                  draft={draft}
                  setDraft={setDraft}
                  errors={errors}
                  limits={limits}
                  verified={verified}
                  clients={clients}
                  driver={driver}
                />
              ) : (
                <GuidedFields
                  config={guided}
                  setConfig={setGuided}
                  errors={errors}
                  limits={limits}
                />
              )}
              <div
                data-spt-legacy=""
                className={`notice ${totalBytes > limits.expert.max_total_bytes * 0.8 ? "notice-warn" : ""}`}
              >
                Maximum test data:{" "}
                <strong data-spt-legacy="">
                  {fmt(totalBytes / 1048576, 2)} MiB
                </strong>
                {mode === "expert" &&
                  ` of ${formatBytes(limits.expert.max_total_bytes)} allowed`}{" "}
                · Unique prefix · Best effort cleanup
              </div>
              <button
                data-spt-legacy=""
                className="primary"
                type="submit"
                disabled={!valid}
              >
                Review run
              </button>
            </form>
          )}
        </fieldset>
        {error && (
          <div data-spt-legacy="" className="error run-error" role="alert">
            {error}
          </div>
        )}
      </section>
    </div>
  );
}

function FieldHint({
  id,
  error,
  hint,
}: {
  id: string;
  error?: string;
  hint: string;
}) {
  return error ? (
    <small data-spt-legacy="" id={id} className="field-error" role="alert">
      {error}
    </small>
  ) : (
    <small data-spt-legacy="" id={id}>
      {hint}
    </small>
  );
}

function GuidedFields({
  config,
  setConfig,
  errors,
  limits,
}: {
  config: Options;
  setConfig: (o: Options) => void;
  errors: OptionErrors;
  limits: NonNullable<State["limits"]>;
}) {
  const g = limits.guided;
  const select = (
    key: "object_count" | "threads" | "max_runtime_seconds",
    label: string,
    values: number[],
    hint: string,
    suffix = "",
  ) => (
    <label data-spt-legacy="">
      {label}
      <select
        data-spt-legacy=""
        value={config[key]}
        aria-invalid={!!errors[key]}
        aria-describedby={`guided-${key}-hint`}
        onChange={(e) =>
          setConfig({ ...config, [key]: Number(e.target.value) })
        }
      >
        {values.map((v) => (
          <option data-spt-legacy="" key={v} value={v}>
            {v}
            {suffix}
          </option>
        ))}
      </select>
      <FieldHint id={`guided-${key}-hint`} error={errors[key]} hint={hint} />
    </label>
  );
  return (
    <div data-spt-legacy="" className="form-grid">
      {select(
        "object_count",
        "Maximum objects",
        g.object_counts,
        "Stop after this many objects.",
      )}
      <label data-spt-legacy="">
        Object size
        <select
          data-spt-legacy=""
          value={config.object_size}
          aria-invalid={!!errors.object_size}
          aria-describedby="guided-object_size-hint"
          onChange={(e) =>
            setConfig({ ...config, object_size: e.target.value })
          }
        >
          {g.object_sizes.map((v) => (
            <option data-spt-legacy="" key={v}>
              {v}
            </option>
          ))}
        </select>
        <FieldHint
          id="guided-object_size-hint"
          error={errors.object_size}
          hint="Size of each test object."
        />
      </label>
      {select(
        "threads",
        "Parallel threads",
        g.threads,
        "Concurrent requests from each client machine.",
      )}
      {select(
        "max_runtime_seconds",
        "Safety time limit",
        g.max_runtime_seconds,
        "Maximum configured runtime.",
        " seconds",
      )}
    </div>
  );
}

function ExpertFields({
  draft,
  setDraft,
  errors,
  limits,
  showPresets = true,
}: {
  draft: ExpertDraft;
  setDraft: (d: ExpertDraft) => void;
  errors: OptionErrors;
  limits: NonNullable<State["limits"]>;
  showPresets?: boolean;
}) {
  const e = limits.expert;
  const number = (
    key: "object_count" | "threads" | "max_runtime_seconds",
    label: string,
    range: { min: number; max: number },
    hint: string,
  ) => (
    <label data-spt-legacy="">
      {label}
      <input
        data-spt-legacy=""
        type="number"
        inputMode="numeric"
        min={range.min}
        max={range.max}
        step={1}
        value={draft[key]}
        aria-invalid={!!errors[key]}
        aria-describedby={`expert-${key}-hint`}
        onChange={(ev) => setDraft({ ...draft, [key]: ev.target.value })}
      />
      <FieldHint id={`expert-${key}-hint`} error={errors[key]} hint={hint} />
    </label>
  );
  return (
    <>
      {showPresets && (
        <div
          data-spt-legacy=""
          className="preset-chips"
          role="group"
          aria-label="Starting points"
        >
          {expertPresets.map((p) => (
            <button
              data-spt-legacy=""
              type="button"
              key={p.label}
              onClick={() => setDraft(toDraft(p.options))}
            >
              {p.label}
              <small data-spt-legacy="">
                {p.options.object_count.toLocaleString("en-US")} ×{" "}
                {p.options.object_size} · {p.options.threads} threads
              </small>
            </button>
          ))}
        </div>
      )}
      <div data-spt-legacy="" className="form-grid">
        {number(
          "object_count",
          "Maximum objects",
          e.object_count,
          `${e.object_count.min}–${e.object_count.max.toLocaleString("en-US")} objects.`,
        )}
        <label data-spt-legacy="">
          Object size
          <span data-spt-legacy="" className="size-input">
            <input
              data-spt-legacy=""
              type="number"
              inputMode="numeric"
              min={1}
              step={1}
              value={draft.size_amount}
              aria-label="Object size amount"
              aria-invalid={!!errors.object_size}
              aria-describedby="expert-object_size-hint"
              onChange={(ev) =>
                setDraft({ ...draft, size_amount: ev.target.value })
              }
            />
            <select
              data-spt-legacy=""
              value={draft.size_unit}
              aria-label="Object size unit"
              onChange={(ev) =>
                setDraft({ ...draft, size_unit: ev.target.value as SizeUnit })
              }
            >
              {Object.keys(sizeUnits).map((u) => (
                <option data-spt-legacy="" key={u}>
                  {u}
                </option>
              ))}
            </select>
          </span>
          <FieldHint
            id="expert-object_size-hint"
            error={errors.object_size}
            hint={`${formatBytes(e.object_size_bytes.min)}–${formatBytes(e.object_size_bytes.max)} per object.`}
          />
        </label>
        {number(
          "threads",
          "Parallel threads",
          e.threads,
          `${e.threads.min}–${e.threads.max} concurrent requests per client machine.`,
        )}
        {number(
          "max_runtime_seconds",
          "Safety time limit (seconds)",
          e.max_runtime_seconds,
          `${e.max_runtime_seconds.min}–${e.max_runtime_seconds.max} seconds.`,
        )}
      </div>
    </>
  );
}

import { useState } from "react";
import type { Mode, RunConfig } from "./types";
import { useRun } from "./hooks/useRun";
import { GuidedDashboard } from "./components/GuidedDashboard";
import { ExpertSelection, allExpertFields } from "./components/ExpertSelection";
import type { ExpertField } from "./components/ExpertSelection";
import { Dashboard } from "./components/Dashboard";
import { Configure } from "./components/Configure";
import { defaultConfig } from "./data/mock";
import { isMock } from "./services";
export default function App() {
  const [page, setPage] = useState<"dashboard" | "configure">("dashboard"),
    [mode, setMode] = useState<Mode>("guided");
  const [expertFields, setExpertFields] = useState<ExpertField[]>([
      ...allExpertFields,
    ]),
    [expertReady, setExpertReady] = useState(false);
  const { snapshot, error, starting, start } = useRun();
  async function onStart(config: RunConfig) {
    if (await start(config)) setPage("dashboard");
  }
  const synthetic = isMock || snapshot?.source === "mock";
  return (
    <div className="app">
      <aside className="sidebar">
        <a
          className="brand"
          href="#"
          onClick={(e) => {
            e.preventDefault();
            setPage("dashboard");
          }}
        >
          <span>SPT</span>
          <div>
            Storage Performance<small>BENCHMARK CONSOLE</small>
          </div>
        </a>
        <div className="sidebar-label">WORKSPACE</div>
        <nav aria-label="Main navigation">
          <button
            className={page === "dashboard" ? "selected" : ""}
            onClick={() => setPage("dashboard")}
          >
            ◫ <span>Live dashboard</span>
          </button>
          <button
            className={page === "configure" ? "selected" : ""}
            onClick={() => setPage("configure")}
          >
            ＋ <span>Configure run</span>
          </button>
        </nav>
        <div className="sidebar-footer">
          <span className="source-dot" />
          {isMock ? "Local mock source" : "Server API source"}
          <p>
            {synthetic
              ? "Synthetic data · no storage measured"
              : "SPT metrics via API"}
          </p>
        </div>
      </aside>
      <div className="workspace">
        <header className="topbar">
          <span>Benchmark workspace</span>
          <div className="topbar-actions">
            <span className="demo-badge">
              {isMock
                ? "LOCAL MOCK"
                : snapshot?.source === "mock"
                  ? "SERVER MOCK"
                  : snapshot
                    ? "SPT API"
                    : "CONNECTING"}
            </span>
            <div className="mode-switch" role="group" aria-label="Display mode">
              {(["guided", "expert"] as const).map((m) => (
                <button
                  aria-pressed={mode === m}
                  className={mode === m ? "active" : ""}
                  key={m}
                  onClick={() => {
                    setMode(m);
                    if (m === "expert") setExpertReady(false);
                  }}
                >
                  {m === "guided" ? "Guided" : "Expert"}
                </button>
              ))}
            </div>
          </div>
        </header>
        <main>
          {page === "configure" ? (
            <Configure
              initial={snapshot?.config ?? defaultConfig}
              busy={starting}
              onStart={onStart}
            />
          ) : snapshot ? (
            mode === "guided" ? (
              <GuidedDashboard
                key={snapshot.runId}
                snapshot={snapshot}
                error={error}
              />
            ) : !expertReady ? (
              <ExpertSelection
                initial={expertFields}
                onApply={(fields) => {
                  setExpertFields(fields);
                  setExpertReady(true);
                }}
              />
            ) : (
              <>
                <div className="expert-edit">
                  <button onClick={() => setExpertReady(false)}>
                    Change selected data
                  </button>
                </div>
                <Dashboard
                  key={snapshot.runId}
                  snapshot={snapshot}
                  mode="expert"
                  error={error}
                  selection={expertFields}
                />
              </>
            )
          ) : (
            <section className="panel" role="status">
              <h1>{error ? "Metrics unavailable" : "Loading dashboard…"}</h1>
              <p>{error ?? "Waiting for the first snapshot."}</p>
              {error && (
                <p>
                  Check VITE_API_BASE_URL and the backend response contract.
                  This screen retries automatically.
                </p>
              )}
            </section>
          )}
          {page === "configure" && error && (
            <div className="error" role="alert">
              {error}
            </div>
          )}
        </main>
        <footer>
          SPT dashboard prototype ·{" "}
          {synthetic ? "Synthetic fixtures" : "SPT metrics"} · Updates every
          second
        </footer>
      </div>
    </div>
  );
}

# Cross-run comparison UI design (SCRUM-45)

Status: design for the compare view that already ships on
`feature/stupelify/run-catalog-compare` (PR2). This note is the chart of what
exists, what Mike asked for on 10/21, and what to build next. It does not invent
a second compare stack.

## Goal

Let a performance engineer open two or more archived SPT runs and see whether
a newer run is worse than the prior comparable run (or any chosen baseline), without digging
through `~/results` by hand.

## What PR2 already implements

PR2's **Results catalog** and **Compare runs** pages cover the table half of
Mike's stretch and a first overlay:

1. **Pick runs.** Catalog checkboxes select 2–6 runs. One radio marks the
   baseline. Malformed runs cannot be selected.
2. **Totals table.** Per phase, each run's IOPS, MiB/s, P50, and P99 are shown
   against the baseline. Deltas use a plain threshold check (default 10%), not a
   statistical test. Flags: improved / regressed / unchanged / not comparable /
   insufficient data. Settings that make a comparison unfair (workload, object
   size, object count, threads, host count, S3 driver) are listed up front.
3. **Overlay charts.** For each shared phase, IOPS / bandwidth / P50 / P99 are
   drawn as time series on a shared "seconds since phase start" axis so runs that
   started at different wall-clock times still line up. One colour per run; a
   single-sample run is a point, not a line.
4. **Data source.** Read-only scan of `~/results` (dashboard and CLI folders).
   No database.

That is enough for a first demo of "compare with the previous matching run". The remaining work is
interaction polish and clearer previous-run shortcuts, not a rewrite.

## Mike's 10/21 ask (stretch)

Store past runs, add a history tab, and overlay graphs to compare a run with
its previous match. He called comparison "a pretty big project." PR2 answers the storage and
overlay parts; SCRUM-45 is the UI chart for how that project should grow.

## Proposed layout (current + next)

```
Results catalog
├─ filters (search / source / status)
├─ table of runs (compare checkbox, baseline radio, details)
└─ "Compare selected" → Compare runs
       ├─ rule strip (threshold %, latency floor, min ops)
       ├─ settings-difference warnings
       ├─ overlay charts (one panel per phase, metric tabs)
       └─ totals / delta table
```

### Near-term additions (this ticket's intent)

| Item | Why | Notes |
|---|---|---|
| **Compare with previous** | One-click prior match | From a catalog row, pick this run and the next older complete run with the same workload + object size + threads (and object count / host count / S3 driver when both sides record them), else the next older complete run. Matching uses the unfiltered catalog so UI filters cannot hide the prior run. Baseline = older run. This is a shape/time match, not an SPT product-version N vs N−1. |
| **Legend + colour lock** | Readability | Keep the palette order tied to selection order so the baseline colour is stable when the radio changes. |
| **Empty / sparse overlays** | Honesty | Keep the existing "No interval samples for these runs" empty state; do not invent points. |
| **Export** | Share | Optional later: download the delta table as CSV. Not required for SCRUM-45. |

### Out of scope for SCRUM-45

- Statistical significance, confidence intervals, or multi-repetition tests.
- Writing or deleting results.
- A real database; keep reading `~/results`.
- Live attach comparison (in-progress vs archive). That stays on SCRUM-15/47.

## Acceptance for the design

- Reviewers can open this doc and the PR2 Compare page and see they describe the
  same UI.
- "Compare with previous" is implemented as a catalog action that opens Compare
  with exactly two runs (older = baseline).
- Overlay graphs remain the primary visual for previous-run comparison; the totals table stays
  the regression checklist.

## Risks

- Runs with different settings are easy to misread as regressions. The settings
  difference banner must stay visible.
- Interval CSVs are missing on some CLI runs; overlays will be empty while
  totals still compare. The UI already says so.
- Large result trees are truncated by the catalog scan; compare only sees what
  the catalog listed.

## Follow-ups after this design

1. Land PR2 (catalog + compare + overlay).
2. Land the "Compare with previous" affordance on top of PR2.
3. Optional: CSV export of the delta table; pinned colour legend; remember last
   threshold in dashboard state.

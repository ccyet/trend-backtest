# UX Correction Plan

## Scope

This document records the reviewed correction plan for the current Streamlit interaction flow in `app.py`.

It does **not** require a full redesign. The goal is to reduce interaction burden while preserving the existing backtest workflow and result structure.

## Current structure reviewed

The review is grounded in the current app structure:

- `数据准备` and offline update area near the top of the page
- sidebar `运行设置` for scope, date range, data source, and run trigger
- main-area strategy configuration with core and advanced expanders
- separate `交易配置说明` page entry
- result tabs for summary, equity, detail, and parameter scan

## Main friction points

### 1. Configuration and submission are physically separated

Most strategy controls live in the main area, but the run trigger is in the sidebar. After finishing configuration, the user must move back to the sidebar to submit.

### 2. Too many reruns while editing

The current control flow relies on ordinary widgets instead of an explicit submission boundary. This makes the app feel heavy during editing because many changes trigger reruns before the user is ready.

### 3. Core and optional decisions are still visually mixed

Although advanced items are already separated better than before, the main configuration area still asks the user to process entry, exit, MA filter, costs, and scan concepts in one continuous decision chain.

### 4. Help content interrupts the task path

The explanation content is useful, but moving between the workbench and a separate help page adds context switching for users who only need a short reminder while configuring.

### 5. Result interpretation starts too late

The result area is complete, but the app still expects the user to interpret multiple tabs and tables before getting a compact conclusion about whether the run is meaningful.

## Minimal correction plan

### Priority 1 — Shorten the path from configuration to execution

- Move the main `开始回测` action to the bottom of the strategy configuration area.
- Keep the sidebar focused on scope and data-source context only.

Expected outcome: the user completes configuration and submits in one continuous flow.

### Priority 2 — Add a clear submit boundary

- Wrap scope and strategy controls in an explicit `st.form` submission flow.
- Only rerun the full analysis when the user confirms the form.

Expected outcome: fewer disruptive reruns while editing parameters.

### Priority 3 — Tighten information hierarchy

- Keep only essential entry/exit/cost controls visible by default.
- Let MA filter, partial exits, and scan stay collapsed until explicitly needed.

Expected outcome: lower cognitive load for the common path.

### Priority 4 — Demote long-form help to contextual guidance

- Keep the full explanation page, but also add a shorter in-flow reminder near the strategy area.
- Use the full page for reference, not as a primary step in the main task path.

Expected outcome: less navigation interruption during parameter setup.

### Priority 5 — Add a compact run summary before deep tables

- After a run, show a small summary block with factor, scope, source, whether scan was enabled, trade count, and main warning or next step.

Expected outcome: the user can judge the quality of a run before reading multiple tables.

## Preserved behavior

- No change to factor logic, signal generation, or trade execution semantics
- No change to export format or result-tab coverage
- No change to offline-first architecture
- No change to existing advanced capabilities; only visibility and flow should be adjusted

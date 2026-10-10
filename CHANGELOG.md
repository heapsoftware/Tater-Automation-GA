# Changelog

## 1.3.0 — 2026-10-10 (minor — Phase 2 of the proactive-agent design, `specs/proactive_agent_design.md`)

- **Features**
  - New **entity journal**: every runner tick diffs the watched entities'
    states against the previous tick and appends change rows to a ring buffer
    (`automation_ga_core:journal`, newest first, capped by the new
    `max_journal_rows` setting, default 2000). Each row carries
    `{ts, ts_text, entity, from, to, held_seconds}` — `held_seconds` is how
    long the previous state had held, so the journal doubles as a retention
    of the arming tracker's knowledge ("how long has the garage door been
    open?"). The first sighting of an entity seeds the cursor silently, like
    edge-trigger arming; a change never costs an LLM call.
    Scope defaults to `watched` (entities referenced by enabled automations
    and builtins); `journal_scope: "all"` observes the whole HA state cache
    capped alphabetically by `journal_max_entities` (default 150) and keeps
    running even when no automations exist.
  - `automation_capabilities` now also returns a **recent-activity digest**
    (`data.recent`): the last 30 journal rows, currently
    `unavailable`/`unknown` entities, per-camera UniFi Protect person-event
    counts over the last 24h, and per-automation outcome stats
    (`run_count`/`last_answer`/`unanswered_count`/`enabled`). This is the
    shared context the upcoming reflection pass will reuse, and it lets chat
    authoring answer "what has the garage door been doing?" from real data.
  - New settings: `journal_scope`, `journal_max_entities`, `max_journal_rows`
    (declared in the settings UI with defaults; all optional).
- No breaking changes: no definition-schema change, existing automations
  unaffected; the uninstall "delete core data" cleanup covers the two new
  journal keys.
- Tests (offline): smoke 190 → 204 checks (journal diff/cursor/holding-time/
  trim, scope cap, digest shape + capabilities digest); live-checklist mirror
  62 → 69 checks (journal across the real `_tick` loop, scope cap, zero-
  automation journaling); both pass offline.

## 1.2.0 — 2026-10-10 (minor — Phase 1 of the proactive-agent design, `specs/proactive_agent_design.md`)

First slice of the proactive-agent roadmap: the LLM-authored rules are now
backed by a small library of built-in wellbeing automations, the runner
answers a real fall-ambush example end to end, and nuisance questions damp
themselves. No breaking changes: every new definition field is optional and
old automations keep validating and running unchanged.

- **Features**
  - Four builtin starter automations, seeded once per install, **disabled**:
    *Stove left on*, *Door left open*, *Wellbeing — stuck on floor*
    (sustained floor presence → room "are you okay?" call-out → unanswered /
    "no" escalation via notification), and *Welcome home* (Protect person
    event + Face ID → greeting). Builtins can be edited/toggled like any
    automation (their edit card offers **Restore defaults**, which rewrites
    the definition from the canonical seed), but not deleted — and the
    WebUI card has no delete button for them.
  - New definition field `watch_entities` (builtin-only): entities a builtin
    scans dynamically, used by the stove/door builtins to find their target
    entity across an install instead of shipping a hardcoded entity id.
  - New `camera_face` condition — a person-presence tier with identity:
    `mode: any_person` (any Protect person event on the camera within the
    lookback) or `mode: named` (snapshot → Face ID match on `person`).
    Face ID off degrades named → any-person with a validation warning.
    On a named match, `{person}` is templated into the run's
    announce/notify/ask/webhook text.
  - `camera_vision` `hold_seconds` — sustained-verdict arming for conditions:
    the condition passes only after that many seconds of one continuous
    passing verdict (re-snapshots at most every `check_seconds`, default 60;
    a failing or unknown check resets the arming). This turns "person lying
    on the floor and not moving" into a real, runnable condition.
  - New `webhook` leaf action — one outbound HTTP call (POST/PUT/PATCH/GET,
    templated payload/headers, 2xx = success) for integrations that push
    elsewhere. Allowed in ask_yes_no branches too.
  - Nuisance damping (unsolicited-contact guard): every Nth unanswered
    `ask_yes_no` window (default N=3, capped at 24h) progressively widens the
    automation's cooldown, with one webui advisory per threshold crossing.
    A real answer resets the counter; toggling or either card's reset clears
    the widened cooldown. Settings: `damping_unanswered_threshold`,
    `damping_cooldown_max_seconds`.
  - Richer `automation_capabilities` discovery: the schema document now also
    lists resolved announcement targets and the Home Assistant `notify.*`
    services from the live `/api/services` endpoint (companion-app phone
    pushes live there), so the authoring LLM can pick a real push target.
    `include_entities: false` keeps the catalog/discovery sections and skips
    only the entity listing.
- **Tests (offline)**: smoke suite grew from 116 → 190 checks (webhook
  transport seam, hold-arming across ticks, `camera_face` tiers + Face-ID
  fallback, damping, builtin seeding/guards/restore, capabilities discovery);
  the live-checklist mirror grew from 51 → 62 checks (damping exercised
  through the real `_process_pending` timeout path, builtin card actions).
  Both pass offline with fake Redis and stubbed externals; the remaining
  live-only items (real TTS/push/vision on satellites, HA companion app)
  are listed in the §11-B report as before.

## 1.1.2 — 2026-10-07 (revision)

- **Fix** — WebUI automations list: `Announce ''` shown for announcements that
  use the random-list or "LLM writes fresh text" modes. The list card's Action
  row only read the fixed `message` field; it now shows a snippet of the first
  random-list line (`Announce random of N: '…'`) or of the reference text
  (`Announce fresh (LLM): '…'`), and `Announce (no text set)` when none is set.
- **Changes** — "LLM writes fresh text" (`message_style`) now behaves the way
  the form implies: the text you enter in the fresh-text box is sent to the
  base LLM as a *reference announcement*, and each run the LLM writes a fresh
  variation of it — same meaning, facts and tone, different words — instead of
  treating the box as a bare style description. A per-run variation seed and a
  higher sampling temperature are added to the prompt so consecutive runs of
  the same reference produce different wording rather than near-identical text.
  The form field is relabeled "Reference for fresh text" and the schema docs,
  validation messages and error text now describe the reference behavior. No
  definition-schema change: existing `message_style` automations keep working,
  and their box content now reads as the reference it was intended to be.
- Tests: 4 new checks (list-page snippets for random-list and fresh-text
  announces, reference prompt with per-run seed + temperature sent to the base
  LLM) — 116-check smoke + 51-check live-checklist mirror, both passing
  offline.

## 1.1.1 — 2026-10-07 (revision)

- **Fix** — WebUI form editor: creating an automation now returns the panel to the
  Automations list along with the success popup. The Create tab previously stayed
  active, leaving the new automation out of sight until the tab was clicked. A
  successful `ga_create_automation` sets a one-shot marker (`automation_ga_core:ui_state`);
  the tab-data fetch that follows — the renderer's post-action refresh — omits the
  Create tab and sets `default_tab` to `automations`, which is the only case the
  renderer re-anchors the active manager tab. The Create tab is offered again from
  the next load, so further automations can still be created from the form.
  Rejected creates set no marker.
- Tests: 6 new checks (post-create refresh lands on the automations list, repeats
  for the next create, Create tab reappears on the next fetch, rejected create sets
  no marker) — 114-check smoke + 51-check live-checklist mirror, both passing
  offline.

## 1.1.0 — 2026-10-06 (minor)

- **Features** — device + state pickers in the WebUI form editor:
  - Trigger/condition devices are dropdowns of live Home Assistant entities
    (friendly-name labels, noise domains filtered), replacing free-text entity
    ID fields. A "Custom entity ID…" escape keeps raw IDs available.
  - State dropdowns per selected device are built from an entity state catalog
    that merges the HA recorder history API (fetched directly with the Tater
    integration's own credentials, TTL-cached in Redis under
    `automation_ga_core:entity_states`) with attribute enumerations, known
    per-domain vocabulary, and the entity's live state. A "Type a state…"
    escape covers sensors HA cannot enumerate.
  - New edge triggers: `from_state`/`to_state` on `entity_state` fire once per
    state transition ("when the washer changes from spinning to finished")
    instead of re-announcing on cooldown while the state holds. Optional
    `for_seconds` requires the new state to hold before firing.
- **Features** — new `automation_entity_states` kernel tool (eleven total):
  returns an entity's current state, the possible states the catalog learned,
  recent transitions, and the data source, so the LLM can look up real state
  names before authoring `entity_state` specs instead of inventing them.
- **Changes** — form trigger mode is a three-way choice (hold / change / attribute);
  the Edit popup round-trips edge triggers; `ga_refresh_devices` also refreshes
  the state catalog. Form-unsupported guard now also covers edge+attribute
  combinations.
- Tests: 118-check smoke + 48-check live-checklist mirror (was 85/38), both
  passing offline.

## 1.0.1 — 2026-10-06 (revision)

- **Fixes** — runner crash on install in the Tater runtime: `_redis()`
  treated `helpers.redis_client` as a callable factory, but Tater exposes it
  as a lazy `RedisClientProxy` used directly (`TypeError: 'RedisClientProxy'
  object is not callable` on the first line of `run()`). Both shapes are now
  handled: client-like objects are used as-is, callables are invoked.
- **Changes** — uninstall data cleanup verified: all core-owned data
  (`automation_ga_core:automations|meta|activity|pending`,
  `automation_ga_core_settings`) is wiped by the Core Manager "delete all core
  Redis data" cleanup, which runs the real host cleanup code in the offline
  checklist (new section B10). No data-layout changes.

## 1.0.0 — 2026-10-06 (initial release)

Initial release of the Generative Agent automation core.

- Core module `cores/automation_ga_core.py` (display title **Generative Agent**):
  - LLM-authored automation definitions executed by a deterministic runner
    (entity-state triggers with arming/hold, interval/time/protect-event triggers,
    ALL-must-pass conditions: HA attribute, camera vision YES/NO, camera AI,
    person-in-frame).
  - Actions: `announce` (with TTS audio scenes: background bed, ducking, lead-in,
    fade), `notify`, `device`, `camera_ai`, `ask_yes_no` (spoken question + webhook
    answer path).
  - Ten `automation_*` kernel tools (capabilities, validate, create, list, get,
    update, delete, toggle, run, activity), available on every platform.
  - WebUI tab: manager + item forms with an Edit-Automation popup, add form,
    Test-now run, and a read-only guard for form-unsupported definitions.
  - Redis state under `automation_ga_core:*` (automations, meta, activity,
    pending) plus `automation_ga_core_settings`.
- `manifest.json` — Tater Core Shop manifest (top-level `cores` catalog).
- Test suites, the spec/integration guide, and project instructions are
  deliberately not part of this repository; they are developed and verified
  locally (85-check offline smoke suite and a 38-check live-checklist mirror,
  both passing).

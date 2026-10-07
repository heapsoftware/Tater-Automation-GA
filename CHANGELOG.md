# Changelog

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

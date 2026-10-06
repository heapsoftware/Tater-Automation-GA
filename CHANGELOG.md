# Changelog

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

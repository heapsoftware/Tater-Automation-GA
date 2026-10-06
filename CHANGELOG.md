# Changelog

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

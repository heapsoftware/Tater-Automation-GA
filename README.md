# Generative Agent — Automation Core for Tater

An automation core for [Tater](https://github.com/TaterTotterson/Tater)
built on the HGA "Sentinel" principle: **the LLM authors the automation
rules, a deterministic runner executes them**. The runner never
improvises — it only performs what a validated definition says.

Version **1.0.0** · Runs on every Tater platform (webui, discord,
voice_core, portals, Little Spud)

## What it does

Smart-home automations authored in plain language (from any chat
platform) or with the WebUI form editor, then executed by a polling
runner:

- **Triggers** — entity state (with an arming/hold: it only fires once
  the state has held for N seconds), interval, time-of-day, and
  protect events (camera detections).
- **Conditions (all must pass)** — HA attribute matching
  (above / below / equals / contains), camera-vision YES/NO verdict on
  a prompt, camera-AI analysis, and person-in-frame checks.
- **Actions** —
  - `announce` — TTS announcement to satellites with an optional audio
    scene (background music bed, TTS ducking, lead-in delay, fade-out).
    The spoken text is one of: a fixed `message`; a `messages` list
    (one random line picked per run); or a `message_style` (the base
    LLM writes a fresh announcement in that style on every run).
  - `notify` — portal notification with priority and metadata.
  - `device` — run a Home Assistant device action (e.g. switch the
    stove off), targeted or all-compatible-devices.
  - `camera_ai` — snapshot + AI analysis with `{vision}` / `{person}`
    template variables.
  - `ask_yes_no` — asks a spoken question and runs its yes-branch on
    "yes" (spoken or via the answer webhook,
    `/webhook/answer?p=<pending_id>&response=yes|no`); "no" or timeout
    skips it, and the verdict is logged.
- **Kernel tools** — ten `automation_*` tools (capabilities, validate,
  create, list, get, update, delete, toggle, run, activity) available on
  every platform, so the assistant can author and manage automations
  directly in chat.
- **WebUI** — a **Generative Agent** tab: automation manager, item
  cards with a Test-now run button, an Add-Automation form, and an
  Edit-Automation popup for form-supported rules.
- **Activity log** — a recent-events feed (triggered, runs, answers,
  timeouts, created/updated/deleted).

## Home Assistant connection

No extra credentials: the core reuses Tater's existing
`homeassistant` integration (base URL + long-lived token) for entity
states, attribute triggers, and device actions. If the integration is
not configured or HA is unreachable, the affected run reports the
failure cleanly — nothing crashes and nothing is retried silently.

## Install

**From the core shop (recommended):** in Tater's core management area,
add this repo's manifest as a trusted source:

```
https://raw.githubusercontent.com/heapsoftware/Tater-Automation-GA/main/manifest.json
```

Then install **Generative Agent** from the catalog — Tater downloads
`cores/automation_ga_core.py`, verifies its SHA256, and starts the
runner. When a new version is released here, the core shop shows an
update.

**Manual:** copy `cores/automation_ga_core.py` into your Tater
`cores/` directory (or point `TATER_CORE_DIR` at this repo's `cores/`
folder) and restart Tater.

**Prerequisite:** configure Tater's `homeassistant` integration first
— entity triggers and device actions depend on it.

## Configuration

Core settings live under **Generative Agent Automation Settings** in
Tater's core settings:

| Setting | Default | Meaning |
|---|---|---|
| Runner poll interval (seconds) | `10` | How often the runner checks triggers and pending responses. |
| Default response timeout (seconds) | `120` | How long an `ask_yes_no` announcement waits for an answer when the automation does not set one. |
| Default cooldown (seconds) | `1800` | Minimum time between runs of one automation when it does not set its own cooldown. |
| Default announce targets | `all_satellites` | Targets for `announce` / `ask_yes_no` when the automation does not set targets (`all_satellites`, or an explicit target list). |
| Activity rows kept | `300` | How many recent activity entries are retained. |

Automations themselves are configured at creation time — in chat (via
the `automation_*` tools) or in the WebUI form: name, enabled, trigger,
conditions, action, per-automation cooldown, and (for `ask_yes_no`)
the yes-branch.

## Examples (say or type these in chat)

- "Create an automation that announces on all satellites five minutes
  after the stove turns on."
- "When the front-door camera detects a person, ask 'Is that you?' —
  and turn the porch light off if I answer no."
- "Make the morning announcement pick randomly from these three
  lines: …"
- "Write the goodnight announcement in the style of a warm one-sentence
  wish — fresh every night."
- "Show my automations." · "Run the stove automation now." ·
  "Delete the porch-light automation."

## Versioning

`MAJOR.MINOR.REVISION` — see [CHANGELOG.md](CHANGELOG.md). The
`__version__` constant in `cores/automation_ga_core.py` is the single
source of truth and must match `manifest.json`; each release is tagged
`vX.Y.Z` with a matching GitHub Release.

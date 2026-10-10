"""Generative Agent Automation Core — HGA-style plain-English automation authoring with a deterministic runner.

This core is a standalone successor to Tater Shop's built-in ``automation_core``. The LLM
authors automations as JSON definitions (via Hydra kernel tools from any chat platform, or
via the WebUI form editor); this core executes them deterministically on a polling loop.
Dangerous or ambiguous steps are never improvised at runtime: the runner only performs what
the validated definition says.

Coexistence with the stock Automation Core:
    The stock core keeps the module key ``automation_core`` and its ``automation:*`` Redis
    keys; this core uses the module key ``automation_ga_core`` and ``automation_ga_core:*``
    keys. Both cores can run side by side — stock rules stay with the stock core.

Definition schema (see the ``automation_capabilities`` kernel tool):

    {
      "name": "Stove safety check",
      "enabled": true,
      "trigger": {"type": "entity_state", "entity": "switch.stove", "state": "on", "for_seconds": 900},
      "conditions": [
        {"type": "entity_state", "entity": "sensor.living_temp", "attribute": "current_temperature", "match": "above", "value": 27},
        {"type": "camera_vision", "camera": "great room", "prompt": "Is at least one person visible?"}
      ],
      "actions": [
        {"type": "camera_ai", "camera": "front door", "face_id": true,
         "announce": {"message": "At the door: {vision} — {person}", "targets": "all_satellites"}}
      ],
      "mode": "single",
      "cooldown_seconds": 1800
    }

Triggers:            interval | entity_state (state, or attribute + match + value) | time | protect_event
Conditions (ALL):    entity_state | camera_people | camera_vision | presence | time_window
Actions:             call_service | announce (+audio_scene) | ask_yes_no | notify (+priority)
                     | wait | camera_ai | device

Form editor subset (WebUI):
    The form expresses a single-action subset: one trigger, up to three conditions, one
    action (announce / call_service / device / camera_ai / notify / restricted ask_yes_no,
    never ``wait``). Chat-authored definitions outside that subset stay editable only via
    chat, so a form save can never silently drop capability.
"""

from __future__ import annotations

import asyncio
import base64
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
import json
import logging
import math
import os
import random
import re
import struct
import threading
import time
import uuid
import wave
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote

try:
    import requests
except Exception:  # pragma: no cover - compatibility with minimal runtimes
    requests = None  # type: ignore

try:
    import face_identity as _shared_face_identity
except Exception:  # pragma: no cover - compatibility
    _shared_face_identity = None

try:
    from helpers import describe_image_with_local_llm, resolve_hydra_base_servers
except Exception:  # pragma: no cover - compatibility
    describe_image_with_local_llm = None  # type: ignore
    resolve_hydra_base_servers = None  # type: ignore

try:
    from helpers import get_primary_llm_client_from_env as _shared_get_primary_llm_client
except Exception:  # pragma: no cover - compatibility with older Tater runtimes
    _shared_get_primary_llm_client = None  # type: ignore

try:
    from integration_registry import get_integration_device_registry, run_integration_device_action
except Exception:  # pragma: no cover - compatibility
    get_integration_device_registry = None  # type: ignore
    run_integration_device_action = None  # type: ignore

try:
    from notify import notifier_destination_catalog
except Exception:  # pragma: no cover - compatibility
    notifier_destination_catalog = None  # type: ignore

try:
    from announcement_targets import build_announcement_target_options
except Exception:  # pragma: no cover - compatibility
    build_announcement_target_options = None  # type: ignore

try:
    from speech_settings import get_speech_settings
except Exception:  # pragma: no cover - compatibility
    get_speech_settings = None  # type: ignore

try:
    from speech_tts import speak_announcement_targets
except Exception:  # pragma: no cover - compatibility
    speak_announcement_targets = None  # type: ignore

try:
    from vision_settings import get_vision_settings
except Exception:  # pragma: no cover - compatibility
    get_vision_settings = None  # type: ignore

try:
    from kernel_tools import video_analyze as _shared_video_analyze
except Exception:  # pragma: no cover - compatibility with Tater versions before video understanding
    _shared_video_analyze = None

try:
    from kernel_tools import describe_image_bytes as _shared_describe_image_bytes
except Exception:  # pragma: no cover - compatibility with older Tater runtimes
    _shared_describe_image_bytes = None

try:
    from tater_paths import agent_lab_path as _tater_agent_lab_path
except Exception:  # pragma: no cover - compatibility with older Tater runtimes
    _tater_agent_lab_path = None


__version__ = "1.3.0"
CORE_DESCRIPTION = (
    "Generative Agent automations: the LLM authors automations as validated JSON definitions "
    "(chat or form editor) and a deterministic polling runner executes them — entity state and "
    "attribute triggers, camera vision conditions and camera AI actions with optional Face ID, "
    "device actions across integrations, TTS announcements with background-audio scenes, "
    "prioritized notifications, and spoken yes/no response windows."
)
TAGS = ["automation", "generative-agent", "integrations", "smart-home", "tts", "vision", "video", "rules"]

logger = logging.getLogger("automation_ga_core")

MODULE_KEY = "automation_ga_core"
SETTINGS_KEY = f"{MODULE_KEY}_settings"
AUTOMATIONS_KEY = f"{MODULE_KEY}:automations"
META_KEY = f"{MODULE_KEY}:meta"
ACTIVITY_KEY = f"{MODULE_KEY}:activity"
PENDING_KEY = f"{MODULE_KEY}:pending"

INTEGRATION_STATES_KEY = "tater:integration_runtime:states"
INTEGRATION_EVENTS_KEY = "tater:integration_runtime:events"
ENTITY_STATES_KEY = f"{MODULE_KEY}:entity_states"
UI_STATE_KEY = f"{MODULE_KEY}:ui_state"
JOURNAL_KEY = f"{MODULE_KEY}:journal"
JOURNAL_META_KEY = f"{MODULE_KEY}:journal_meta"

LEAF_ACTION_TYPES = ("call_service", "announce", "notify", "wait", "webhook")
ALL_ACTION_TYPES = ("ask_yes_no", "camera_ai", "device", *LEAF_ACTION_TYPES)
TRIGGER_TYPES = ("interval", "entity_state", "time", "protect_event")
CONDITION_TYPES = ("entity_state", "camera_people", "camera_vision", "camera_face", "presence", "time_window")
ENTITY_MATCH_OPS = ("equals", "not_equals", "contains", "above", "below")
NOTIFY_PRIORITIES = ("low", "normal", "high", "urgent")

_CAMERA_MEDIA_MODES = {"image", "video"}
_CAMERA_MEDIA_MODE_OPTIONS = [
    {
        "value": "image",
        "label": "Image description",
        "description": "Describe one snapshot. Faster and supported by every compatible camera.",
        "icon": "▧",
    },
    {
        "value": "video",
        "label": "Video description",
        "description": "Analyze a short clip to understand actions, changes, and sequence.",
        "icon": "▶",
    },
]

# entity state catalog: possible states per HA entity, sourced from the HA
# recorder history API and cached in Redis (see _refresh_state_catalog).
_HA_HISTORY_WINDOW_HOURS = 72
_HA_HISTORY_TIMEOUT_SECONDS = 20.0
_HA_HISTORY_MAX_ENTITIES_PER_CALL = 50
_HA_HISTORY_MAX_ROWS = 20_000
_STATE_CATALOG_TTL_SECONDS = 60.0
_STATE_CATALOG_MAX_STATES = 25
_STATE_CATALOG_MAX_TRANSITIONS = 12
_STATE_FORM_MAX_OPTIONS = 8
_ENTITY_STATE_SKIP_DOMAINS = frozenset(
    {
        "update",
        "button",
        "event",
        "conversation",
        "assist_satellite",
        "stt",
        "tts",
        "wake_word",
        "image",
        "video",
        "camera",
        "number",
        "select",
        "text",
        "scene",
        "script",
        "button_group",
    }
)
_DOMAIN_STATE_VOCAB: Dict[str, Tuple[str, ...]] = {
    "switch": ("on", "off"),
    "light": ("on", "off"),
    "fan": ("on", "off"),
    "input_boolean": ("on", "off"),
    "binary_sensor": ("on", "off"),
    "person": ("home", "not_home"),
    "device_tracker": ("home", "not_home"),
    "lock": ("locked", "unlocked"),
    "cover": ("open", "closed", "opening", "closing", "stopped"),
    "climate": ("off", "heat", "cool", "heat_cool", "auto"),
    "media_player": ("playing", "paused", "idle", "standby", "off", "on"),
    "sun": ("above_horizon", "below_horizon"),
    "vacuum": ("cleaning", "paused", "docked", "returning", "error"),
    "humidifier": ("on", "off"),
    "water_heater": ("off", "eco", "heat_pump", "gas", "electric"),
    "alarm_control_panel": ("disarmed", "armed_home", "armed_away", "armed_night"),
}
# attributes that enumerate the values an entity can take
_ENTITY_OPTION_ATTRS = (
    "options",
    "hvac_modes",
    "fan_modes",
    "swing_modes",
    "preset_modes",
    "source_list",
    "effect_list",
    "sound_mode_list",
    "available_presets",
    "preset_list",
)
_FORM_CUSTOM_SENTINEL = "__custom__"

_CATEGORY_ICONS = {
    "light": "☀",
    "switch": "⏻",
    "plug": "⌁",
    "fan": "✣",
    "garage_door": "▥",
    "cover": "▤",
    "entry_sensor": "↔",
    "lock": "◆",
    "motion": "⌁",
    "camera": "◉",
    "doorbell": "◉",
    "leak": "◒",
    "climate": "◐",
    "temperature": "°",
    "humidity": "◔",
    "illuminance": "☼",
    "energy": "ϟ",
    "battery": "▰",
    "media_player": "♪",
    "presence": "◎",
    "network_device": "⌘",
    "remote": "⌁",
    "scene": "✦",
    "script": "▶",
    "sensor": "◇",
    "device": "◆",
}

_ACTION_LABELS = {
    "turn_on": "Turn on",
    "turn_off": "Turn off",
    "toggle": "Toggle",
    "set_brightness": "Set brightness",
    "set_color": "Set color",
    "open": "Open",
    "close": "Close",
    "stop": "Stop",
    "set_position": "Set position",
    "lock": "Lock",
    "unlock": "Unlock",
    "set_temperature": "Set temperature",
    "set_hvac_mode": "Set HVAC mode",
    "play": "Play",
    "pause": "Pause",
    "playpause": "Play / pause",
    "next": "Next",
    "previous": "Previous",
    "set_volume": "Set volume",
    "volume_up": "Volume up",
    "volume_down": "Volume down",
    "mute": "Mute",
    "unmute": "Unmute",
    "play_media": "Play media",
    "play_url": "Play URL",
    "announce": "Announce",
    "activate": "Activate",
    "run": "Run",
}

_TRUE = {"1", "true", "yes", "on", "enabled", "y"}
_FALSE = {"0", "false", "no", "off", "disabled", "n"}

_BACKGROUND_AUDIO_MAX_UPLOAD_BYTES = 16 * 1024 * 1024
_BACKGROUND_AUDIO_PRESET_SECONDS = 12
_BACKGROUND_AUDIO_PRESET_SAMPLE_RATE = 24000
_CAMERA_FACE_ID_TIMEOUT_SECONDS = 8.0
_FORM_MAX_CONDITIONS = 3

_BACKGROUND_AUDIO_PRESETS: Tuple[Dict[str, str], ...] = (
    {
        "id": "morning_glow",
        "label": "Morning Glow",
        "description": "Warm, optimistic synth chords for weather and wake-up announcements.",
    },
    {
        "id": "calm_focus",
        "label": "Calm Focus",
        "description": "A soft, steady ambient bed for reminders and status updates.",
    },
    {
        "id": "gentle_rain",
        "label": "Gentle Rain",
        "description": "A light, seamless rain-like texture with subtle tonal movement.",
    },
    {
        "id": "bright_pulse",
        "label": "Bright Pulse",
        "description": "A quiet rhythmic pulse for upbeat announcements.",
    },
)
_background_audio_preset_lock = threading.Lock()
_RUNNER_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="automation-ga")
RUN_LOCK = threading.Lock()

AFFIRMATIVE_WORDS = (
    "yes", "yeah", "yep", "yup", "sure", "ok", "okay", "do it", "go ahead",
    "turn it off", "turn off", "confirm", "please do", "affirmative", "sounds good",
    "shut it off", "shut it down", "go for it",
)
NEGATIVE_WORDS = (
    "no", "nope", "don't", "dont", "do not", "stop", "cancel", "leave it",
    "never mind", "nevermind", "negative", "leave it on", "don't turn it off",
)
_VISION_YES_WORDS = ("yes", "yep", "yup", "affirmative")
_VISION_NO_WORDS = ("no", "nope", "negative")

CORE_SETTINGS = {
    "label": "Generative Agent Automation Settings",
    "category": "Generative Agent",
    "required": {
        "poll_seconds": {
            "label": "Runner poll interval (seconds)",
            "type": "number",
            "default": 10,
            "description": "How often the runner checks triggers and pending responses.",
        },
        "default_response_timeout_seconds": {
            "label": "Default response timeout (seconds)",
            "type": "number",
            "default": 120,
            "description": "How long an ask_yes_no announcement waits for an answer when the automation does not set one.",
        },
        "default_cooldown_seconds": {
            "label": "Default cooldown (seconds)",
            "type": "number",
            "default": 1800,
            "description": "Minimum time between runs of one automation when it does not set cooldown_seconds.",
        },
        "default_announce_targets": {
            "label": "Default announce targets",
            "type": "text",
            "default": "all_satellites",
            "description": "Targets used by announce/ask_yes_no when the automation does not set targets (all_satellites, or explicit target list).",
        },
        "max_activity_rows": {
            "label": "Activity rows kept",
            "type": "number",
            "default": 300,
            "description": "How many recent activity entries to retain.",
        },
        "damping_unanswered_threshold": {
            "label": "Damping threshold (unanswered prompts)",
            "type": "number",
            "default": 3,
            "description": "Every Nth unanswered ask_yes_no window widens the automation's cooldown (nuisance damping); answering resets the count.",
        },
        "damping_cooldown_max_seconds": {
            "label": "Damping cooldown cap (seconds)",
            "type": "number",
            "default": 86400,
            "description": "Upper bound the damping can widen an automation's cooldown to; toggling or answering resets it.",
        },
        "journal_scope": {
            "label": "Entity journal scope",
            "type": "text",
            "default": "watched",
            "description": "Which entities the activity journal tracks: 'watched' (entities referenced by automations and builtins) or 'all' (every cached HA entity, capped by journal_max_entities).",
        },
        "journal_max_entities": {
            "label": "Entity journal max entities",
            "type": "number",
            "default": 150,
            "description": "Cap on journaled entities when journal_scope is 'all' (first N alphabetically).",
        },
        "max_journal_rows": {
            "label": "Journal rows kept",
            "type": "number",
            "default": 2000,
            "description": "Ring-buffer size of the entity-state-change journal.",
        },
    },
}

CORE_WEBUI_TAB = {
    "label": "Generative Agent",
    "order": 55,
    "requires_running": True,
}


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------

def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "ignore").strip()
    return str(value or "").strip()


def _bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    token = _text(value).lower()
    if token in _TRUE:
        return True
    if token in _FALSE:
        return False
    return bool(default)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _int(value: Any, default: int = 0, *, minimum: int = 0, maximum: int = 1_000_000) -> int:
    try:
        parsed = int(float(_text(value)))
    except Exception:
        parsed = int(default)
    return max(minimum, min(maximum, parsed))


def _float(value: Any) -> Optional[float]:
    try:
        return float(_text(value))
    except Exception:
        return None


def _token(value: Any) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", _text(value).lower().replace("-", "_")).strip("_")


def _list(value: Any) -> List[str]:
    raw = value
    if isinstance(raw, str):
        token = raw.strip()
        if token.startswith("[") and token.endswith("]"):
            try:
                raw = json.loads(token)
            except Exception:
                raw = token
    if isinstance(raw, (tuple, set)):
        raw = list(raw)
    if not isinstance(raw, list):
        raw = [] if raw in (None, "") else [raw]
    out: List[str] = []
    seen: set[str] = set()
    for item in raw:
        if isinstance(item, dict):
            item = item.get("value") or item.get("id") or item.get("key") or item.get("target")
        text = _text(item)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _json_object(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    token = _text(value)
    if not token:
        return {}
    try:
        parsed = json.loads(token)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _json_record(value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(_text(value))
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _now_label(ts: Any) -> str:
    value = _float(ts)
    if not value:
        return "never"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))


def _seconds_to_milliseconds(value: Any, default_ms: int = 0) -> int:
    try:
        parsed = int(round(float(value) * 1000.0))
    except Exception:
        parsed = int(default_ms)
    return max(0, min(30000, parsed))


def _redis():
    from helpers import redis_client as _rc

    # Real Tater exposes ``redis_client`` as a lazy RedisClientProxy that is
    # used directly (``redis_client.get(...)``); factory-style helpers instead
    # return the client when called. Support both shapes.
    client_like = False
    try:
        client_like = hasattr(_rc, "get") and hasattr(_rc, "hgetall")
    except Exception:
        client_like = False
    if client_like or not callable(_rc):
        return _rc
    return _rc()


def _json_loads(raw: Any, default: Any = None) -> Any:
    try:
        return json.loads(raw)
    except Exception:
        return default


def _load_settings(client: Any = None) -> Dict[str, Any]:
    rc = client if client is not None else _redis()
    try:
        raw = rc.get(SETTINGS_KEY)
        if raw:
            data = _json_loads(raw, {})
            if isinstance(data, dict):
                return data
        data = rc.hgetall(SETTINGS_KEY) or {}
        return {k: v for k, v in data.items()} if isinstance(data, dict) else {}
    except Exception:
        return {}


def _setting(client: Any, key: str, default: Any = None) -> Any:
    settings = _load_settings(client)
    raw = settings.get(key)
    if raw in (None, ""):
        return default
    return raw


def _poll_seconds(client: Any) -> float:
    return max(3.0, _as_float(_setting(client, "poll_seconds", 10), 10))


def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# activity log
# ---------------------------------------------------------------------------

def _log_activity(client: Any, automation_id: str, name: str, event: str, detail: str = "") -> None:
    rc = client if client is not None else _redis()
    row = {
        "ts": time.time(),
        "ts_text": _now_iso(),
        "automation_id": automation_id,
        "automation": name,
        "event": event,
        "detail": _text(detail),
    }
    try:
        rc.lpush(ACTIVITY_KEY, json.dumps(row, separators=(",", ":"), default=str))
        max_rows = max(50, _as_int(_setting(rc, "max_activity_rows", 300), 300))
        rc.ltrim(ACTIVITY_KEY, 0, max_rows - 1)
    except Exception:
        logger.exception("activity log failed")


def _read_activity(client: Any, automation_id: str = "", limit: int = 30) -> List[Dict[str, Any]]:
    rc = client if client is not None else _redis()
    try:
        raw_rows = rc.lrange(ACTIVITY_KEY, 0, max(0, _as_int(limit, 30) - 1)) or []
    except Exception:
        return []
    rows: List[Dict[str, Any]] = []
    for raw in raw_rows:
        row = _json_loads(raw, {})
        if not isinstance(row, dict):
            continue
        if automation_id and _text(row.get("automation_id")) != automation_id:
            continue
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# automation storage
# ---------------------------------------------------------------------------

def _load_automations(client: Any = None) -> Dict[str, Dict[str, Any]]:
    rc = client if client is not None else _redis()
    try:
        raw = rc.hgetall(AUTOMATIONS_KEY) or {}
    except Exception:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for auto_id, raw_def in raw.items():
        definition = _json_loads(raw_def, {})
        if isinstance(definition, dict) and definition:
            out[str(auto_id)] = definition
    return out


def _load_meta(client: Any = None) -> Dict[str, Dict[str, Any]]:
    rc = client if client is not None else _redis()
    try:
        raw = rc.hgetall(META_KEY) or {}
    except Exception:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for auto_id, raw_meta in raw.items():
        meta = _json_loads(raw_meta, {})
        if isinstance(meta, dict):
            out[str(auto_id)] = meta
    return out


def _save_meta(client: Any, auto_id: str, meta: Dict[str, Any]) -> None:
    rc = client if client is not None else _redis()
    rc.hset(META_KEY, auto_id, json.dumps(meta, separators=(",", ":"), default=str))


def _meta_for(metas: Dict[str, Dict[str, Any]], auto_id: str) -> Dict[str, Any]:
    meta = metas.get(auto_id)
    return dict(meta) if isinstance(meta, dict) else {}


def _automation_enabled(definition: Dict[str, Any]) -> bool:
    return _bool(definition.get("enabled"), True)


# ---------------------------------------------------------------------------
# background audio engine (TTS audio scenes)
# ---------------------------------------------------------------------------

def _background_audio_root() -> Path:
    # Tater exposes this shared audio-scene asset folder through its existing
    # /api/ai-tasks/background-audio route, even when AI Task Core is disabled.
    if callable(_tater_agent_lab_path):
        return Path(_tater_agent_lab_path("ai_task", "background_audio")).resolve()
    configured = _text(os.getenv("TATER_AGENT_ROOT"))
    base = Path(configured).expanduser() if configured else Path.cwd() / "agent_lab"
    return (base / "ai_task" / "background_audio").resolve()


def _background_audio_base_url() -> str:
    port = _int(os.getenv("HTMLUI_PORT"), 8501, minimum=1, maximum=65535)
    return f"http://127.0.0.1:{port}/api/ai-tasks/background-audio"


def _background_audio_file_url(kind: str, filename: str) -> str:
    clean_kind = _text(kind).lower()
    clean_filename = Path(_text(filename)).name
    if clean_kind not in {"presets", "uploads"} or not clean_filename:
        return ""
    return f"{_background_audio_base_url()}/{clean_kind}/{quote(clean_filename)}"


def _background_audio_periodic_frequency(frequency: float) -> float:
    duration = float(_BACKGROUND_AUDIO_PRESET_SECONDS)
    return max(1.0 / duration, round(float(frequency) * duration) / duration)


def _background_audio_preset_sample(preset_id: str, sample_time: float) -> float:
    tau = math.tau

    def tone(frequency: float, phase: float = 0.0) -> float:
        return math.sin(tau * _background_audio_periodic_frequency(frequency) * sample_time + phase)

    if preset_id == "morning_glow":
        breath = 0.72 + (0.18 * math.sin(tau * sample_time / 6.0))
        pad = (tone(261.63) + tone(329.63, 0.7) + tone(392.0, 1.4)) / 3.0
        shimmer_gate = (0.5 + (0.5 * math.sin(tau * sample_time / 3.0))) ** 6
        return (0.25 * breath * pad) + (0.055 * tone(659.25, 0.3) * shimmer_gate)
    if preset_id == "calm_focus":
        breath = 0.68 + (0.2 * math.sin(tau * sample_time / 12.0))
        low = (tone(130.81) + tone(196.0, 0.8)) * 0.5
        air = (tone(261.63, 1.1) + tone(293.66, 2.0)) * 0.5
        return (0.22 * breath * low) + (0.07 * air)
    if preset_id == "gentle_rain":
        texture = 0.0
        for index, frequency in enumerate((487.0, 613.0, 743.0, 887.0, 1061.0, 1229.0, 1451.0, 1693.0)):
            texture += tone(frequency, 0.73 * index) * (0.7 / math.sqrt(index + 2.0))
        drift = tone(174.61, 0.5) * (0.5 + (0.5 * math.sin(tau * sample_time / 6.0)))
        return (0.04 * texture) + (0.045 * drift)
    pulse = (0.5 + (0.5 * math.sin(tau * sample_time / 1.5))) ** 5
    chord = (tone(220.0) + tone(277.18, 0.6) + tone(329.63, 1.2)) / 3.0
    upper = tone(554.37, 0.9) * (0.5 + (0.5 * math.sin(tau * sample_time / 3.0)))
    return (0.2 * chord * (0.35 + (0.65 * pulse))) + (0.045 * upper)


def _write_background_audio_preset(path: Path, preset_id: str) -> None:
    sample_rate = int(_BACKGROUND_AUDIO_PRESET_SAMPLE_RATE)
    frame_count = sample_rate * int(_BACKGROUND_AUDIO_PRESET_SECONDS)
    pcm = bytearray(frame_count * 2)
    for index in range(frame_count):
        sample = _background_audio_preset_sample(preset_id, index / sample_rate)
        struct.pack_into("<h", pcm, index * 2, int(max(-0.92, min(0.92, sample)) * 32767.0))
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with wave.open(str(temp_path), "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(sample_rate)
            wav_file.writeframes(bytes(pcm))
        os.replace(temp_path, path)
    finally:
        try:
            temp_path.unlink(missing_ok=True)
        except Exception:
            pass


def _ensure_background_audio_presets() -> Dict[str, Path]:
    root = _background_audio_root() / "presets"
    out: Dict[str, Path] = {}
    with _background_audio_preset_lock:
        for preset in _BACKGROUND_AUDIO_PRESETS:
            preset_id = _text(preset.get("id"))
            if not preset_id:
                continue
            path = root / f"{preset_id}.wav"
            if not path.is_file() or path.stat().st_size <= 44:
                _write_background_audio_preset(path, preset_id)
            out[preset_id] = path
    return out


def _background_audio_preset_url(preset_id: Any) -> str:
    clean_id = _text(preset_id).lower()
    if clean_id not in {_text(row.get("id")) for row in _BACKGROUND_AUDIO_PRESETS}:
        raise ValueError("Choose a valid background audio preset.")
    path = _ensure_background_audio_presets().get(clean_id)
    if not path or not path.is_file():
        raise ValueError("The selected background audio preset could not be created.")
    return _background_audio_file_url("presets", path.name)


def _background_audio_detect_extension(filename: Any, content_type: Any, data: bytes) -> str:
    suffix = Path(_text(filename)).suffix.lower()
    content = _text(content_type).split(";", 1)[0].strip().lower()
    if data.startswith(b"RIFF") and len(data) >= 12 and data[8:12] == b"WAVE":
        detected = ".wav"
    elif data.startswith(b"fLaC"):
        detected = ".flac"
    elif data.startswith(b"ID3") or any(
        data[index] == 0xFF and (data[index + 1] & 0xE0) == 0xE0
        for index in range(max(0, min(len(data) - 1, 4096)))
    ):
        detected = ".mp3"
    else:
        raise ValueError("Uploaded background audio must be a valid WAV, MP3, or FLAC file.")
    content_extensions = {
        "audio/wav": ".wav",
        "audio/x-wav": ".wav",
        "audio/mpeg": ".mp3",
        "audio/mp3": ".mp3",
        "audio/flac": ".flac",
        "audio/x-flac": ".flac",
    }
    claimed = suffix or content_extensions.get(content, "")
    if claimed and claimed not in {".wav", ".mp3", ".flac"}:
        raise ValueError("Uploaded background audio must use a .wav, .mp3, or .flac filename.")
    if claimed and claimed != detected:
        raise ValueError("Uploaded background audio content does not match its filename.")
    if detected == ".wav":
        try:
            with wave.open(io.BytesIO(data), "rb") as wav_file:
                channels = int(wav_file.getnchannels())
                sample_width = int(wav_file.getsampwidth())
                sample_rate = int(wav_file.getframerate())
                frame_count = int(wav_file.getnframes())
        except Exception as exc:
            raise ValueError("Uploaded WAV background audio is invalid or unsupported.") from exc
        if channels not in {1, 2} or sample_width != 2 or not 8000 <= sample_rate <= 96000 or frame_count <= 0:
            raise ValueError("Uploaded WAV background audio must be 16-bit mono or stereo at 8–96 kHz.")
    return detected


def _store_background_audio_upload(raw: Any) -> str:
    upload = raw if isinstance(raw, dict) else {}
    encoded = _text(upload.get("data_b64"))
    if not encoded:
        raise ValueError("Choose a WAV, MP3, or FLAC file to upload.")
    try:
        data = base64.b64decode(encoded, validate=True)
    except Exception as exc:
        raise ValueError("The uploaded background audio could not be decoded.") from exc
    if not data:
        raise ValueError("The uploaded background audio is empty.")
    if len(data) > _BACKGROUND_AUDIO_MAX_UPLOAD_BYTES:
        raise ValueError("Uploaded background audio must be 16 MB or smaller.")
    extension = _background_audio_detect_extension(upload.get("filename"), upload.get("content_type"), data)
    source_stem = Path(_text(upload.get("filename")) or "background-audio").stem
    safe_stem = re.sub(r"[^a-zA-Z0-9_-]+", "-", source_stem).strip("-_").lower() or "background-audio"
    digest = hashlib.sha256(data).hexdigest()[:12]
    filename = f"{safe_stem[:48]}-{digest}{extension}"
    root = _background_audio_root() / "uploads"
    root.mkdir(parents=True, exist_ok=True)
    path = root / filename
    if not path.is_file() or path.stat().st_size != len(data):
        temp_path = root / f".{filename}.{uuid.uuid4().hex}.tmp"
        try:
            temp_path.write_bytes(data)
            os.replace(temp_path, path)
        finally:
            try:
                temp_path.unlink(missing_ok=True)
            except Exception:
                pass
    return _background_audio_file_url("uploads", filename)


def _background_audio_source_from_url(url: Any) -> str:
    text = _text(url)
    for preset in _BACKGROUND_AUDIO_PRESETS:
        preset_id = _text(preset.get("id"))
        if text.endswith(f"/presets/{preset_id}.wav"):
            return f"preset:{preset_id}"
    if "/api/ai-tasks/background-audio/uploads/" in text:
        return "upload"
    return "custom"


def _normalize_tts_audio_scene(raw: Any) -> Dict[str, Any]:
    scene = raw if isinstance(raw, dict) else {}
    background = scene.get("background") if isinstance(scene.get("background"), dict) else {}
    foreground = scene.get("foreground") if isinstance(scene.get("foreground"), dict) else {}
    ducking = scene.get("ducking") if isinstance(scene.get("ducking"), dict) else {}
    finish = scene.get("finish") if isinstance(scene.get("finish"), dict) else {}
    background_url = _text(background.get("url") or scene.get("background_url"))
    if not background_url:
        return {}
    return {
        "background": {
            "url": background_url,
            "loop": _bool(background.get("loop"), True),
            "volume_percent": _int(background.get("volume_percent"), 60, maximum=100),
        },
        "foreground": {
            "start_delay_ms": _int(
                foreground.get("start_delay_ms", scene.get("tts_start_delay_ms")),
                0,
                maximum=30000,
            ),
        },
        "ducking": {
            "target_percent": _int(ducking.get("target_percent"), 35, maximum=100),
            "attack_ms": _int(ducking.get("attack_ms"), 150, maximum=10000),
            "release_ms": _int(ducking.get("release_ms"), 350, maximum=10000),
        },
        "finish": {
            "fade_ms": _int(finish.get("fade_ms"), 500, maximum=10000),
        },
    }


# ---------------------------------------------------------------------------
# integration device registry helpers
# ---------------------------------------------------------------------------

def _encode_device(provider: Any, device_id: Any) -> str:
    left = _text(provider)
    right = _text(device_id)
    return f"{left}|{right}" if left and right else ""


def _decode_device(value: Any) -> Tuple[str, str]:
    token = _text(value)
    if "|" not in token:
        return "", token
    provider, device_id = token.split("|", 1)
    return _text(provider), _text(device_id)


def _device_id(device: Dict[str, Any]) -> str:
    return _text(device.get("id") or device.get("ref"))


def _device_ref(device: Dict[str, Any]) -> str:
    return _text(device.get("ref") or device.get("id"))


def _device_categories(device: Dict[str, Any]) -> set[str]:
    values = [
        *(device.get("category_ids") or []),
        *(device.get("capabilities") or []),
        device.get("type"),
    ]
    return {_token(item) for item in values if _token(item)}


def _device_actions(device: Dict[str, Any]) -> List[str]:
    return [_token(item) for item in (device.get("actions") or []) if _token(item)]


def _device_room(device: Dict[str, Any]) -> str:
    return _token(device.get("room") or device.get("area") or "unassigned")


def _registry(client: Any = None, *, refresh: bool = False, overlay_runtime_state: bool = True) -> Dict[str, Any]:
    empty = {"devices": [], "categories": [], "rooms": [], "category_definitions": []}
    if get_integration_device_registry is None:
        return empty
    rc = client if client is not None else _redis()
    try:
        try:
            result = get_integration_device_registry(
                rc,
                refresh=refresh,
                overlay_runtime_state=overlay_runtime_state,
            )
        except TypeError as exc:
            if overlay_runtime_state or "overlay_runtime_state" not in str(exc):
                raise
            # Compatibility with Tater releases before lightweight inventory
            # reads were added. The core still works, but pays the older load.
            result = get_integration_device_registry(rc, refresh=refresh)
    except Exception:
        logger.debug("[automation_ga] device registry unavailable", exc_info=True)
        return empty
    return result if isinstance(result, dict) else empty


def _device_option(device: Dict[str, Any]) -> Dict[str, Any]:
    provider = _text(device.get("integration_id"))
    device_id = _device_id(device)
    name = _text(device.get("name")) or device_id
    room = _text(device.get("room") or device.get("area"))
    integration = _text(device.get("integration_name")) or provider
    categories = sorted(_device_categories(device))
    primary_category = categories[0] if categories else "device"
    details = " • ".join(item for item in (room, integration) if item)
    return {
        "value": _encode_device(provider, device_id),
        "label": name,
        "description": details,
        "meta": _text(device.get("state") or device.get("status")),
        "icon": _CATEGORY_ICONS.get(primary_category, "◆"),
    }


def _token_variants(value: Any) -> set[str]:
    text = _text(value).lower()
    if not text:
        return set()
    variants = {text}
    for delimiter in (".", ":", "/"):
        if delimiter in text:
            variants.add(text.rsplit(delimiter, 1)[-1])
    variants.add(re.sub(r"[^a-z0-9]+", "", text))
    return {item for item in variants if item}


def _device_tokens(device: Dict[str, Any]) -> set[str]:
    values: List[Any] = [
        device.get("id"),
        device.get("ref"),
        device.get("name"),
    ]
    details = device.get("details") if isinstance(device.get("details"), dict) else {}
    values.extend(
        details.get(key)
        for key in ("id", "device_id", "entity_id", "resource_id", "camera_id", "sensor_id", "serial", "mac")
    )
    for source in device.get("event_sources") or []:
        if isinstance(source, dict):
            values.extend([source.get("id"), source.get("ref"), source.get("resource_ref")])
    out: set[str] = set()
    for value in values:
        out.update(_token_variants(value))
    return out


def _find_device(registry: Dict[str, Any], encoded: Any) -> Optional[Dict[str, Any]]:
    provider, device_id = _decode_device(encoded)
    variants = _token_variants(device_id)
    if not device_id:
        return None
    for device in registry.get("devices") or []:
        if not isinstance(device, dict):
            continue
        if provider and _text(device.get("integration_id")).lower() != provider.lower():
            continue
        if variants.intersection(_device_tokens(device)):
            return device
    return None


def _camera_supports_media_mode(device: Dict[str, Any], mode: Any) -> bool:
    media_mode = _token(mode)
    if media_mode not in _CAMERA_MEDIA_MODES or not _device_categories(device).intersection(
        {"camera", "doorbell"}
    ):
        return False
    actions = set(_device_actions(device))
    capabilities = {
        _token(value)
        for value in [*(device.get("capabilities") or []), *(device.get("features") or [])]
        if _token(value)
    }
    if media_mode == "video":
        return bool(actions.intersection({"camera_clip", "video_clip", "clip"})) or bool(
            capabilities.intersection({"camera_clip", "video_clip", "clip"})
        )
    return bool(actions.intersection({"camera_snapshot", "snapshot"})) or "snapshot" in capabilities


def _camera_device_options(registry: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for device in registry.get("devices") or []:
        if not isinstance(device, dict) or not any(
            _camera_supports_media_mode(device, mode) for mode in _CAMERA_MEDIA_MODES
        ):
            continue
        option = _device_option(device)
        if not option["value"] or option["value"] in seen:
            continue
        seen.add(option["value"])
        rows.append(option)
    rows.sort(key=lambda row: (_text(row.get("label")).casefold(), _text(row.get("value"))))
    return rows


def _camera_media_mode_dependency(
    registry: Dict[str, Any],
    *,
    source_key: str,
    current_device: Any = "",
    current_mode: Any = "image",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    options_by_source: Dict[str, List[Dict[str, Any]]] = {}
    all_values: set[str] = set()
    for device in registry.get("devices") or []:
        if not isinstance(device, dict):
            continue
        encoded = _encode_device(device.get("integration_id"), _device_id(device))
        if not encoded:
            continue
        rows = [
            dict(option)
            for option in _CAMERA_MEDIA_MODE_OPTIONS
            if _camera_supports_media_mode(device, option["value"])
        ]
        if rows:
            options_by_source[encoded] = rows
            all_values.update(_text(row.get("value")) for row in rows)
    default_options = [
        dict(option)
        for option in _CAMERA_MEDIA_MODE_OPTIONS
        if _text(option.get("value")) in all_values
    ]
    selected = [dict(row) for row in options_by_source.get(_text(current_device), default_options)]
    saved_mode = _token(current_mode)
    if saved_mode in _CAMERA_MEDIA_MODES and not any(
        _text(row.get("value")) == saved_mode for row in selected
    ):
        saved = next(
            (dict(row) for row in _CAMERA_MEDIA_MODE_OPTIONS if row["value"] == saved_mode),
            {"value": saved_mode, "label": saved_mode.title(), "icon": "◆"},
        )
        saved["meta"] = "Saved setting; currently unavailable"
        selected.append(saved)
    return selected, {
        "source_key": source_key,
        "options_by_source": options_by_source,
        "default_options": default_options,
    }


def _homeassistant_config() -> Dict[str, str]:
    try:
        from tateros import integration_store as integration_store_module

        module = integration_store_module.integration_module("homeassistant")
        if module is not None:
            result = module.load_homeassistant_config(required=False)
            if isinstance(result, dict):
                return {"base": _text(result.get("base")), "token": _text(result.get("token"))}
    except Exception:
        pass
    return {"base": "", "token": ""}


# ---------------------------------------------------------------------------
# announcement / notification destination options
# ---------------------------------------------------------------------------

def _announcement_options(current_values: Any = None) -> List[Dict[str, str]]:
    if build_announcement_target_options is None:
        return []
    ha = _homeassistant_config()
    try:
        return [
            {
                **dict(row),
                "description": _text(row.get("description")) or "Speaker, media player, or Tater satellite",
                "icon": _text(row.get("icon")) or "♪",
            }
            for row in build_announcement_target_options(
                homeassistant_base_url=ha["base"],
                homeassistant_token=ha["token"],
                include_homeassistant=bool(ha["base"] and ha["token"]),
                include_sonos=True,
                include_unifi_protect=True,
                include_voice_core=True,
                include_integrations=True,
                current_values=current_values,
            )
            if isinstance(row, dict)
        ]
    except Exception:
        logger.debug("[automation_ga] announcement target discovery failed", exc_info=True)
        return []


def _encode_notification_target(platform: Any, targets: Any = None) -> str:
    platform_name = _text(platform).lower()
    if not platform_name:
        return ""
    payload = targets if isinstance(targets, dict) else {}
    return json.dumps({"platform": platform_name, "targets": payload}, sort_keys=True, separators=(",", ":"))


def _decode_notification_target(value: Any) -> Optional[Dict[str, Any]]:
    payload = _json_object(value)
    platform = _text(payload.get("platform")).lower()
    if not platform:
        return None
    return {
        "platform": platform,
        "targets": dict(payload.get("targets") or {}) if isinstance(payload.get("targets"), dict) else {},
    }


def _notification_label(platform: str, targets: Dict[str, Any]) -> str:
    for key in (
        "label",
        "channel",
        "channel_id",
        "room_alias",
        "room_id",
        "chat_id",
        "device_name",
        "device_id",
        "service",
        "device_service",
        "scope",
    ):
        value = _text(targets.get(key))
        if value:
            return value
    return "Defaults"


def _notification_options(client: Any, current_values: Any = None) -> List[Dict[str, str]]:
    if notifier_destination_catalog is None:
        return []
    try:
        catalog = notifier_destination_catalog(redis_client=client, limit=250)
    except Exception:
        catalog = {"platforms": []}
    rows: List[Dict[str, str]] = []
    seen: set[str] = set()
    for platform_row in catalog.get("platforms") or []:
        if not isinstance(platform_row, dict):
            continue
        platform = _text(platform_row.get("platform")).lower()
        platform_label = _text(platform_row.get("label")) or platform.replace("_", " ").title()
        if not platform:
            continue
        if not _bool(platform_row.get("requires_target"), False):
            value = _encode_notification_target(platform, {})
            rows.append({"value": value, "label": f"{platform_label}: defaults"})
            seen.add(value)
        for destination in platform_row.get("destinations") or []:
            if not isinstance(destination, dict):
                continue
            targets = destination.get("targets") if isinstance(destination.get("targets"), dict) else {}
            value = _encode_notification_target(platform, targets)
            if not value or value in seen:
                continue
            seen.add(value)
            label = _text(destination.get("label")) or _notification_label(platform, targets)
            rows.append({"value": value, "label": f"{platform_label}: {label}"})
    for saved in _list(current_values):
        if saved and saved not in seen:
            rows.append({"value": saved, "label": f"{saved} (saved)"})
    return rows


# ---------------------------------------------------------------------------
# Home Assistant helpers (state cache + attribute matching + service calls)
# ---------------------------------------------------------------------------

def _ha_entities(client: Any = None) -> Dict[str, Dict[str, Any]]:
    """entity_id -> {state, attributes, friendly_name, updated_at} from the integration runtime cache."""
    rc = client if client is not None else _redis()
    try:
        raw = rc.hgetall(INTEGRATION_STATES_KEY) or {}
    except Exception:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for field, raw_record in raw.items():
        if ":" not in str(field):
            continue
        provider, _, token = str(field).partition(":")
        if provider != "homeassistant":
            continue
        record = _json_loads(raw_record, {})
        payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
        attributes = payload.get("attributes") if isinstance(payload.get("attributes"), dict) else {}
        friendly = _text(attributes.get("friendly_name"))
        out[token] = {
            "entity_id": token,
            "state": _text(payload.get("state")),
            "attributes": attributes,
            "friendly_name": friendly,
            "updated_at": _as_float(record.get("updated_at"), 0.0),
        }
    return out


def _ha_entity_state(client: Any, entity_id: str) -> Tuple[Optional[str], float]:
    row = _ha_entities(client).get(_text(entity_id))
    if not row:
        return None, 0.0
    return row.get("state"), float(row.get("updated_at") or 0.0)


def _walk_values(value: Any, *, depth: int = 0) -> Iterable[Tuple[str, Any]]:
    if depth > 5:
        return
    if isinstance(value, dict):
        for key, child in value.items():
            yield _text(key), child
            yield from _walk_values(child, depth=depth + 1)
    elif isinstance(value, list):
        for child in value[:100]:
            yield "", child
            yield from _walk_values(child, depth=depth + 1)


def _path_value(payload: Dict[str, Any], path: str) -> Any:
    token = _text(path)
    if not token:
        return None
    current: Any = payload
    for part in token.split("."):
        if not isinstance(current, dict):
            return None
        if part in current:
            current = current[part]
            continue
        lowered = part.lower()
        matched = next((key for key in current if _text(key).lower() == lowered), None)
        if matched is None:
            return None
        current = current[matched]
    return current


def _entity_attribute_value(row: Dict[str, Any], attribute: Any) -> Any:
    """Resolve a dotted attribute path against an HA entity row's attributes dict."""
    path = _text(attribute)
    lowered = path.lower()
    for prefix in ("state.attributes.", "attributes."):
        if lowered.startswith(prefix):
            path = path[len(prefix):]
            break
    attributes = row.get("attributes") if isinstance(row.get("attributes"), dict) else {}
    return _path_value(attributes, path)


def _entity_state_match(client: Any, spec: Dict[str, Any]) -> Tuple[bool, str]:
    """Evaluate an entity_state trigger/condition spec.

    Supports either an exact ``state`` or ``attribute`` + ``match`` + ``value``
    (match: equals | not_equals | contains | above | below). Returns (satisfied, detail).
    """
    entity_id = _text(spec.get("entity"))
    row = _ha_entities(client).get(entity_id)
    if row is None:
        return False, f"entity '{entity_id}' has no cached state"
    attribute = _text(spec.get("attribute"))
    if attribute:
        match_op = _text(spec.get("match") or "equals").lower()
        raw = _entity_attribute_value(row, attribute)
        if raw is None:
            return False, f"attribute '{attribute}' not found on {entity_id}"
        detail = f"{entity_id} {attribute}={raw!r}"
        if match_op in ("equals", "not_equals", "contains"):
            current = _text(raw)
            wanted = _text(spec.get("value"))
            if match_op == "equals":
                ok = current.casefold() == wanted.casefold()
            elif match_op == "not_equals":
                ok = current.casefold() != wanted.casefold()
            else:
                ok = wanted.casefold() in current.casefold()
            return ok, detail + ("" if ok else f" (match {match_op} '{wanted}' failed)")
        current_num = _float(raw)
        target_num = _float(spec.get("value"))
        if current_num is None or target_num is None:
            return False, f"{detail} — cannot compare non-numeric values with '{match_op}'"
        ok = current_num > target_num if match_op == "above" else current_num < target_num
        return ok, detail + ("" if ok else f" (match {match_op} {target_num:g} failed)")
    wanted = _text(spec.get("state"))
    current = _text(row.get("state"))
    ok = current == wanted
    return ok, f"{entity_id} is '{current}'" + ("" if ok else f" not '{wanted}'")


def _ha_notify_services(rc: Any) -> List[str]:
    """HA notify.* service names (companion-app `notify.mobile_app_*` entries among
    them) for the authoring capabilities document. Read-only GET /api/services
    through the same REST channel `_ha_call_service` already uses; [] on any failure
    so a broken/unconfigured HA never breaks authoring."""
    if requests is None:
        return []
    base, token = _ha_rest_config(rc)
    if not base or not token:
        return []
    try:
        resp = requests.get(
            f"{base.rstrip('/')}/api/services",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        if resp.status_code >= 400:
            return []
        rows = resp.json() or []
    except Exception:
        return []
    out: List[str] = []
    for row in rows:
        if not isinstance(row, dict) or _text(row.get("domain")) != "notify":
            continue
        for service in row.get("services") or []:
            if isinstance(service, dict) and _text(service.get("service")):
                out.append(f"notify.{_text(service['service'])}")
    return sorted(dict.fromkeys(out))


def _ha_call_service(client: Any, domain: str, service: str, data: Dict[str, Any]) -> Tuple[bool, str]:
    rc = client if client is not None else _redis()
    domain = _text(domain)
    service = _text(service)
    if not domain or not service:
        return False, "call_service requires domain and service"
    try:
        from tateros import integration_store as integration_store_module

        fn = integration_store_module.integration_function("homeassistant", "call_service_sync")
        if callable(fn):
            payload = fn(domain, service, dict(data or {}))
            if isinstance(payload, dict) and payload.get("ok") is False:
                return False, _text(payload.get("error") or "HA service call failed")
            return True, f"Called {domain}.{service}"
    except Exception as exc:
        logger.warning("call_service_sync via integration failed: %s", exc)

    try:
        if requests is None:
            return False, "the requests library is unavailable in this runtime"
        from integration_runtime import load_homeassistant_config

        conf = load_homeassistant_config(required=False, client=rc)
        base = _text(conf.get("base"))
        token = _text(conf.get("token"))
        if not base or not token:
            return False, "Home Assistant integration is not configured"
        url = f"{base.rstrip('/')}/api/services/{domain}/{service}"
        resp = requests.post(
            url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=dict(data or {}),
            timeout=15,
        )
        if resp.status_code >= 400:
            return False, f"HA returned HTTP {resp.status_code}"
        return True, f"Called {domain}.{service}"
    except Exception as exc:
        return False, f"HA service call failed: {exc}"


# ---------------------------------------------------------------------------
# entity state catalog: possible states per entity from HA recorder history
# ---------------------------------------------------------------------------

def _ha_rest_config(rc: Any) -> Tuple[str, str]:
    """base URL + bearer token for direct HA REST calls, reusing the integration's own credentials."""
    try:
        from integration_runtime import load_homeassistant_config

        conf = load_homeassistant_config(required=False, client=rc)
        if isinstance(conf, dict):
            base = _text(conf.get("base"))
            token = _text(conf.get("token"))
            if base and token:
                return base, token
    except Exception:
        pass
    conf = _homeassistant_config()
    return conf.get("base", ""), conf.get("token", "")


def _ha_history_timestamp(value: Any) -> float:
    raw = _text(value)
    if not raw:
        return 0.0
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.timestamp()
    except Exception:
        return 0.0


def _ha_entity_history(
    client: Any,
    entity_ids: Sequence[Any],
    window_hours: float = _HA_HISTORY_WINDOW_HOURS,
) -> Dict[str, List[Tuple[str, float]]]:
    """entity_id -> chronological [(state, last_changed)] from the HA recorder history API.

    Returns {} when HA is unreachable, unconfigured, or the recorder has no data —
    callers must degrade to fallback state sources.
    """
    rc = client if client is not None else _redis()
    wanted: List[str] = []
    for entity_id in entity_ids:
        token = _text(entity_id)
        if token and token not in wanted:
            wanted.append(token)
    if not wanted or requests is None:
        return {}
    base, token = _ha_rest_config(rc)
    if not base or not token:
        return {}
    start = datetime.fromtimestamp(time.time() - max(1.0, window_hours) * 3600.0).astimezone().isoformat()
    url = f"{base.rstrip('/')}/api/history/period/{quote(start)}"
    params: List[Tuple[str, str]] = [
        ("filter_entity_id", ",".join(wanted[:_HA_HISTORY_MAX_ENTITIES_PER_CALL])),
        ("minimal_response", "true"),
        ("no_attributes", "true"),
    ]
    try:
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=_HA_HISTORY_TIMEOUT_SECONDS,
        )
        if response.status_code >= 400:
            logger.warning("HA history request failed with HTTP %s", response.status_code)
            return {}
        payload = response.json()
    except Exception as exc:
        logger.warning("HA history request failed: %s", exc)
        return {}
    if not isinstance(payload, list):
        return {}
    rows = 0
    out: Dict[str, List[Tuple[str, float]]] = {}
    for index, block in enumerate(payload):
        entity_id = wanted[index] if index < len(wanted) else ""
        if not isinstance(block, list):
            continue
        timeline: List[Tuple[str, float]] = []
        for entry in block:
            if rows >= _HA_HISTORY_MAX_ROWS:
                break
            state = ""
            ts = 0.0
            if isinstance(entry, dict):
                state = _text(entry.get("state"))
                ts = _ha_history_timestamp(entry.get("last_changed") or entry.get("last_updated"))
                entity_id = _text(entry.get("entity_id")) or entity_id
            elif isinstance(entry, list) and entry:
                # compact minimal_response rows: [state, last_changed, ...]
                state = _text(entry[0] if not isinstance(entry[0], list) else "")
                ts = _ha_history_timestamp(entry[1]) if len(entry) > 1 else 0.0
            if state:
                timeline.append((state, ts))
                rows += 1
        if entity_id and timeline:
            existing = out.setdefault(entity_id, [])
            existing.extend(timeline)
    return out


def _entity_state_fallback(row: Optional[Dict[str, Any]], entity_id: str) -> List[str]:
    """Candidate states for an entity without history: enumerating attributes, domain vocabulary, live state."""
    domain = entity_id.split(".", 1)[0]
    out: List[str] = []
    if row is not None:
        for attr in _ENTITY_OPTION_ATTRS:
            raw = _entity_attribute_value(row, attr)
            if isinstance(raw, list):
                for item in raw:
                    token = _text(item)
                    if token and token not in out:
                        out.append(token)
            elif _text(raw) and _text(raw) not in out:
                out.append(_text(raw))
    for state in _DOMAIN_STATE_VOCAB.get(domain, ()):
        if state not in out:
            out.append(state)
    if row is not None:
        live = _text(row.get("state"))
        if live and live not in out:
            out.append(live)
    return out


def _state_catalog_read(rc: Any, entity_id: str, *, max_age: float = _STATE_CATALOG_TTL_SECONDS) -> Optional[Dict[str, Any]]:
    try:
        raw = rc.hget(ENTITY_STATES_KEY, _text(entity_id))
    except Exception:
        return None
    record = _json_loads(raw, None)
    if not isinstance(record, dict):
        return None
    if max_age > 0 and _as_float(record.get("fetched_at"), 0.0) < time.time() - max_age:
        return None
    return record


def _state_catalog_write(rc: Any, entity_id: str, record: Dict[str, Any]) -> None:
    try:
        rc.hset(ENTITY_STATES_KEY, _text(entity_id), json.dumps(record, separators=(",", ":"), default=str))
    except Exception as exc:
        logger.warning("could not persist entity state catalog for %s: %s", entity_id, exc)


def _refresh_state_catalog(client: Any, entity_ids: Sequence[Any], *, force: bool = False) -> int:
    """Refresh the cached possible-states catalog for the given entities. Returns entities refreshed."""
    rc = client if client is not None else _redis()
    wanted: List[str] = []
    for entity_id in entity_ids:
        token = _text(entity_id)
        if token and token not in wanted:
            wanted.append(token)
    if not wanted:
        return 0
    stale = [] if force else [e for e in wanted if _state_catalog_read(rc, e) is None]
    if not stale:
        stale = wanted
    history: Dict[str, List[Tuple[str, float]]] = {}
    for offset in range(0, len(stale), _HA_HISTORY_MAX_ENTITIES_PER_CALL):
        chunk = stale[offset:offset + _HA_HISTORY_MAX_ENTITIES_PER_CALL]
        history.update(_ha_entity_history(rc, chunk))
    rows = _ha_entities(rc)
    now = time.time()
    refreshed = 0
    for entity_id in stale:
        timeline = history.get(entity_id, [])
        catalog_states: Dict[str, float] = {}
        transitions: List[List[Any]] = []
        previous = ""
        for state, ts in timeline:
            if state:
                catalog_states[state] = max(catalog_states.get(state, 0.0), ts or now)
            if state and previous and state != previous:
                transitions.append([previous, state, ts or now])
            previous = state
        row = rows.get(entity_id)
        live = _text(row.get("state")) if row else ""
        if live:
            catalog_states[live] = max(catalog_states.get(live, 0.0), now)
        ordered = sorted(catalog_states.items(), key=lambda pair: pair[1], reverse=True)
        if len(ordered) > _STATE_CATALOG_MAX_STATES:
            keep = {state for state, _ts in ordered[:_STATE_CATALOG_MAX_STATES]}
            if live:
                keep.add(live)
            ordered = [(state, ts) for state, ts in ordered if state in keep]
        source = "history" if timeline else ("live" if live else "none")
        record = {
            "fetched_at": now,
            "source": source,
            "states": [[state, ts] for state, ts in ordered[:_STATE_CATALOG_MAX_STATES]],
            "transitions": transitions[-_STATE_CATALOG_MAX_TRANSITIONS:],
        }
        _state_catalog_write(rc, entity_id, record)
        refreshed += 1
    return refreshed


_STATE_CATALOG_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="automation-ga-states")
_STATE_CATALOG_REFRESH_LOCK = threading.Lock()
_STATE_CATALOG_PENDING: set[str] = set()


def _refresh_state_catalog_async(client: Any, entity_ids: Iterable[Any]) -> None:
    """Queue a background catalog refresh; never blocks the caller (runner tick / tab render)."""
    todo = [e for e in entity_ids if _text(e)]
    if not todo:
        return
    with _STATE_CATALOG_REFRESH_LOCK:
        fresh = [e for e in todo if e not in _STATE_CATALOG_PENDING]
        _STATE_CATALOG_PENDING.update(fresh)
    if not fresh:
        return

    def _run() -> None:
        try:
            _refresh_state_catalog(client, list(fresh))
        except Exception as exc:
            logger.warning("entity state catalog refresh failed: %s", exc)
        finally:
            with _STATE_CATALOG_REFRESH_LOCK:
                for entity_id in fresh:
                    _STATE_CATALOG_PENDING.discard(entity_id)

    try:
        _STATE_CATALOG_EXECUTOR.submit(_run)
    except Exception:
        with _STATE_CATALOG_REFRESH_LOCK:
            for entity_id in fresh:
                _STATE_CATALOG_PENDING.discard(entity_id)


def _state_catalog_states(rc: Any, entity_id: str, rows: Optional[Dict[str, Dict[str, Any]]] = None) -> Tuple[List[str], str]:
    """Merged candidate states for the UI: catalog (history) first, then fallbacks. (states, source)"""
    record = _state_catalog_read(rc, entity_id, max_age=max(_STATE_CATALOG_TTL_SECONDS, 86400.0))
    out: List[str] = []
    source = "none"
    if record is not None:
        source = _text(record.get("source")) or "none"
        for entry in record.get("states") if isinstance(record.get("states"), list) else []:
            if isinstance(entry, (list, tuple)) and entry:
                token = _text(entry[0])
            else:
                token = _text(entry)
            if token and token not in out:
                out.append(token)
    if rows is None:
        rows = _ha_entities(rc)
    row = rows.get(entity_id)
    for state in _entity_state_fallback(row, entity_id):
        if state not in out:
            out.append(state)
    return out[:_STATE_CATALOG_MAX_STATES * 2], source


def _state_catalog_transitions(rc: Any, entity_id: str, limit: int = 6) -> List[Tuple[str, str, float]]:
    record = _state_catalog_read(rc, entity_id, max_age=max(_STATE_CATALOG_TTL_SECONDS, 86400.0))
    out: List[Tuple[str, str, float]] = []
    for entry in record.get("transitions") if isinstance(record, dict) and isinstance(record.get("transitions"), list) else []:
        if isinstance(entry, (list, tuple)) and len(entry) >= 2:
            out.append((_text(entry[0]), _text(entry[1]), _as_float(entry[2] if len(entry) > 2 else 0, 0.0)))
    return out[-limit:]


def _automation_entities(automations: Dict[str, Dict[str, Any]]) -> List[str]:
    """HA entity ids referenced by automation triggers/conditions (plus builtin
    watch_entities scans)."""
    out: List[str] = []
    for definition in automations.values():
        if not isinstance(definition, dict) or not _automation_enabled(definition):
            continue
        for section in ("trigger", "conditions"):
            block = definition.get(section)
            items = [block] if section == "trigger" else (block if isinstance(block, list) else [])
            for spec in items:
                if isinstance(spec, dict) and _text(spec.get("type")) == "entity_state":
                    entity_id = _text(spec.get("entity"))
                    if entity_id and entity_id not in out:
                        out.append(entity_id)
        watch = definition.get("watch_entities")
        if isinstance(watch, list):
            for watch_id in watch:
                watched = _text(watch_id)
                if watched and watched not in out:
                    out.append(watched)
    return out


# ---------------------------------------------------------------------------
# built-in anomaly automations (seeded once, always disabled)
# ---------------------------------------------------------------------------

BUILTINS_MARKER = "automation_ga_core:builtins_seeded"


def _builtin_definitions_for_seed(rc: Any) -> List[Tuple[str, str, Dict[str, Any]]]:
    """Canonical (builtin_id, name, definition) rows, with best-effort entity/camera
    discovery from the live caches. Placeholder references that do not exist simply
    never fire, so a failed discovery is inert-safe, never an error."""
    entities = _ha_entities(rc)

    def _find_entity(needle: str, domains: Sequence[str]) -> str:
        for entity_id in sorted(entities):
            domain = entity_id.split(".", 1)[0]
            if domains and domain not in domains:
                continue
            if needle in entity_id.lower() or needle in _text(entities[entity_id].get("friendly_name")).lower():
                return entity_id
        return ""

    camera_rows = _camera_device_options(_registry(rc))
    camera_ref = _text(camera_rows[0].get("value")) if camera_rows else ""
    stove_entity = _find_entity("stove", ("switch", "binary_sensor", "sensor")) or "switch.stove"
    door_entity = (_find_entity("door", ("binary_sensor", "sensor")) or _find_entity("window", ("binary_sensor", "sensor"))) or "binary_sensor.door"
    camera_display = _text(camera_rows[0].get("label")) if camera_rows else "great room"

    return [
        (
            "stove_left_on",
            "Stove-left-on safety check",
            {
                "name": "Stove-left-on safety check",
                "builtin": True,
                "builtin_id": "stove_left_on",
                "enabled": False,
                "trigger": {"type": "entity_state", "entity": stove_entity, "state": "on", "for_seconds": 900},
                "conditions": [{"type": "camera_people", "camera": camera_ref, "lookback_seconds": 180, "expect_people": False}],
                "actions": [
                    {
                        "type": "ask_yes_no",
                        "targets": "all_satellites",
                        "message": "The stove has been on for a while and I do not see anyone in the kitchen. Turn it off?",
                        "timeout_seconds": 120,
                        "yes_actions": [{"type": "call_service", "domain": "switch", "service": "turn_off", "entity_id": stove_entity}],
                    }
                ],
                "mode": "single",
                "cooldown_seconds": 1800,
            },
        ),
        (
            "door_left_open",
            "Door-open watch",
            {
                "name": "Door-open watch",
                "builtin": True,
                "builtin_id": "door_left_open",
                "enabled": False,
                "trigger": {"type": "entity_state", "entity": door_entity, "state": "on", "for_seconds": 600},
                "conditions": [{"type": "time_window", "after": "22:00", "before": "06:00"}],
                "actions": [
                    {
                        "type": "notify",
                        "platform": "webui",
                        "title": "Door left open",
                        "message": f"{door_entity} has been open for 10 minutes tonight.",
                        "priority": "high",
                    }
                ],
                "mode": "single",
                "cooldown_seconds": 3600,
            },
        ),
        (
            "wellbeing_floor_check",
            "Floor check (living room)",
            {
                "name": "Floor check (living room)",
                "builtin": True,
                "builtin_id": "wellbeing_floor_check",
                "enabled": False,
                "trigger": {"type": "interval", "seconds": 60},
                "conditions": [
                    {
                        "type": "camera_vision",
                        "camera": camera_ref or "great room",
                        "prompt": "Is a person lying on the floor in an unusual or unsafe position?",
                        "expect": True,
                        "hold_seconds": 900,
                        "check_seconds": 60,
                    },
                    {"type": "time_window", "after": "09:00", "before": "23:00"},
                ],
                "actions": [
                    {
                        "type": "ask_yes_no",
                        "targets": "all_satellites",
                        "message": "I have detected someone on the floor who has not been moving for some time. Are you okay?",
                        "timeout_seconds": 120,
                        "yes_actions": [{"type": "announce", "message": "Okay — glad you are alright.", "targets": "all_satellites"}],
                        "no_actions": [
                            {"type": "notify", "platform": "webui", "title": "Floor check: help declined",
                             "message": "Someone on the floor said they are NOT okay.", "priority": "urgent"}
                        ],
                        "unanswered_actions": [
                            {"type": "notify", "platform": "webui", "title": "No answer to floor check",
                             "message": "The floor-presence alert went unanswered.", "priority": "urgent"}
                        ],
                    }
                ],
                "mode": "single",
                "cooldown_seconds": 900,
            },
        ),
        (
            "welcome_home",
            "Welcome home",
            {
                "name": "Welcome home",
                "builtin": True,
                "builtin_id": "welcome_home",
                "enabled": False,
                "trigger": {"type": "protect_event", "camera": camera_ref, "event_type": "person"},
                "conditions": [{"type": "camera_face", "camera": camera_ref, "mode": "any_person"}],
                "actions": [{"type": "announce", "message": "Welcome home.", "targets": "all_satellites"}],
                "mode": "single",
                "cooldown_seconds": 1800,
            },
        ),
    ]


def _seed_builtins(rc: Any = None) -> List[str]:
    """Seed the builtin automations once per install (marker-gated), always disabled.
    Every builtin must validate with the same validate_definition authors run; a
    builtin that fails validation is re-attempted on the next start (marker stays
    unset). Returns the ids seeded this call."""
    rc = rc if rc is not None else _redis()
    existing = _load_automations(rc)
    try:
        already = _bool(rc.get(BUILTINS_MARKER), False)
    except Exception:
        already = False
    if already:
        return []
    seeds = _builtin_definitions_for_seed(rc)
    seeded: List[str] = []
    blocking_failures = 0
    for builtin_id, name, definition in seeds:
        auto_id = f"a_builtin_{builtin_id}"
        if auto_id in existing:
            continue
        errors, _warnings = validate_definition(definition, rc)
        if errors:
            blocking_failures += 1
            logger.warning("[automation_ga] builtin '%s' failed seeding validation: %s", builtin_id, "; ".join(errors))
            continue
        rc.hset(
            AUTOMATIONS_KEY,
            auto_id,
            json.dumps(definition, separators=(",", ":"), default=str),
        )
        _log_activity(rc, auto_id, name, "builtin seeded (disabled)", "")
        seeded.append(auto_id)
    if blocking_failures == 0:
        try:
            rc.set(BUILTINS_MARKER, "true")
        except Exception:
            pass
    return seeded


def _ha_entity_options(
    rc: Any,
    *,
    rows: Optional[Dict[str, Dict[str, Any]]] = None,
    current_values: Sequence[Any] = (),
) -> List[Dict[str, str]]:
    """Dropdown options for HA entities: friendly names, noise domains filtered out."""
    entities = rows if rows is not None else _ha_entities(rc)
    out: List[Dict[str, str]] = []
    for entity_id in sorted(entities, key=lambda e: (e.casefold())):
        row = entities[entity_id]
        domain = entity_id.split(".", 1)[0]
        if domain in _ENTITY_STATE_SKIP_DOMAINS:
            continue
        friendly = _text(row.get("friendly_name"))
        label = f"{friendly} ({entity_id})" if friendly and friendly != entity_id else entity_id
        out.append({"value": entity_id, "label": label})
    for value in current_values:
        token = _text(value)
        if token and not any(row.get("value") == token for row in out):
            out.append({"value": token, "label": f"{token} (saved)"})
    out.sort(key=lambda row: (_text(row.get("label")).casefold(), _text(row.get("value"))))
    return out


def _entity_state_dependency(
    rc: Any,
    entity_ids: Iterable[Any],
    *,
    source_key: str,
    empty_label: str = "",
    fallback_default: Optional[List[Dict[str, str]]] = None,
    ensure: Optional[Dict[str, Sequence[Any]]] = None,
    rows: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """dependent_options map: entity_id -> candidate states (plain strings) for the WebUI renderer.

    empty_label prepends a "no value" option to every entity's list (e.g. "Any state");
    ensure pins values (e.g. a saved state history has pruned) into their entity's list.
    """
    ensure_map = {
        _text(entity_id): [_text(value) for value in values if _text(value)]
        for entity_id, values in (ensure or {}).items()
    }
    options_by_source: Dict[str, List[Any]] = {}
    for entity_id in entity_ids:
        token = _text(entity_id)
        if not token:
            continue
        states, _source = _state_catalog_states(rc, token, rows=rows)
        if not states:
            continue
        option_rows: List[Any] = [{"value": "", "label": empty_label}] if empty_label else []
        option_rows.extend(states[:_STATE_FORM_MAX_OPTIONS])
        for extra in ensure_map.get(token, ()):
            if all(_text(row.get("value") if isinstance(row, dict) else row) != extra for row in option_rows):
                option_rows.append(extra)
        options_by_source[token] = option_rows
    return {
        "source_key": source_key,
        "options_by_source": options_by_source,
        "default_options": list(fallback_default or []),
    }


def _definition_watch_entities(definition: Dict[str, Any]) -> List[str]:
    """Entity ids referenced by one definition's trigger + entity_state conditions."""
    out: List[str] = []
    sections: List[Any] = [definition.get("trigger")]
    conditions = definition.get("conditions")
    if isinstance(conditions, list):
        sections.extend(conditions)
    for spec in sections:
        if isinstance(spec, dict) and _text(spec.get("type")) == "entity_state":
            entity_id = _text(spec.get("entity"))
            if entity_id and entity_id not in out:
                out.append(entity_id)
    return out


def _entity_activity_hint(rc: Any, entity_id: str, rows: Optional[Dict[str, Dict[str, Any]]] = None) -> str:
    """One-line 'currently X, recent transitions' hint for the form, empty when nothing is known."""
    entity_id = _text(entity_id)
    if not entity_id:
        return ""
    if rows is None:
        rows = _ha_entities(rc)
    row = rows.get(entity_id)
    live = _text(row.get("state")) if row else ""
    parts: List[str] = []
    if live:
        parts.append(f"currently '{live}'")
    transitions = _state_catalog_transitions(rc, entity_id)
    if transitions:
        chain = " → ".join(f"{frm}→{to}" for frm, to, _ts in transitions[-4:])
        parts.append(f"recent: {chain}")
    return "; ".join(parts)


# ---------------------------------------------------------------------------
# integration events (UniFi Protect person detection etc.)
# ---------------------------------------------------------------------------

def _recent_events(client: Any, limit: int = 400) -> List[Dict[str, Any]]:
    rc = client if client is not None else _redis()
    try:
        raw_rows = rc.lrange(INTEGRATION_EVENTS_KEY, 0, max(0, limit - 1)) or []
    except Exception:
        return []
    rows: List[Dict[str, Any]] = []
    for raw in raw_rows:
        row = _json_loads(raw, {})
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _event_matches_camera(payload: Dict[str, Any], camera: str) -> bool:
    wanted = _text(camera).lower()
    if not wanted:
        return True
    haystacks: List[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for value in node.values():
                walk(value)
        elif isinstance(node, (list, tuple)):
            for value in node:
                walk(value)
        elif isinstance(node, str):
            haystacks.append(node.lower())

    walk(payload)
    return any(wanted in blob for blob in haystacks)


def _protect_people_seen(client: Any, camera: str, lookback_seconds: float) -> Tuple[bool, str]:
    rc = client if client is not None else _redis()
    cutoff = time.time() - max(10.0, _as_float(lookback_seconds, 120.0))
    motion_only = False
    for row in _recent_events(rc, limit=600):
        if _text(row.get("provider")) != "unifi_protect":
            continue
        if _as_float(row.get("ts"), 0.0) < cutoff:
            continue
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        event_type = _text(
            payload.get("type") or payload.get("eventType") or payload.get("event_type")
        ).lower()
        if "person" not in event_type and "motion" not in event_type:
            continue
        if not _event_matches_camera(payload, camera):
            continue
        if "person" in event_type:
            ts = _as_float(row.get("ts"), 0.0)
            detail = f"person event on camera '{_text(camera)}' at {datetime.fromtimestamp(ts).strftime('%H:%M:%S')}" if ts else f"person event on camera '{_text(camera)}'"
            return True, detail
        motion_only = True
    if motion_only:
        return True, "motion (no person classification) on camera"
    return False, f"no protect person events for '{_text(camera) or 'any camera'}' in the last {_as_float(lookback_seconds, 120):.0f}s"


def _protect_event_after(client: Any, camera: str, event_type: str, last_seq: int) -> Tuple[Optional[int], str]:
    rc = client if client is not None else _redis()
    wanted = _text(event_type).lower() or "event"
    for row in _recent_events(rc, limit=200):
        if _text(row.get("provider")) != "unifi_protect":
            continue
        seq = _as_int(row.get("seq"), 0)
        if seq <= last_seq:
            continue
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        row_type = _text(
            payload.get("type") or payload.get("eventType") or payload.get("event_type")
        ).lower()
        if wanted not in row_type:
            continue
        if not _event_matches_camera(payload, camera):
            continue
        return seq, f"{row_type} event seq={seq}"
    return None, ""


# ---------------------------------------------------------------------------
# BLE presence
# ---------------------------------------------------------------------------

def _presence_trackers() -> Dict[str, str]:
    """tracker name/alias -> 'home' | 'away' from the native BLE presence engine."""
    try:
        from tater_voice import native_ble

        snapshot = native_ble.snapshot()
    except Exception as exc:
        logger.debug("presence snapshot unavailable: %s", exc)
        return {}
    trackers: Dict[str, str] = {}
    raw_trackers = snapshot.get("trackers") if isinstance(snapshot, dict) else None
    rows: List[Any] = []
    if isinstance(raw_trackers, dict):
        rows = list(raw_trackers.values())
    elif isinstance(raw_trackers, list):
        rows = raw_trackers
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = _text(row.get("name") or row.get("alias") or row.get("id")).lower()
        state = _text(row.get("state") or row.get("tracker_state") or row.get("presence")).lower()
        if name:
            trackers[name] = state or "unknown"
    return trackers


# ---------------------------------------------------------------------------
# camera media capture + vision (snapshots / clips / descriptions)
# ---------------------------------------------------------------------------

def _snapshot_result_bytes(result: Any) -> Tuple[bytes, str]:
    if isinstance(result, tuple) and len(result) >= 1:
        content = result[0]
        content_type = _text(result[1]) if len(result) > 1 else "image/jpeg"
    elif isinstance(result, dict):
        content = result.get("bytes") or result.get("content") or result.get("image")
        content_type = _text(result.get("content_type") or result.get("mimetype") or "image/jpeg")
        if not content and _text(result.get("base64")):
            content = base64.b64decode(_text(result.get("base64")))
    else:
        content = result
        content_type = "image/jpeg"
    if isinstance(content, str):
        try:
            content = base64.b64decode(content)
        except Exception:
            content = b""
    if not isinstance(content, (bytes, bytearray)) or not content:
        raise RuntimeError("The camera integration returned no snapshot image.")
    return bytes(content), content_type or "image/jpeg"


def _clip_result_bytes(result: Any) -> Tuple[bytes, str]:
    if isinstance(result, tuple) and result:
        content = result[0]
        content_type = _text(result[1]) if len(result) > 1 else "video/mp4"
    elif isinstance(result, dict):
        content = result.get("bytes") or result.get("content") or result.get("video_bytes")
        content_type = _text(
            result.get("content_type") or result.get("mime_type") or result.get("mimetype") or "video/mp4"
        )
        encoded = _text(result.get("base64"))
        if not content and encoded:
            content = encoded
    else:
        content = result
        content_type = "video/mp4"
    if isinstance(content, str):
        encoded = content
        if encoded.startswith("data:") and "," in encoded:
            header, encoded = encoded.split(",", 1)
            content_type = header[5:].split(";", 1)[0] or content_type
        try:
            content = base64.b64decode(encoded)
        except Exception:
            content = b""
    if not isinstance(content, (bytes, bytearray)) or not content:
        raise RuntimeError("The camera integration returned no video clip.")
    return bytes(content), content_type or "video/mp4"


def _camera_clip_payload(context: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "duration_seconds": 8,
        "pre_event_seconds": 2,
        "post_event_seconds": 4,
        "event_id": _text(context.get("event_id")),
        "event_start": context.get("event_start"),
        "event_end": context.get("event_end"),
    }


def _normalize_vision_provider(value: Any) -> str:
    token = _text(value).lower().replace("-", "_").replace(" ", "_")
    if token in {
        "hf",
        "huggingface",
        "hugging_face",
        "transformers",
        "hf_transformers",
        "local_transformers",
    }:
        return "hf_transformers"
    if token in {
        "llama",
        "llamacpp",
        "llama_cpp",
        "llama.cpp",
        "gguf",
        "llama_cpp_python",
    }:
        return "llama_cpp"
    if token in {
        "mlx",
        "mlx_lm",
        "apple_mlx",
        "apple_silicon",
        "mlxlm",
    }:
        return "mlx_lm"
    return "openai_compatible"


def _is_local_vision_provider(value: Any) -> bool:
    return _normalize_vision_provider(value) in {"hf_transformers", "llama_cpp", "mlx_lm"}


def _base_vision_target(client: Any = None) -> Tuple[str, str]:
    if resolve_hydra_base_servers is None:
        return "", ""
    try:
        rows = resolve_hydra_base_servers(redis_conn=client if client is not None else _redis(), include_legacy=True)
    except Exception:
        logger.exception("[automation_ga] failed to read base LLM settings for vision routing")
        return "", ""
    row = dict(rows[0]) if rows and isinstance(rows[0], dict) else {}
    return _normalize_vision_provider(row.get("provider")), _text(row.get("model"))


def _describe_snapshot_local(
    image_bytes: bytes,
    prompt: str,
    provider: str,
    model: str,
) -> str:
    if describe_image_with_local_llm is None:
        raise RuntimeError("Local vision is unavailable in this Tater runtime.")
    result = describe_image_with_local_llm(
        provider=provider,
        model=model,
        image_bytes=image_bytes,
        filename="tater-automation-camera.jpg",
        prompt=prompt,
        timeout=90.0,
    )
    description = _text((result or {}).get("description"))
    if not description:
        raise RuntimeError("The local vision model returned no description.")
    return description


def _describe_snapshot_sync(image_bytes: bytes, content_type: str, prompt: str, client: Any = None) -> str:
    if callable(_shared_describe_image_bytes):
        result = _shared_describe_image_bytes(
            image_bytes=image_bytes,
            filename="tater-automation-camera.jpg",
            prompt=prompt,
        )
        description = _text((result or {}).get("description") or (result or {}).get("text"))
        if description:
            return description
        error = _text((result or {}).get("error"))
        if error:
            raise RuntimeError(error)
    if get_vision_settings is None:
        raise RuntimeError("Vision is not available in this Tater runtime.")
    if requests is None:
        raise RuntimeError("The requests library is unavailable; cannot call a vision API.")
    settings = get_vision_settings(
        default_api_base="http://127.0.0.1:1234",
        default_model="qwen2.5-vl-7b-instruct",
    )
    routing_mode = _token(settings.get("mode") or "api")
    if routing_mode not in {"api", "auto", "base", "dedicated"}:
        routing_mode = "api"
    provider = _normalize_vision_provider(settings.get("provider") or "openai_compatible")
    model = _text(settings.get("model") or "qwen2.5-vl-7b-instruct")

    if routing_mode == "dedicated" and _is_local_vision_provider(provider):
        return _describe_snapshot_local(image_bytes, prompt, provider, model)

    if routing_mode in {"auto", "base"}:
        base_provider, base_model = _base_vision_target(client)
        if _is_local_vision_provider(base_provider) and base_model:
            try:
                return _describe_snapshot_local(
                    image_bytes,
                    prompt,
                    base_provider,
                    base_model,
                )
            except Exception:
                if routing_mode == "base":
                    raise
                logger.exception(
                    "[automation_ga] local base vision failed; falling back to configured vision API"
                )
        elif routing_mode == "base":
            raise RuntimeError(
                "Vision is set to use the base model, but the base LLM is not a local provider."
            )

    api_base = _text(settings.get("api_base") or "http://127.0.0.1:1234").rstrip("/")
    api_key = _text(settings.get("api_key"))
    b64 = base64.b64encode(image_bytes).decode("ascii")
    data_url = f"data:{content_type or 'image/jpeg'};base64,{b64}"
    body = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Describe only visible, relevant facts for a short home automation alert. "
                    "Do not invent identity, intent, or details that are not visible."
                ),
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": data_url}},
                ],
            },
        ],
        "temperature": 0.2,
        "max_tokens": 160,
    }
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    response = requests.post(
        f"{api_base}/v1/chat/completions",
        headers=headers,
        data=json.dumps(body),
        timeout=45,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Vision HTTP {response.status_code}: {response.text[:200]}")
    payload = response.json() or {}
    description = _text(((payload.get("choices") or [{}])[0].get("message") or {}).get("content"))
    if not description:
        raise RuntimeError("The vision model returned no description.")
    return description


def _describe_video_sync(video_bytes: bytes, content_type: str, prompt: str) -> str:
    if not callable(_shared_video_analyze):
        raise RuntimeError("Video Understanding is unavailable in this Tater runtime.")
    extension = {
        "video/webm": "webm",
        "video/quicktime": "mov",
        "video/x-matroska": "mkv",
    }.get(_text(content_type).lower(), "mp4")
    result = _shared_video_analyze(
        media_ref={
            "bytes": bytes(video_bytes or b""),
            "name": f"tater-automation-camera.{extension}",
            "mimetype": _text(content_type) or "video/mp4",
        },
        prompt=prompt,
    )
    if not isinstance(result, dict) or not result.get("ok"):
        error = result.get("error") if isinstance(result, dict) else {}
        message = _text(error.get("message")) if isinstance(error, dict) else ""
        raise RuntimeError(message or "Video Understanding could not analyze the camera clip.")
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    description = _text(
        data.get("description") or data.get("text") or result.get("summary_for_user")
    ).strip()
    if not description:
        raise RuntimeError("The video model returned no description.")
    return description


# ---------------------------------------------------------------------------
# Face ID (shared People profiles)
# ---------------------------------------------------------------------------

def _camera_face_id_readiness(client: Any) -> Dict[str, Any]:
    if _shared_face_identity is None:
        return {"status": "disabled", "warning": "Face ID is unavailable in this Tater runtime."}
    try:
        status = dict(_shared_face_identity.runtime_status(client) or {})
    except Exception as exc:
        return {"status": "not_ready", "warning": f"Face ID status unavailable: {exc}"}
    if not _bool(status.get("enabled"), False):
        return {
            "status": "disabled",
            "warning": "Face ID is disabled in Settings › Models.",
        }
    if not _bool(status.get("loaded"), False):
        return {
            "status": "not_ready",
            "warning": "Face ID is enabled but its model is not ready yet.",
        }
    return {"status": "ready", "warning": ""}


def _recognize_camera_faces_with_timeout(
    client: Any,
    image_bytes: bytes,
    *,
    source: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    def _run() -> Dict[str, Any]:
        if _shared_face_identity is None:
            raise RuntimeError("Face ID is unavailable in this Tater runtime.")
        return _shared_face_identity.recognize_image(
            image_bytes,
            event_id=f"automation_ga_{uuid.uuid4().hex[:16]}",
            source=source,
            record=True,
            redis_client=client,
        )

    future = _RUNNER_EXECUTOR.submit(_run)
    try:
        return future.result(timeout=_CAMERA_FACE_ID_TIMEOUT_SECONDS)
    except concurrent.futures.TimeoutError:
        return {
            "status": "timeout",
            "warning": "Face ID did not finish before the automation timeout.",
            "people": [],
            "identity_ids": [],
        }
    except Exception as exc:
        return {
            "status": "error",
            "warning": _text(exc) or "Face ID failed.",
            "people": [],
            "identity_ids": [],
        }


def _camera_person_check(
    client: Any,
    camera_ref: Any,
    person_wanted: str,
    lookback_seconds: float = 60.0,
) -> Tuple[bool, str, str]:
    """Shared camera presence evaluation: any-person tier (Protect person events
    within a lookback) or named tier (snapshot + Face ID identity match).

    Returns (matched, detail, matched_name). Used by the camera_face condition and
    by the v1.4.0 per-person suggestion-delivery rows — one helper, two consumers.
    """
    rc = client if client is not None else _redis()
    camera = _text(camera_ref)
    person_wanted = _text(person_wanted)
    device = _resolve_camera_device(rc, camera)
    if not device:
        return False, f"camera '{camera}' not found in the integration device registry", ""
    seen, seen_detail = _protect_people_seen(rc, camera, max(15.0, _as_float(lookback_seconds, 60.0)))
    if not person_wanted:
        if seen:
            return True, f"person events on '{_text(device.get('name')) or camera}' — {seen_detail}", ""
        return False, f"no one seen on '{_text(device.get('name')) or camera}' — {seen_detail}", ""
    readiness = _camera_face_id_readiness(rc)
    if readiness.get("status") != "ready":
        # Named tier degrades to any-person when Face ID is unavailable (validated
        # with a warning at authoring time; mirrored in tests).
        if seen:
            return True, f"face-id not ready ({readiness.get('warning') or 'unavailable'}); treating as any-person — {seen_detail}", ""
        return False, f"face-id not ready ({readiness.get('warning') or 'unavailable'}); no one seen — {seen_detail}", ""
    if run_integration_device_action is None:
        return False, "integration device actions are unavailable in this Tater runtime", ""
    actions = set(_device_actions(device))
    snapshot_action = next((name for name in ("camera_snapshot", "snapshot") if name in actions), "")
    if not snapshot_action and _camera_supports_media_mode(device, "image"):
        snapshot_action = "camera_snapshot"
    if not snapshot_action:
        return False, f"camera '{_text(device.get('name')) or camera}' does not expose snapshots (face-id tier)", ""
    try:
        snapshot_result = run_integration_device_action(
            _text(device.get("integration_id")),
            snapshot_action,
            _device_id(device),
            {},
        )
        image_bytes, image_content_type = _snapshot_result_bytes(snapshot_result)
    except Exception as exc:
        return False, f"camera snapshot failed: {exc}", ""
    source = {
        "integration_id": _text(device.get("integration_id")),
        "device_id": _device_id(device),
        "name": _text(device.get("name")),
    }
    face_result = _recognize_camera_faces_with_timeout(rc, image_bytes, source=source)
    people = [_text(name) for name in (face_result.get("people") or []) if _text(name)]
    wanted = person_wanted.lower()
    for name in people:
        if wanted and wanted in name.lower():
            label = _text(device.get("name")) or camera
            return True, f"face-id matched '{name}' on '{label}'", name
    if people:
        return False, f"face-id saw {', '.join(people)} — no match for '{person_wanted}'", ""
    extra = _text(face_result.get("warning"))
    return False, "face-id recognized nobody" + (f" ({extra})" if extra else ""), ""


def _camera_face_people_text(people: Sequence[Any]) -> str:
    names = [_text(value) for value in people if _text(value)]
    if len(names) <= 1:
        return names[0] if names else ""
    if len(names) == 2:
        return f"{names[0]} and {names[1]}"
    return f"{', '.join(names[:-1])}, and {names[-1]}"


def _camera_prompt_with_face_context(prompt: Any, face_result: Dict[str, Any]) -> str:
    base_prompt = _text(prompt)
    people_text = _camera_face_people_text(face_result.get("people") or [])
    if people_text:
        subject = "visitor" if len(face_result.get("people") or []) == 1 else "visitors"
        context = (
            f"Face ID independently recognized the {subject} as {people_text}. "
            "Use this exact name naturally when appropriate, and do not infer any other identity."
        )
    else:
        context = (
            "Face ID did not provide a known visitor name. Use a neutral greeting and do not guess "
            "the visitor's identity."
        )
    return f"{base_prompt}\n\n{context}" if base_prompt else context


# ---------------------------------------------------------------------------
# templates, speech settings, announcements
# ---------------------------------------------------------------------------

def _render_template(value: Any, context: Dict[str, Any]) -> str:
    text = _text(value)
    for key in (
        "device",
        "room",
        "event",
        "state",
        "old_state",
        "value",
        "category",
        "provider",
        "vision",
        "person",
        "person_id",
    ):
        text = text.replace("{" + key + "}", _text(context.get(key)))
    return text


def _render_template_deep(value: Any, context: Dict[str, Any]) -> Any:
    """Render {placeholder}s in every nested string (webhook payloads); other scalars pass through."""
    if isinstance(value, str):
        return _render_template(value, context)
    if isinstance(value, list):
        return [_render_template_deep(item, context) for item in value]
    if isinstance(value, dict):
        return {key: _render_template_deep(item, context) for key, item in value.items()}
    return value


def _speech_settings() -> Dict[str, Any]:
    fallback: Dict[str, Any] = {
        "backend": "wyoming",
        "model": "",
        "voice": "",
        "wyoming_host": "",
        "wyoming_port": None,
        "wyoming_voice": "",
        "voice_core_backend": "",
        "voice_core_model": "",
        "voice_core_voice": "",
        "voice_core_wyoming_host": "",
        "voice_core_wyoming_port": None,
        "voice_core_wyoming_voice": "",
    }
    if get_speech_settings is None:
        return fallback
    try:
        shared = get_speech_settings() or {}
    except Exception:
        return fallback
    if not isinstance(shared, dict):
        return fallback
    return {
        "backend": _text(shared.get("announcement_tts_backend") or shared.get("tts_backend") or "wyoming"),
        "model": _text(shared.get("announcement_tts_model")),
        "voice": _text(shared.get("announcement_tts_voice")),
        "wyoming_host": _text(shared.get("wyoming_tts_host")),
        "wyoming_port": shared.get("wyoming_tts_port"),
        "wyoming_voice": _text(shared.get("wyoming_tts_voice")),
        "voice_core_backend": _text(shared.get("tts_backend")),
        "voice_core_model": _text(shared.get("tts_model")),
        "voice_core_voice": _text(shared.get("tts_voice")),
        "voice_core_wyoming_host": _text(shared.get("wyoming_tts_host")),
        "voice_core_wyoming_port": shared.get("wyoming_tts_port"),
        "voice_core_wyoming_voice": _text(shared.get("wyoming_tts_voice")),
    }


def _voice_satellite_targets(client: Any = None) -> List[str]:
    try:
        from announcement_targets import get_voice_core_satellite_target_options

        rows = get_voice_core_satellite_target_options()
        targets = []
        for row in rows:
            value = _text(row.get("value") if isinstance(row, dict) else row)
            if value:
                targets.append(value if value.startswith("voice_core:") else f"voice_core:{value}")
        return targets
    except Exception as exc:
        logger.debug("satellite target discovery failed: %s", exc)
        return []


def _resolve_announce_targets(client: Any, targets: Any) -> List[str]:
    rc = client if client is not None else _redis()
    default_targets = _text(_setting(rc, "default_announce_targets", "all_satellites"))
    raw = targets if targets not in (None, "") else default_targets
    if isinstance(raw, str):
        items = [item.strip() for item in raw.split(",") if item.strip()]
    elif isinstance(raw, (list, tuple)):
        items = [_text(item) for item in raw if _text(item)]
    else:
        items = []
    if any(item.lower() in {"all_satellites", "all_sats", "all"} for item in items):
        satellite_targets = _voice_satellite_targets(rc)
        rest = [item for item in items if item.lower() not in {"all_satellites", "all_sats", "all"}]
        return satellite_targets + rest
    return items


def _announce_blocking(
    message: str,
    targets: List[str],
    audio_scene: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, str]:
    try:
        if speak_announcement_targets is None:
            return False, "speech_tts is unavailable in this Tater runtime"
        from announcement_targets import normalize_announcement_targets

        resolved = normalize_announcement_targets(targets)
        if not resolved:
            return False, "No announcement targets resolved (check satellites/announce settings)."
        conf = _homeassistant_config()
        settings = _speech_settings()
        scene = _normalize_tts_audio_scene(audio_scene)

        async def _run() -> Dict[str, Any]:
            return await speak_announcement_targets(
                text=message,
                backend=settings["backend"],
                ha_base=conf["base"],
                token=conf["token"],
                targets=resolved,
                model=settings["model"],
                voice=settings["voice"],
                wyoming_host=settings["wyoming_host"],
                wyoming_port=settings["wyoming_port"],
                wyoming_voice=settings["wyoming_voice"],
                voice_core_backend=settings["voice_core_backend"],
                voice_core_model=settings["voice_core_model"],
                voice_core_voice=settings["voice_core_voice"],
                voice_core_wyoming_host=settings["voice_core_wyoming_host"],
                voice_core_wyoming_port=settings["voice_core_wyoming_port"],
                voice_core_wyoming_voice=settings["voice_core_wyoming_voice"],
                default_backend=settings["backend"],
                public_base_url="",
                tts_kind="automation_announce",
                audio_scene=scene,
            )

        result = asyncio.run(_run())
        if isinstance(result, dict) and (result.get("error") or result.get("ok") is False):
            return False, _text(result.get("error")) or "Announcement failed"
        return True, f"Announced to {len(resolved)} target(s)"
    except Exception as exc:
        logger.exception("announce failed")
        return False, f"Announcement failed: {exc}"


def _announce_async(
    message: str,
    targets: List[str],
    audio_scene: Optional[Dict[str, Any]] = None,
) -> None:
    """Fire-and-forget announce usable from inside a running event loop."""
    thread = threading.Thread(
        target=_announce_blocking,
        args=(message, targets, audio_scene),
        daemon=True,
        name="automation-ga-announce",
    )
    thread.start()


# ---------------------------------------------------------------------------
# announce message resolution (fixed text / random list / LLM-generated style)
# ---------------------------------------------------------------------------

_ANNOUNCE_GEN_SYSTEM_PROMPT = (
    "You write short spoken home-automation announcements for a home assistant's "
    "text-to-speech. You are given a reference announcement. Write ONE fresh "
    "variation of it: keep the same meaning, facts and tone, but use different "
    "words and phrasing — never copy the reference word for word. Output only the "
    "announcement text: no quotes, no labels, no preamble. One to three short "
    "sentences of plain spoken language."
)
_ANNOUNCE_GEN_TIMEOUT_SECONDS = 25.0
_ANNOUNCE_GEN_MAX_CHARS = 600


def _generate_announce_text(reference: str, client: Any = None, timeout: float = _ANNOUNCE_GEN_TIMEOUT_SECONDS) -> str:
    """Ask the primary Hydra base LLM for a fresh variation of the reference message."""
    if _shared_get_primary_llm_client is None:
        raise RuntimeError("the Tater base LLM client is unavailable in this runtime")

    # A per-run seed so consecutive runs of the same reference do not produce the
    # same variation (an identical prompt would yield nearly identical text).
    seed = random.randint(1, 999999)
    user_prompt = (
        f"Reference announcement:\n{reference}\n\n"
        f"Write a fresh variation of the reference above. Keep its meaning, facts "
        f"and tone; change the words. (Variation seed: {seed})"
    )

    def _run() -> str:
        async def _chat() -> str:
            async with _shared_get_primary_llm_client(redis_conn=client) as llm:
                result = await llm.chat(
                    [
                        {"role": "system", "content": _ANNOUNCE_GEN_SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    timeout=timeout,
                    max_tokens=150,
                    temperature=0.9,
                )
            if not isinstance(result, dict):
                return ""
            message = result.get("message") if isinstance(result.get("message"), dict) else {}
            return _text(message.get("content"))

        # asyncio.run on the executor thread: safe even if the caller is inside a
        # running event loop (the runner tick is not, but Hydra tool calls may be).
        return asyncio.run(_chat())

    future = _RUNNER_EXECUTOR.submit(_run)
    return future.result(timeout=timeout + 10.0)


def _resolve_announce_message(action: Dict[str, Any], client: Any = None) -> Tuple[Optional[str], str]:
    """Resolve the announce text: 'message' (fixed), 'messages' (random pick) or
    'message_style' (a fresh variation of the reference, written by the base LLM each
    run). Returns (text, error)."""
    message = _text(action.get("message"))
    if message:
        return message, ""
    raw_list = action.get("messages")
    if isinstance(raw_list, list) and raw_list:
        options = [text for text in (_text(item) for item in raw_list) if text]
        if options:
            return random.choice(options), ""
    reference = _text(action.get("message_style"))
    if reference:
        try:
            generated = _generate_announce_text(reference, client).strip().strip("\"'“”‘’").strip()
        except Exception as exc:
            return None, f"could not generate announcement text: {exc}"
        if not generated:
            return None, "the base LLM returned no announcement text"
        if len(generated) > _ANNOUNCE_GEN_MAX_CHARS:
            generated = generated[:_ANNOUNCE_GEN_MAX_CHARS].rsplit(" ", 1)[0] + "…"
        return generated, ""
    return None, "no announcement text (set 'message', 'messages' or 'message_style')"


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------

def _check_entity_known(client: Any, entity_id: str, warnings: List[str]) -> None:
    rc = client if client is not None else _redis()
    entities = _ha_entities(rc)
    if entities and _text(entity_id) not in entities:
        warnings.append(f"Entity '{entity_id}' was not found in the Home Assistant state cache.")


def _validate_entity_spec(
    spec: Dict[str, Any],
    label: str,
    errors: List[str],
    warnings: List[str],
    client: Any,
) -> None:
    if not _text(spec.get("entity")):
        errors.append(f"{label}: entity_state requires 'entity'")
        return
    _check_entity_known(client, spec.get("entity"), warnings)
    from_state = _text(spec.get("from_state"))
    to_state = _text(spec.get("to_state"))
    if from_state or to_state:
        if _text(spec.get("state")):
            warnings.append(f"{label}: 'state' is ignored when from_state/to_state are set — the trigger fires on transitions")
        if _text(spec.get("attribute")):
            errors.append(f"{label}: from_state/to_state cannot be combined with attribute matching")
        return
    if _text(spec.get("attribute")):
        match_op = _text(spec.get("match") or "equals").lower()
        if match_op not in ENTITY_MATCH_OPS:
            errors.append(f"{label}: attribute match must be one of {', '.join(ENTITY_MATCH_OPS)}")
        if _text(spec.get("value")) == "":
            errors.append(f"{label}: attribute matching requires 'value'")
    elif not _text(spec.get("state")):
        errors.append(f"{label}: entity_state requires 'state' (or 'attribute' + 'match' + 'value')")


def _announce_message_present(action: Dict[str, Any], key: str) -> bool:
    """True when an announce message source ('message'/'messages'/'message_style') is meaningfully set."""
    if key == "messages":
        items = action.get("messages")
        return isinstance(items, list) and any(_text(item) for item in items)
    return bool(_text(action.get(key)))


def _validate_actions(
    actions: Any,
    *,
    allow_ask: bool,
    path: str,
    errors: List[str],
    warnings: List[str],
    client: Any,
) -> None:
    if not isinstance(actions, list):
        errors.append(f"{path} must be a list of actions")
        return
    for index, action in enumerate(actions):
        label = f"{path}[{index}]"
        if not isinstance(action, dict):
            errors.append(f"{label} is not an object")
            continue
        action_type = _text(action.get("type"))
        if action_type == "ask_yes_no":
            if not allow_ask:
                errors.append(f"{label}: ask_yes_no may not be nested inside another ask_yes_no")
                continue
        elif action_type in ("camera_ai", "device") and not allow_ask:
            errors.append(
                f"{label}: {action_type} may not be used inside ask_yes_no branches "
                "(only call_service/announce/notify/webhook/wait)"
            )
            continue
        elif action_type not in ALL_ACTION_TYPES:
            errors.append(f"{label}: unknown action type '{action_type}' (expected one of {', '.join(ALL_ACTION_TYPES)})")
            continue
        if action_type == "call_service":
            if not _text(action.get("domain")) or not _text(action.get("service")):
                errors.append(f"{label}: call_service requires 'domain' and 'service'")
            if _text(action.get("entity_id")):
                _check_entity_known(client, action.get("entity_id"), warnings)
        elif action_type in ("announce", "ask_yes_no"):
            if action_type == "ask_yes_no" and not _text(action.get("message")):
                errors.append(f"{label}: ask_yes_no requires a fixed 'message' (the spoken question)")
            if action_type == "announce":
                sources = [key for key in ("message", "messages", "message_style") if _announce_message_present(action, key)]
                if len(sources) != 1:
                    errors.append(
                        f"{label}: announce needs exactly one of 'message' (fixed text), "
                        f"'messages' (list — one is picked at random each run) or "
                        f"'message_style' (the base LLM writes a fresh variation of it each run)"
                    )
                elif sources[0] == "messages":
                    items = action.get("messages")
                    if not isinstance(items, list) or not items or not all(_text(item) for item in items):
                        errors.append(f"{label}: 'messages' must be a non-empty list of non-empty strings")
                    elif len(items) == 1:
                        warnings.append(f"{label}: a one-entry 'messages' list never varies — use 'message' instead")
                elif sources[0] == "message_style":
                    if len(_text(action.get("message_style"))) < 12:
                        warnings.append(f"{label}: 'message_style' is very short — enter the reference message (or a clear description of its tone and content)")
                    if _shared_get_primary_llm_client is None:
                        warnings.append(f"{label}: 'message_style' needs the Tater base LLM, which is unavailable in this runtime — the announcement will fail at run time")
            if action_type == "ask_yes_no":
                yes_actions = action.get("yes_actions")
                if not isinstance(yes_actions, list) or not yes_actions:
                    errors.append(f"{label}: ask_yes_no requires a non-empty 'yes_actions' list")
                for branch in ("yes_actions", "no_actions", "unanswered_actions"):
                    if isinstance(action.get(branch), list):
                        _validate_actions(
                            action.get(branch),
                            allow_ask=False,
                            path=f"{label}.{branch}",
                            errors=errors,
                            warnings=warnings,
                            client=client,
                        )
        elif action_type == "wait":
            if _as_float(action.get("seconds"), 0) <= 0:
                errors.append(f"{label}: wait requires positive 'seconds'")
        elif action_type == "notify":
            if not _text(action.get("message")) and not _text(action.get("title")):
                errors.append(f"{label}: notify requires 'message' or 'title'")
            priority = _text(action.get("priority") or "normal")
            if priority not in NOTIFY_PRIORITIES:
                warnings.append(f"{label}: notify priority '{priority}' is not one of {', '.join(NOTIFY_PRIORITIES)}")
        elif action_type == "camera_ai":
            if not _text(action.get("camera")):
                errors.append(f"{label}: camera_ai requires 'camera'")
            media_mode = _text(action.get("media_mode"))
            if media_mode and media_mode not in _CAMERA_MEDIA_MODES:
                errors.append(f"{label}: media_mode must be 'image' or 'video'")
            announce_spec = action.get("announce") if isinstance(action.get("announce"), dict) else {}
            notify_spec = action.get("notify") if isinstance(action.get("notify"), dict) else {}
            if not _text(announce_spec.get("message")) and not _text(notify_spec.get("message")) and not _text(notify_spec.get("title")):
                errors.append(f"{label}: camera_ai needs an 'announce' with 'message' and/or a 'notify' with 'message'/'title'")
            if _text(notify_spec.get("priority")) and _text(notify_spec.get("priority")) not in NOTIFY_PRIORITIES:
                warnings.append(f"{label}: camera_ai notify priority '{notify_spec.get('priority')}' is not one of {', '.join(NOTIFY_PRIORITIES)}")
        elif action_type == "device":
            if not _text(action.get("provider")) or not _text(action.get("action")):
                errors.append(f"{label}: device requires 'provider' and 'action'")
            if not _text(action.get("device")):
                warnings.append(f"{label}: device without 'device' targets every device on the provider that supports the action")
        elif action_type == "webhook":
            url = _text(action.get("url"))
            if not url:
                errors.append(f"{label}: webhook requires 'url'")
            elif not url.lower().startswith(("http://", "https://")):
                errors.append(f"{label}: webhook 'url' must start with http:// or https://")
            elif url.lower().startswith("http://"):
                warnings.append(f"{label}: webhook uses plain http — the payload travels unencrypted")
            method = _text(action.get("method")).upper()
            if method and method not in _WEBHOOK_METHODS:
                errors.append(f"{label}: webhook 'method' must be one of {', '.join(_WEBHOOK_METHODS)}")
            timeout = _as_float(action.get("timeout_seconds"), 10.0)
            if "timeout_seconds" in action and not 1.0 <= timeout <= 60.0:
                errors.append(f"{label}: webhook 'timeout_seconds' must be between 1 and 60")
            if action.get("payload") is not None and not isinstance(action.get("payload"), dict):
                errors.append(f"{label}: webhook 'payload' must be an object")
            if action.get("headers") is not None and not isinstance(action.get("headers"), dict):
                errors.append(f"{label}: webhook 'headers' must be an object")


def validate_definition(definition: Any, client: Any = None) -> Tuple[List[str], List[str]]:
    """Returns (errors, warnings). Empty errors means the definition is accepted."""
    errors: List[str] = []
    warnings: List[str] = []
    if not isinstance(definition, dict):
        return ["definition must be a JSON object"], warnings
    if not _text(definition.get("name")):
        errors.append("definition requires a 'name'")
    watch_entities = definition.get("watch_entities")
    if watch_entities is not None:
        if not isinstance(watch_entities, list) or not watch_entities or not all(_text(item) for item in watch_entities):
            errors.append("watch_entities must be a non-empty list of HA entity ids")
        elif not _bool(definition.get("builtin")):
            errors.append("watch_entities is reserved for builtin automations")
        else:
            for watched in watch_entities:
                _check_entity_known(client, watched, warnings)
    trigger = definition.get("trigger")
    if not isinstance(trigger, dict):
        errors.append("definition requires a 'trigger' object")
        trigger = {}
    trigger_type = _text(trigger.get("type"))
    if trigger_type not in TRIGGER_TYPES:
        errors.append(f"trigger.type must be one of {', '.join(TRIGGER_TYPES)}")
    else:
        if trigger_type == "interval" and _as_float(trigger.get("seconds"), 0) < 3:
            errors.append("interval trigger requires 'seconds' >= 3")
        if trigger_type == "entity_state":
            _validate_entity_spec(trigger, "trigger", errors, warnings, client)
        if trigger_type == "time" and not re.match(r"^\d{1,2}:\d{2}$", _text(trigger.get("time"))):
            errors.append("time trigger requires 'time' as HH:MM")
        if trigger_type == "protect_event" and not _text(trigger.get("event_type")):
            errors.append("protect_event trigger requires 'event_type' (e.g. 'person')")

    conditions = definition.get("conditions") or []
    if not isinstance(conditions, list):
        errors.append("conditions must be a list")
        conditions = []
    for index, condition in enumerate(conditions):
        label = f"conditions[{index}]"
        if not isinstance(condition, dict):
            errors.append(f"{label} is not an object")
            continue
        condition_type = _text(condition.get("type"))
        if condition_type not in CONDITION_TYPES:
            errors.append(f"{label}: unknown condition type '{condition_type}'")
            continue
        if condition_type == "entity_state":
            _validate_entity_spec(condition, label, errors, warnings, client)
        if condition_type == "camera_people" and not _text(condition.get("camera")):
            warnings.append(f"{label}: camera_people without 'camera' matches any Protect camera")
        if condition_type == "camera_vision":
            if not _text(condition.get("camera")):
                errors.append(f"{label}: camera_vision requires 'camera'")
            media_mode = _text(condition.get("media_mode"))
            if media_mode and media_mode not in _CAMERA_MEDIA_MODES:
                errors.append(f"{label}: media_mode must be 'image' or 'video'")
            hold = _as_float(condition.get("hold_seconds"), 0.0)
            if "hold_seconds" in condition and not (hold == 0.0 or 3.0 <= hold <= 86400.0):
                errors.append(f"{label}: camera_vision 'hold_seconds' must be 0 (one-shot) or between 3 and 86400")
            check = _as_float(condition.get("check_seconds"), 0.0)
            if "check_seconds" in condition and not (check == 0.0 or 15.0 <= check <= 3600.0):
                errors.append(f"{label}: camera_vision 'check_seconds' must be 0 (every evaluation) or between 15 and 3600")
            if "check_seconds" in condition and hold == 0.0:
                warnings.append(f"{label}: camera_vision 'check_seconds' only applies with 'hold_seconds' — it is ignored for one-shot checks")
        if condition_type == "camera_face":
            if not _text(condition.get("camera")):
                errors.append(f"{label}: camera_face requires 'camera'")
            mode = _token(condition.get("mode") or "any_person")
            if mode not in ("any_person", "named"):
                errors.append(f"{label}: camera_face mode must be 'any_person' or 'named'")
            elif mode == "named" and not _text(condition.get("person")):
                errors.append(f"{label}: camera_face named mode requires 'person'")
            lookback = _as_float(condition.get("lookback_seconds"), 0.0)
            if "lookback_seconds" in condition and lookback < 15:
                errors.append(f"{label}: camera_face 'lookback_seconds' must be >= 15")
            if mode == "named" and _text(condition.get("person")) and _camera_face_id_readiness(client).get("status") != "ready":
                warnings.append(
                    f"{label}: camera_face named mode needs Face ID — falling back to any-person behavior at run time"
                )
        if condition_type == "presence" and not _text(condition.get("tracker")):
            errors.append(f"{label}: presence requires 'tracker'")
        if condition_type == "time_window":
            if not _text(condition.get("after")) and not _text(condition.get("before")):
                errors.append(f"{label}: time_window requires 'after' and/or 'before'")

    actions = definition.get("actions") or []
    if not isinstance(actions, list):
        errors.append("actions must be a list")
        actions = []
    if not actions:
        errors.append("definition requires at least one action")
    _validate_actions(actions, allow_ask=True, path="actions", errors=errors, warnings=warnings, client=client)

    mode = _text(definition.get("mode")) or "single"
    if mode not in ("single", "parallel"):
        errors.append("mode must be 'single' or 'parallel'")
    return errors, warnings


# ---------------------------------------------------------------------------
# camera device resolution + vision condition
# ---------------------------------------------------------------------------

def _resolve_camera_device(client: Any, ref: Any) -> Optional[Dict[str, Any]]:
    """Resolve a camera reference: encoded 'provider|id', or a name/room substring match."""
    registry = _registry(client)
    device = _find_device(registry, ref)
    if device and _device_categories(device).intersection({"camera", "doorbell"}):
        return device
    wanted = _text(ref).lower()
    if not wanted:
        return None
    for candidate in registry.get("devices") or []:
        if not isinstance(candidate, dict):
            continue
        if not _device_categories(candidate).intersection({"camera", "doorbell"}):
            continue
        option = _device_option(candidate)
        haystack = " ".join(
            _text(option.get(key)) for key in ("label", "description", "value")
        ).lower()
        if wanted in haystack:
            return candidate
    return None


def _classify_vision_verdict(text: str) -> str:
    cleaned = re.sub(r"[^a-z0-9 ]+", " ", _text(text).lower()).strip()
    if not cleaned:
        return ""
    for word in _VISION_NO_WORDS:
        if re.search(rf"\b{re.escape(word)}\b", cleaned):
            return "no"
    for word in _VISION_YES_WORDS:
        if re.search(rf"\b{re.escape(word)}\b", cleaned):
            return "yes"
    return ""


def _capture_camera_description(
    client: Any,
    device: Dict[str, Any],
    media_mode: str,
    prompt: str,
    image_bytes: bytes = b"",
    image_content_type: str = "image/jpeg",
) -> Tuple[str, str, List[str]]:
    """Capture and describe camera media. Returns (description, actual_media, errors)."""
    provider = _text(device.get("integration_id"))
    device_id = _device_id(device)
    actions = set(_device_actions(device))
    snapshot_action = next(
        (action for action in ("camera_snapshot", "snapshot") if action in actions),
        "",
    )
    if not snapshot_action and _camera_supports_media_mode(device, "image"):
        snapshot_action = "camera_snapshot"
    clip_action = next(
        (action for action in ("camera_clip", "video_clip", "clip") if action in actions),
        "",
    )
    if not clip_action and _camera_supports_media_mode(device, "video"):
        clip_action = "camera_clip"

    actual_media = media_mode
    media_errors: List[str] = []
    description = ""
    if media_mode == "video":
        if not clip_action:
            media_errors.append("video: the selected camera integration does not expose video clips")
            actual_media = "image"
        else:
            try:
                clip_result = run_integration_device_action(provider, clip_action, device_id, _camera_clip_payload({}))
                video_bytes, video_content_type = _clip_result_bytes(clip_result)
                description = _describe_video_sync(video_bytes, video_content_type, prompt)
            except Exception as exc:
                media_errors.append(f"video: {exc}")
                actual_media = "image"
                logger.warning("[automation_ga] camera video failed for %s: %s", device_id, exc)
    if actual_media == "image" and not description:
        if not snapshot_action:
            media_errors.append("image: the selected camera integration does not expose snapshots")
        else:
            try:
                if not image_bytes:
                    snapshot_result = run_integration_device_action(provider, snapshot_action, device_id, {})
                    image_bytes, image_content_type = _snapshot_result_bytes(snapshot_result)
                description = _describe_snapshot_sync(image_bytes, image_content_type, prompt, client)
            except Exception as exc:
                media_errors.append(f"image: {exc}")
                logger.warning("[automation_ga] camera image failed for %s: %s", device_id, exc)
    return description, actual_media, media_errors


def _evaluate_camera_vision(client: Any, condition: Dict[str, Any]) -> Tuple[bool, str]:
    camera = _text(condition.get("camera"))
    device = _resolve_camera_device(client, camera)
    if not device:
        return False, f"camera '{camera}' not found in the integration device registry"
    prompt = _text(condition.get("prompt")) or "Is at least one person visible in this image?"
    prompt = f"Answer with a single word: YES or NO. {prompt}"
    media_mode = _token(condition.get("media_mode") or "image")
    if media_mode not in _CAMERA_MEDIA_MODES:
        media_mode = "image"
    if run_integration_device_action is None:
        return False, "integration device actions are unavailable in this Tater runtime"
    description, _actual, media_errors = _capture_camera_description(client, device, media_mode, prompt)
    if not description:
        return False, "camera_vision failed: " + "; ".join(media_errors)
    verdict = _classify_vision_verdict(description)
    expect = _bool(condition.get("expect"), True)
    satisfied = (verdict == "yes") == expect
    detail = f"camera '{_text(device.get('name')) or camera}' vision: '{description[:80]}'"
    if not verdict:
        detail += " (no YES/NO verdict)"
    elif not satisfied:
        detail += f" (expected {'YES' if expect else 'NO'})"
    return satisfied, detail


# ---------------------------------------------------------------------------
# camera_ai + device actions
# ---------------------------------------------------------------------------

def _execute_camera_ai_action(client: Any, automation: Dict[str, Any], action: Dict[str, Any]) -> List[str]:
    """Describe a camera snapshot/clip with the vision model and deliver announce/notify.

    Runs synchronously in the runner thread and may block for up to ~90s while vision
    is running — definitions that use it should keep mode 'single' in mind.
    """
    notes: List[str] = []
    if run_integration_device_action is None:
        raise RuntimeError("integration device actions are unavailable in this Tater runtime")
    device = _resolve_camera_device(client, action.get("camera"))
    if not device:
        raise ValueError(f"camera '{_text(action.get('camera'))}' not found in the integration device registry")
    provider = _text(device.get("integration_id"))
    device_id = _device_id(device)
    actions = set(_device_actions(device))
    snapshot_action = next(
        (item for item in ("camera_snapshot", "snapshot") if item in actions),
        "",
    )
    if not snapshot_action and _camera_supports_media_mode(device, "image"):
        snapshot_action = "camera_snapshot"

    context = {"device": _text(device.get("name")) or device_id, "person": "", "vision": ""}

    face_requested = _bool(action.get("face_id"), False)
    face_result: Dict[str, Any] = {
        "status": "disabled_for_automation",
        "warning": "",
        "people": [],
        "identity_ids": [],
    }
    image_bytes = b""
    image_content_type = "image/jpeg"
    if face_requested:
        readiness = _camera_face_id_readiness(client)
        face_result = {**readiness, "people": [], "identity_ids": []}
        if face_result.get("status") == "ready":
            if not snapshot_action:
                face_result = {
                    "status": "unavailable",
                    "warning": "The selected camera cannot provide a snapshot for Face ID.",
                    "people": [],
                    "identity_ids": [],
                }
            else:
                try:
                    snapshot_result = run_integration_device_action(provider, snapshot_action, device_id, {})
                    image_bytes, image_content_type = _snapshot_result_bytes(snapshot_result)
                    face_result = _recognize_camera_faces_with_timeout(
                        client,
                        image_bytes,
                        source={
                            "owner": MODULE_KEY,
                            "provider": provider,
                            "camera_target": device_id,
                            "automation_id": _text(automation.get("id")),
                        },
                    )
                except Exception as exc:
                    face_result = {
                        "status": "error",
                        "warning": _text(exc) or "Face ID snapshot capture failed.",
                        "people": [],
                        "identity_ids": [],
                    }
        context["person"] = _camera_face_people_text(face_result.get("people") or [])

    vision_prompt = _text(action.get("vision_prompt")) or (
        "Describe the scene in one or two short sentences. Do not invent details."
    )
    prompt = _render_template(vision_prompt, context)
    if face_requested:
        prompt = _camera_prompt_with_face_context(prompt, face_result)

    media_mode = _token(action.get("media_mode") or "image")
    if media_mode not in _CAMERA_MEDIA_MODES:
        media_mode = "image"
    description, actual_media, media_errors = _capture_camera_description(
        client, device, media_mode, prompt, image_bytes, image_content_type
    )
    if not description:
        actual_media = "fallback"
        description = _text(action.get("vision_fallback")) or "Camera activity was detected."
    context["vision"] = description
    notes.append(f"Camera AI ({actual_media}) for '{context['device']}': {description[:120]}")
    if media_errors:
        notes.append("camera media issues: " + "; ".join(media_errors))
    if face_result.get("warning"):
        notes.append(f"face id: {face_result['warning']}")

    announce_spec = action.get("announce") if isinstance(action.get("announce"), dict) else {}
    notify_spec = action.get("notify") if isinstance(action.get("notify"), dict) else {}
    delivered = False
    if _text(announce_spec.get("message")):
        message = _render_template(announce_spec.get("message"), context)
        targets = _resolve_announce_targets(client, announce_spec.get("targets"))
        ok, note = _announce_blocking(message, targets, _normalize_tts_audio_scene(announce_spec.get("audio_scene")))
        notes.append("Announced: " if ok else f"FAILED announce: {note}")
        delivered = delivered or ok
    if _text(notify_spec.get("message")) or _text(notify_spec.get("title")):
        message = _render_template(notify_spec.get("message") or notify_spec.get("title"), context)
        title = _render_template(notify_spec.get("title"), context) or None
        platform = _text(notify_spec.get("platform")) or "webui"
        targets = notify_spec.get("targets") if isinstance(notify_spec.get("targets"), dict) else None
        priority = _text(notify_spec.get("priority")) or "normal"
        try:
            from notify.core import dispatch_notification_sync

            dispatch_notification_sync(
                platform,
                title,
                message,
                targets=targets,
                origin={"platform": MODULE_KEY, "user": MODULE_KEY, "user_id": MODULE_KEY},
                meta={"automation": _text(automation.get("name")), "priority": priority},
            )
            notes.append(f"Sent {platform} notification (priority {priority})")
            delivered = True
        except Exception as exc:
            notes.append(f"FAILED notify: {exc}")
    if not delivered:
        raise RuntimeError("camera_ai delivered nothing (announcement failed or no destination configured)")
    return notes


def _execute_device_action(client: Any, action: Dict[str, Any]) -> List[str]:
    """Run a non-HA integration device action from the shared device registry."""
    if run_integration_device_action is None:
        raise RuntimeError("integration device actions are unavailable in this Tater runtime")
    provider = _text(action.get("provider"))
    operation = _text(action.get("action"))
    data = _json_object(action.get("data"))
    ref = _text(action.get("device"))
    registry = _registry(client)
    devices: List[Dict[str, Any]] = []
    for device in registry.get("devices") or []:
        if not isinstance(device, dict):
            continue
        if _text(device.get("integration_id")).lower() != provider.lower():
            continue
        if operation not in _device_actions(device):
            continue
        devices.append(device)
    if ref:
        found = _find_device(registry, _encode_device(provider, ref))
        if found is None:
            wanted = ref.lower()
            found = next(
                (
                    device
                    for device in devices
                    if wanted
                    in " ".join(
                        _text(device.get(key)) for key in ("name", "room", "area", "id", "ref")
                    ).lower()
                ),
                None,
            )
        if found is None:
            raise ValueError(f"device '{ref}' not found on provider '{provider}'")
        devices = [found]
    if not devices:
        raise ValueError(f"no devices on provider '{provider}' support action '{operation}'")
    errors: List[str] = []
    succeeded = 0
    for device in devices:
        try:
            result = run_integration_device_action(provider, operation, _device_id(device), data)
            if isinstance(result, dict) and result.get("ok") is False:
                errors.append(_text(result.get("error") or result.get("message")) or f"{_device_id(device)} rejected the action")
            else:
                succeeded += 1
        except Exception as exc:
            errors.append(f"{_text(device.get('name')) or _device_id(device)}: {exc}")
    if succeeded <= 0:
        raise RuntimeError(errors[0] if errors else "the device action failed")
    label = _ACTION_LABELS.get(_token(operation), operation.replace("_", " ").title())
    notes = [f"{label} sent to {succeeded} device{'s' if succeeded != 1 else ''}"]
    notes.extend(errors)
    return notes


# ---------------------------------------------------------------------------
# action execution
# ---------------------------------------------------------------------------

_WEBHOOK_METHODS = ("POST", "PUT", "PATCH", "GET")


def _http_request(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    body: Optional[bytes] = None,
    timeout_seconds: float = 10.0,
) -> Tuple[bool, int, str]:
    """One outbound HTTP call. Single transport seam: offline tests monkeypatch
    this function instead of touching the network (build machine is offline).

    Returns (transport_ok, status_code, note). Non-2xx responses raise
    HTTPError in urllib, so they arrive as (False, code, "HTTP <code> ...")."""
    import urllib.request

    req = urllib.request.Request(url, data=body, method=method)
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=max(1.0, float(timeout_seconds))) as resp:
            status = int(getattr(resp, "status", 0) or 0)
            return (True, status, f"HTTP {status}")
    except Exception as exc:
        code = getattr(exc, "code", 0)
        note = f"HTTP {code} — {exc}" if code else str(exc) or exc.__class__.__name__
        return (False, int(code or 0), note)


def _execute_webhook_action(
    rc: Any,
    automation: Dict[str, Any],
    action: Dict[str, Any],
    template_context: Optional[Dict[str, Any]] = None,
) -> List[str]:
    url = _text(action.get("url"))
    if not url:
        return ["FAILED webhook — no 'url' configured"]
    method = (_text(action.get("method")) or "POST").upper()
    if method not in _WEBHOOK_METHODS:
        method = "POST"
    headers = {key: _text(value) for key, value in _json_object(action.get("headers")).items() if _text(key)}
    context = template_context if isinstance(template_context, dict) else {}
    payload: Dict[str, Any] = _json_object(action.get("payload"))
    body: Optional[bytes] = None
    if payload and method != "GET":
        rendered = {key: _render_template_deep(value, context) for key, value in payload.items()}
        try:
            body = json.dumps(rendered).encode("utf-8")
        except Exception as exc:
            return [f"FAILED webhook — payload not serializable: {exc}"]
    timeout = min(60.0, max(1.0, _as_float(action.get("timeout_seconds"), 10.0)))
    ok, status, note = _http_request(method, url, headers=headers or None, body=body, timeout_seconds=timeout)
    success = bool(ok) and 200 <= status < 300
    label = f"{method} {url[:80]}"
    if success:
        return [f"Webhook {label} — {note}"]
    return [f"FAILED webhook {label} — {note}"]


def _execute_actions(client: Any, automation: Dict[str, Any], actions: Any, depth: int = 0, template_context: Optional[Dict[str, Any]] = None) -> List[str]:
    rc = client if client is not None else _redis()
    notes: List[str] = []
    if depth > 1:
        return ["action nesting limit reached"]
    for action in actions if isinstance(actions, list) else []:
        if not isinstance(action, dict):
            continue
        action_type = _text(action.get("type"))
        if action_type == "call_service":
            data = _json_object(action.get("data"))
            if _text(action.get("entity_id")):
                data["entity_id"] = action.get("entity_id")
            ok, note = _ha_call_service(rc, action.get("domain"), action.get("service"), data)
            entity_label = _text(action.get("entity_id"))
            notes.append(("Called " if ok else "FAILED ") + f"{_text(action.get('domain'))}.{_text(action.get('service'))}" + (f" {entity_label}" if entity_label else "") + ("" if ok else f" — {note}"))
        elif action_type == "announce":
            message, message_error = _resolve_announce_message(action, rc)
            if not message:
                notes.append(f"FAILED announce — {message_error}")
                continue
            message = _render_template(message, template_context if isinstance(template_context, dict) else {})
            targets = _resolve_announce_targets(rc, action.get("targets"))
            notes.append(f"Announced: '{message[:80]}' to {len(targets)} target(s)")
            _announce_async(message, targets, _normalize_tts_audio_scene(action.get("audio_scene")))
        elif action_type == "notify":
            try:
                from notify.core import dispatch_notification_sync

                priority = _text(action.get("priority")) or "normal"
                dispatch_notification_sync(
                    _text(action.get("platform")) or "webui",
                    _render_template(_text(action.get("title")) or None, template_context if isinstance(template_context, dict) else {}),
                    _render_template(_text(action.get("message")), template_context if isinstance(template_context, dict) else {}),
                    targets=action.get("targets") if isinstance(action.get("targets"), dict) else None,
                    origin={"platform": MODULE_KEY, "user": MODULE_KEY, "user_id": MODULE_KEY},
                    meta={"automation": _text(automation.get("name")), "priority": priority},
                )
                notes.append(f"Sent {_text(action.get('platform')) or 'webui'} notification (priority {priority})")
            except Exception as exc:
                notes.append(f"FAILED notify: {exc}")
        elif action_type == "wait":
            deadline = time.time() + min(300.0, max(0.5, _as_float(action.get("seconds"), 1.0)))
            while time.time() < deadline:
                time.sleep(0.25)
            notes.append(f"Waited {_as_float(action.get('seconds'), 1.0):.0f}s")
        elif action_type == "camera_ai":
            notes.extend(_execute_camera_ai_action(rc, automation, action))
        elif action_type == "device":
            notes.extend(_execute_device_action(rc, action))
        elif action_type == "webhook":
            notes.extend(_execute_webhook_action(rc, automation, action, template_context))
        elif action_type == "ask_yes_no":
            message = _render_template(_text(action.get("message")), template_context if isinstance(template_context, dict) else {})
            pending_id = _register_pending(rc, automation, action, question_override=message)
            targets = _resolve_announce_targets(rc, action.get("targets"))
            notes.append(f"Asked via announcement (pending response {pending_id}): '{message[:80]}' to {len(targets)} target(s)")
            _announce_async(message, targets, _normalize_tts_audio_scene(action.get("audio_scene")))
    return notes


def _execute_automation(
    client: Any,
    auto_id: str,
    definition: Dict[str, Any],
    meta: Dict[str, Any],
    trigger_detail: str,
    run_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    rc = client if client is not None else _redis()
    name = _text(definition.get("name")) or auto_id
    cooldown = max(0.0, _as_float(definition.get("cooldown_seconds"), _as_float(_setting(rc, "default_cooldown_seconds", 1800), 1800)))
    meta["last_run_at"] = time.time()
    meta["run_count"] = _as_int(meta.get("run_count"), 0) + 1
    meta["last_trigger"] = trigger_detail
    meta["busy"] = True
    meta["last_result"] = "running"
    _save_meta(rc, auto_id, meta)
    _log_activity(rc, auto_id, name, "run_started", trigger_detail)
    try:
        notes = _execute_actions(rc, definition, definition.get("actions"), template_context=run_context)
        meta["last_result"] = "; ".join(notes) if notes else "no actions executed"
        _log_activity(rc, auto_id, name, "run_finished", meta["last_result"])
    except Exception as exc:
        meta["last_result"] = f"error: {exc}"
        meta["last_error"] = str(exc)
        _log_activity(rc, auto_id, name, "run_error", str(exc))
        logger.exception("automation %s failed", auto_id)
    finally:
        pending_extension = _pending_busy_extension(rc, auto_id)
        meta["busy"] = False
        meta["cooldown_until"] = time.time() + max(cooldown, pending_extension)
        _save_meta(rc, auto_id, meta)
    return meta


def _pending_busy_extension(client: Any, auto_id: str) -> float:
    rc = client if client is not None else _redis()
    try:
        raw = rc.hgetall(PENDING_KEY) or {}
    except Exception:
        return 0.0
    extension = 0.0
    for raw_pending in raw.values():
        pending = _json_loads(raw_pending, {})
        if not isinstance(pending, dict):
            continue
        if _text(pending.get("automation_id")) != auto_id:
            continue
        remaining = max(0.0, _as_float(pending.get("deadline"), 0.0) - time.time())
        extension = max(extension, remaining + 60.0)
    return extension


# ---------------------------------------------------------------------------
# pending yes/no response windows
# ---------------------------------------------------------------------------

def _register_pending(client: Any, automation: Dict[str, Any], action: Dict[str, Any], question_override: str = "") -> str:
    rc = client if client is not None else _redis()
    settings_timeout = _as_float(_setting(rc, "default_response_timeout_seconds", 120), 120)
    timeout = min(900.0, max(15.0, _as_float(action.get("timeout_seconds"), settings_timeout)))
    pending_id = f"p_{uuid.uuid4().hex[:10]}"
    pending = {
        "id": pending_id,
        "automation_id": _text(automation.get("id")),
        "automation_name": _text(automation.get("name")),
        "question": question_override or _text(action.get("message")),
        "timeout_seconds": timeout,
        "registered_at": time.time(),
        "deadline": time.time() + timeout,
        "yes_actions": action.get("yes_actions") if isinstance(action.get("yes_actions"), list) else [],
        "no_actions": action.get("no_actions") if isinstance(action.get("no_actions"), list) else [],
        "unanswered_actions": action.get("unanswered_actions") if isinstance(action.get("unanswered_actions"), list) else [],
        "watermarks": {},
    }
    rc.hset(PENDING_KEY, pending_id, json.dumps(pending, separators=(",", ":"), default=str))
    _log_activity(rc, pending["automation_id"], pending["automation_name"], "question_asked", pending["question"])
    return pending_id


def _history_entry_text(entry: Any) -> str:
    if not isinstance(entry, dict):
        return _text(entry)
    content = entry.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict):
                parts.append(_text(item.get("text") or item.get("content")))
            else:
                parts.append(_text(item))
        return " ".join(part for part in parts if part)
    return _text(content)


def _classify_response(text: str) -> str:
    cleaned = re.sub(r"[^a-z0-9' ]+", " ", _text(text).lower()).strip()
    if not cleaned:
        return ""
    for word in NEGATIVE_WORDS:
        if re.search(rf"\b{re.escape(word)}\b", cleaned):
            return "no"
    for word in AFFIRMATIVE_WORDS:
        if word.strip() and re.search(rf"\b{re.escape(word)}\b", cleaned):
            return "yes"
    return ""


def _pending_summaries(client: Any) -> List[Dict[str, Any]]:
    rc = client if client is not None else _redis()
    try:
        raw = rc.hgetall(PENDING_KEY) or {}
    except Exception:
        return []
    rows = []
    now = time.time()
    for pending_id, raw_pending in raw.items():
        pending = _json_loads(raw_pending, {})
        if not isinstance(pending, dict):
            continue
        if _as_float(pending.get("deadline"), 0.0) > now:
            rows.append(
                {
                    "id": _text(pending.get("id")) or str(pending_id),
                    "automation": _text(pending.get("automation_name")),
                    "question": _text(pending.get("question")),
                    "seconds_left": int(_as_float(pending.get("deadline"), 0.0) - now),
                }
            )
    return rows


def _process_pending(client: Any) -> None:
    rc = client if client is not None else _redis()
    try:
        raw = rc.hgetall(PENDING_KEY) or {}
    except Exception:
        return
    now = time.time()
    conv_keys: List[str] = []
    try:
        conv_keys = list(rc.scan_iter(match="tater:voice:conv:*:history"))
    except Exception:
        conv_keys = []
    for pending_id, raw_pending in list(raw.items()):
        pending = _json_loads(raw_pending, {})
        if not isinstance(pending, dict):
            rc.hdel(PENDING_KEY, pending_id)
            continue
        deadline = _as_float(pending.get("deadline"), 0.0)
        watermarks = pending.get("watermarks") if isinstance(pending.get("watermarks"), dict) else {}
        verdict = ""
        verdict_text = ""
        newest: Dict[str, int] = {}
        for conv_key in conv_keys:
            try:
                length = int(rc.llen(conv_key) or 0)
            except Exception:
                continue
            newest[conv_key] = length
            watermark = _as_int(watermarks.get(conv_key), -1)
            if watermark < 0:
                continue
            if length <= watermark:
                continue
            try:
                entries = rc.lrange(conv_key, watermark, length - 1) or []
            except Exception:
                continue
            for raw_entry in entries:
                entry = _json_loads(raw_entry, {})
                if not isinstance(entry, dict) or _text(entry.get("role")) != "user":
                    continue
                answer = _classify_response(_history_entry_text(entry))
                if answer:
                    verdict = answer
                    verdict_text = _history_entry_text(entry)
                    break
            if verdict:
                break
        if not verdict and deadline <= now:
            verdict = "unanswered"
        if not verdict:
            pending["watermarks"] = newest
            rc.hset(PENDING_KEY, pending_id, json.dumps(pending, separators=(",", ":"), default=str))
            continue

        branch_key = f"{verdict}_actions" if verdict in ("yes", "no") else "unanswered_actions"
        automation_id = _text(pending.get("automation_id"))
        automations = _load_automations(rc)
        shell = {"name": pending.get("automation_name"), "id": automation_id}
        branch = pending.get(branch_key) if isinstance(pending.get(branch_key), list) else []
        notes = _execute_actions(rc, shell, branch) if branch else ["no actions configured for this answer"]
        _log_activity(
            rc,
            automation_id,
            _text(pending.get("automation_name")),
            f"answered_{verdict}" if verdict in ("yes", "no") else "timed_out",
            (verdict_text or "no response") + " — " + ("; ".join(notes) if notes else "no actions"),
        )
        rc.hdel(PENDING_KEY, pending_id)
        try:
            metas = _load_meta(rc)
            meta = metas.get(automation_id) or {}
            meta["last_answer"] = f"{verdict} ({verdict_text or 'timeout'})"
            _save_meta(rc, automation_id, meta)
        except Exception:
            pass
        _record_pending_outcome(rc, automation_id, verdict)


def _record_pending_outcome(rc: Any, auto_id: str, verdict: str) -> None:
    """Nuisance damping (§15.4.4): unanswered ask_yes_no windows progressively widen
    the automation's cooldown; a real answer resets the counter (cooldown stays
    until an explicit undamp via toggle or the WebUI card)."""
    if not auto_id:
        return
    try:
        metas = _load_meta(rc)
        meta = metas.get(auto_id) or {}
        automations = _load_automations(rc)
        definition = automations.get(auto_id)
        if not isinstance(definition, dict):
            _save_meta(rc, auto_id, meta)
            return
        if verdict in ("yes", "no"):
            if _as_int(meta.get("unanswered_count"), 0) > 0:
                meta["unanswered_count"] = 0
                _save_meta(rc, auto_id, meta)
            return
        count = _as_int(meta.get("unanswered_count"), 0) + 1
        meta["unanswered_count"] = count
        _save_meta(rc, auto_id, meta)
        threshold = max(1, _as_int(_setting(rc, "damping_unanswered_threshold", 3), 3))
        if count % threshold != 0:
            return
        cap = max(60.0, _as_float(_setting(rc, "damping_cooldown_max_seconds", 86400.0), 86400.0))
        settings_default = max(0.0, _as_float(_setting(rc, "default_cooldown_seconds", 1800.0), 1800.0))
        current = max(0.0, _as_float(definition.get("cooldown_seconds"), settings_default))
        widened = min(cap, max(60.0, current * 2.0))
        if widened <= current and current >= cap:
            return
        metas = _load_meta(rc)
        meta = metas.get(auto_id) or {}
        meta["pre_damp_cooldown"] = current if "pre_damp_cooldown" not in meta else meta.get("pre_damp_cooldown")
        meta["last_auto_cooldown"] = widened
        meta["unanswered_count"] = count
        _save_meta(rc, auto_id, meta)
        definition = dict(definition)
        definition["cooldown_seconds"] = widened
        rc.hset(AUTOMATIONS_KEY, auto_id, json.dumps(definition, separators=(",", ":"), default=str))
        _log_activity(rc, auto_id, _text(definition.get("name")), "damped",
                      f"unanswered {count} — cooldown widened to {widened:.0f}s")
        _notify_damping(rc, _text(definition.get("name")), count, widened)
    except Exception:
        logger.exception("damping update failed for %s", auto_id)


def _notify_damping(rc: Any, name: str, count: int, widened: float) -> None:
    try:
        from notify.core import dispatch_notification_sync

        dispatch_notification_sync(
            "webui",
            "An automation is being muted",
            f"'{name}' has gone unanswered {count} times — its cooldown was widened to "
            f"{widened:.0f}s. Answer one of its questions, or toggle it, to reset damping.",
            origin={"platform": MODULE_KEY, "user": MODULE_KEY, "user_id": MODULE_KEY},
            meta={"automation": name, "priority": "normal"},
        )
    except Exception:
        logger.debug("damping notification not delivered: %s", name)


def _undamp_automation(rc: Any, auto_id: str) -> Tuple[bool, str]:
    """Restore a damped automation: original cooldown back, counters cleared."""
    rc = rc if rc is not None else _redis()
    automations = _load_automations(rc)
    definition = automations.get(auto_id)
    if not isinstance(definition, dict):
        return False, f"No automation '{auto_id}'"
    was_damped = _as_float(_load_meta(rc).get(auto_id, {}).get("last_auto_cooldown"), 0.0) > 0
    metas = _load_meta(rc)
    meta = metas.get(auto_id) or {}
    pre = meta.pop("pre_damp_cooldown", None)
    meta.pop("last_auto_cooldown", None)
    meta["unanswered_count"] = 0
    _save_meta(rc, auto_id, meta)
    definition = dict(definition)
    if pre:
        definition["cooldown_seconds"] = _as_float(pre, 1800.0)
    else:
        definition.pop("cooldown_seconds", None)
    rc.hset(AUTOMATIONS_KEY, auto_id, json.dumps(definition, separators=(",", ":"), default=str))
    if was_damped:
        _log_activity(rc, auto_id, _text(definition.get("name")), "undamped", "")
    return True, "damping reset"


# ---------------------------------------------------------------------------
# triggers and conditions
# ---------------------------------------------------------------------------

def _hhmm_to_minutes(value: Any) -> Optional[int]:
    match = re.match(r"^(\d{1,2}):(\d{2})$", _text(value))
    if not match:
        return None
    return int(match.group(1)) * 60 + int(match.group(2))


def _time_window_open(after: Any, before: Any) -> bool:
    now_minutes = datetime.now().hour * 60 + datetime.now().minute
    after_minutes = _hhmm_to_minutes(after)
    before_minutes = _hhmm_to_minutes(before)
    if after_minutes is None and before_minutes is None:
        return True
    if after_minutes is not None and before_minutes is not None and after_minutes > before_minutes:
        return now_minutes >= after_minutes or now_minutes < before_minutes
    if after_minutes is not None and now_minutes < after_minutes:
        return False
    if before_minutes is not None and now_minutes >= before_minutes:
        return False
    return True


def _evaluate_conditions(
    client: Any,
    auto_id: str,
    definition: Dict[str, Any],
    meta: Dict[str, Any],
    state_since: Dict[str, float],
    run_context: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, str]:
    rc = client if client is not None else _redis()
    conditions = definition.get("conditions") if isinstance(definition.get("conditions"), list) else []
    for condition in conditions:
        if not isinstance(condition, dict):
            continue
        condition_type = _text(condition.get("type"))
        if condition_type == "entity_state":
            entity_id = _text(condition.get("entity"))
            satisfied, detail = _entity_state_match(rc, condition)
            if not satisfied:
                key = f"{auto_id}:{entity_id}"
                state_since.pop(key, None)
                return False, detail
            for_seconds = max(0.0, _as_float(condition.get("for_seconds"), 0.0))
            if for_seconds > 0:
                key = f"{auto_id}:{entity_id}"
                since = _as_float(state_since.get(key, 0.0), 0.0)
                if since <= 0:
                    state_since[key] = time.time()
                    return False, f"{entity_id} just became true (arming {for_seconds:.0f}s)"
                if time.time() - since < for_seconds:
                    return False, f"{entity_id} true for {time.time() - since:.0f}s of {for_seconds:.0f}s"
        elif condition_type == "camera_people":
            seen, detail = _protect_people_seen(rc, _text(condition.get("camera")), _as_float(condition.get("lookback_seconds"), 120.0))
            expect_people = _bool(condition.get("expect_people"), False)
            if expect_people and not seen:
                return False, f"no people seen — {detail}"
            if not expect_people and seen:
                return False, f"people seen — {detail}"
        elif condition_type == "camera_vision":
            hold_seconds = max(0.0, _as_float(condition.get("hold_seconds"), 0.0))
            if hold_seconds <= 0:
                passed, detail = _evaluate_camera_vision(rc, condition)
                if not passed:
                    return False, detail
                continue
            # sustained-pose arming: the verdict must hold continuously. Trackers ride the
            # same state_since map as entity arming ("<auto_id>:camera:<camera>:<prompt>";
            # ":check_ts"/":verdict" side keys cache the last verdict between check_seconds
            # windows so an armed condition snapshots at most once per window, not per tick).
            camera = _text(condition.get("camera"))
            prompt = _text(condition.get("prompt")) or "Is at least one person visible in this image?"
            key = f"{auto_id}:camera:{camera}:{prompt}"
            check_key = key + ":check_ts"
            verdict_key = key + ":verdict"
            check_seconds = max(0.0, _as_float(condition.get("check_seconds"), 60.0))
            now = time.time()
            last_check = _as_float(state_since.get(check_key), 0.0)
            if last_check <= 0 or now - last_check >= check_seconds:
                passed, detail = _evaluate_camera_vision(rc, condition)
                state_since[check_key] = now
                state_since[verdict_key] = 1.0 if passed else 0.0
                if not passed:
                    state_since.pop(key, None)
                    return False, detail
            else:
                passed = _as_float(state_since.get(verdict_key), 0.0) > 0.5
                if not passed:
                    return False, "cached camera verdict: not passed (arming continues)"
            since = _as_float(state_since.get(key), 0.0)
            if since <= 0:
                state_since[key] = now
                return False, f"camera verdict armed (holding {hold_seconds:.0f}s)"
            held = now - since
            if held < hold_seconds:
                return False, f"camera verdict held {held:.0f}s of {hold_seconds:.0f}s"
            state_since.pop(key, None)
            state_since.pop(check_key, None)
            state_since.pop(verdict_key, None)
        elif condition_type == "camera_face":
            person = _text(condition.get("person"))
            mode = _token(condition.get("mode") or "any_person")
            matched, detail, matched_name = _camera_person_check(
                rc,
                _text(condition.get("camera")),
                person if mode == "named" else "",
                lookback_seconds=_as_float(condition.get("lookback_seconds"), 60.0),
            )
            if not matched:
                return False, detail
            if matched_name and isinstance(run_context, dict):
                run_context["person"] = matched_name
        elif condition_type == "presence":
            tracker_wanted = _text(condition.get("tracker")).lower()
            state_wanted = _text(condition.get("state") or "home").lower()
            trackers = _presence_trackers()
            current = trackers.get(tracker_wanted)
            if current is None:
                return False, f"presence tracker '{tracker_wanted}' not found"
            if current != state_wanted:
                return False, f"tracker '{tracker_wanted}' is '{current}' not '{state_wanted}'"
        elif condition_type == "time_window":
            if not _time_window_open(condition.get("after"), condition.get("before")):
                return False, "outside time window"
    return True, "all conditions passed"


def _edge_trigger_due(
    client: Any,
    auto_id: str,
    trigger: Dict[str, Any],
    state_seen: Dict[str, str],
    edge_since: Dict[str, float],
) -> Tuple[bool, str]:
    """Edge-triggered entity_state: fire on a state transition (from_state -> to_state), not while a state holds.

    state_seen tracks the last observed state per (auto_id, entity) — the baseline is recorded
    silently on the first tick so a runner restart never causes a spurious fire.
    """
    rc = client if client is not None else _redis()
    entity_id = _text(trigger.get("entity"))
    from_state = _text(trigger.get("from_state"))
    to_state = _text(trigger.get("to_state"))
    key = f"{auto_id}:{entity_id}"
    if not entity_id:
        return False, ""
    row = _ha_entities(rc).get(entity_id)
    current = _text(row.get("state")) if row else ""
    if not current:
        return False, ""
    previous = _text(state_seen.get(key))
    if not previous:
        state_seen[key] = current
        return False, ""
    for_seconds = max(0.0, _as_float(trigger.get("for_seconds"), 0.0))
    if current == previous:
        since = _as_float(edge_since.get(key, 0.0), 0.0)
        if since > 0 and time.time() - since >= for_seconds:
            edge_since.pop(key, None)
            return True, f"{entity_id} held '{current}' for {for_seconds:.0f}s"
        return False, ""
    # an actual transition previous -> current happened
    state_seen[key] = current
    from_ok = not from_state or previous == from_state
    to_ok = not to_state or current == to_state
    if not (from_ok and to_ok):
        edge_since.pop(key, None)
        return False, ""
    if for_seconds <= 0:
        return True, f"{entity_id} changed '{previous}' → '{current}'"
    edge_since[key] = time.time()
    return False, ""


def _trigger_due(
    client: Any,
    auto_id: str,
    definition: Dict[str, Any],
    meta: Dict[str, Any],
    state_since: Dict[str, float],
    state_seen: Optional[Dict[str, str]] = None,
    edge_since: Optional[Dict[str, float]] = None,
) -> Tuple[bool, str]:
    rc = client if client is not None else _redis()
    trigger = definition.get("trigger") if isinstance(definition.get("trigger"), dict) else {}
    trigger_type = _text(trigger.get("type"))
    if trigger_type == "interval":
        interval = max(3.0, _as_float(trigger.get("seconds"), _poll_seconds(rc)))
        last_run = _as_float(meta.get("last_run_at"), 0.0)
        if last_run <= 0:
            return True, "first interval"
        if time.time() - last_run >= interval:
            return True, f"interval {interval:.0f}s elapsed"
        return False, ""
    if trigger_type == "entity_state":
        from_state = _text(trigger.get("from_state"))
        to_state = _text(trigger.get("to_state"))
        if from_state or to_state:
            return _edge_trigger_due(
                rc,
                auto_id,
                trigger,
                state_seen if state_seen is not None else {},
                edge_since if edge_since is not None else {},
            )
        entity_id = _text(trigger.get("entity"))
        satisfied, detail = _entity_state_match(rc, trigger)
        key = f"{auto_id}:{entity_id}"
        if not satisfied:
            state_since.pop(key, None)
            return False, ""
        for_seconds = max(0.0, _as_float(trigger.get("for_seconds"), 0.0))
        since = _as_float(state_since.get(key, 0.0), 0.0)
        if since <= 0:
            state_since[key] = time.time()
            return False, ""
        if time.time() - since >= for_seconds:
            return True, f"{entity_id} satisfied for {for_seconds:.0f}s ({detail})"
        return False, ""
    if trigger_type == "time":
        target = _hhmm_to_minutes(trigger.get("time"))
        if target is None:
            return False, ""
        now = datetime.now()
        now_minutes = now.hour * 60 + now.minute
        last_fire = _text(meta.get("last_time_fire"))
        today = now.strftime("%Y-%m-%d")
        if now_minutes >= target and last_fire != today:
            meta["last_time_fire"] = today
            return True, f"time trigger {trigger.get('time')}"
        return False, ""
    if trigger_type == "protect_event":
        camera = _text(trigger.get("camera"))
        event_type = _text(trigger.get("event_type") or "person")
        last_seq = _as_int(meta.get("last_event_seq"), 0)
        seq, detail = _protect_event_after(rc, camera, event_type, last_seq)
        if seq:
            meta["last_event_seq"] = seq
            return True, f"protect {detail}"
        return False, ""
    return False, ""


# ---------------------------------------------------------------------------
# main runner loop
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# entity journal (§15.5 — Phase 2, v1.3.0)
# ---------------------------------------------------------------------------

def _journal_scope_entities(rc: Any, automations: Dict[str, Dict[str, Any]]) -> List[str]:
    """Entities the journal diffs each tick: the automation-referenced entities
    (builtins' watch_entities included) by default, or the whole state cache
    capped alphabetically when journal_scope="all"."""
    if _token(_setting(rc, "journal_scope", "watched")) == "all":
        cap = max(1, _as_int(_setting(rc, "journal_max_entities", 150), 150))
        return sorted(_ha_entities(rc))[:cap]
    watched = _automation_entities(automations)
    return sorted(dict.fromkeys(watched))


def _journal_tick(rc: Any, automations: Dict[str, Dict[str, Any]]) -> int:
    """Diff the watched entities' states against the previous tick's journal
    cursor and append change rows (newest-first). First sighting of an entity
    seeds the cursor silently, like edge-trigger arming — no row on baseline.

    Returns the number of change rows appended. Failures never break the tick.
    """
    try:
        entities = _journal_scope_entities(rc, automations)
        if not entities:
            return 0
        cursor = rc.hgetall(JOURNAL_META_KEY) or {}
        states = _ha_entities(rc)
        now = time.time()
        rows: List[Dict[str, Any]] = []
        cursor_updates: List[Tuple[str, str]] = []
        for entity_id in entities:
            current = _text(states.get(entity_id, {}).get("state"))
            if not current:
                continue
            prev_row: Dict[str, Any] = {}
            try:
                prev_raw = cursor.get(entity_id)
                if prev_raw:
                    loaded = json.loads(prev_raw)
                    prev_row = loaded if isinstance(loaded, dict) else {}
            except Exception:
                prev_row = {}
            last_state = _text(prev_row.get("last_state"))
            last_change = _as_float(prev_row.get("last_change_ts"), 0.0)
            if not last_state or last_change <= 0.0:
                cursor_updates.append((entity_id, json.dumps({"last_state": current, "last_change_ts": now}, separators=(",", ":"))))
                continue
            if last_state == current:
                continue
            rows.append({
                "ts": round(now, 3),
                "ts_text": _now_iso(),
                "entity": entity_id,
                "from": last_state,
                "to": current,
                "held_seconds": round(max(0.0, now - last_change), 1),
            })
            cursor_updates.append((entity_id, json.dumps({"last_state": current, "last_change_ts": now}, separators=(",", ":"))))
        if cursor_updates:
            for entity_id, blob in cursor_updates:
                rc.hset(JOURNAL_META_KEY, entity_id, blob)
        if rows:
            max_rows = max(1, _as_int(_setting(rc, "max_journal_rows", 2000), 2000))
            for row in rows:
                rc.lpush(JOURNAL_KEY, json.dumps(row, separators=(",", ":"), default=str))
            if rc.llen(JOURNAL_KEY) > max_rows:
                rc.ltrim(JOURNAL_KEY, 0, max_rows - 1)
        return len(rows)
    except Exception:
        logger.exception("entity journal update failed")
        return 0


def _activity_digest(rc: Any) -> Dict[str, Any]:
    """The capped observe-side context shared by automation_capabilities and the
    v1.4 reflection pass: what changed recently, what is down, and how the
    automations have been answered."""
    digest: Dict[str, Any] = {}
    try:
        journal_rows = rc.lrange(JOURNAL_KEY, 0, 29)
        digest["recent_changes"] = [
            json.loads(raw) if isinstance(raw, str) else dict(raw) for raw in journal_rows
        ]
    except Exception:
        digest["recent_changes"] = []
    try:
        states = _ha_entities(rc)
        digest["unavailable_entities"] = sorted(
            entity_id for entity_id, row in states.items()
            if _text(row.get("state")) in ("unavailable", "unknown")
        )
    except Exception:
        digest["unavailable_entities"] = []
    try:
        cutoff = time.time() - 86400.0
        counts: Dict[str, int] = {}
        for row in _recent_events(rc, limit=600):
            if _text(row.get("provider")) != "unifi_protect":
                continue
            if _as_float(row.get("ts"), 0.0) < cutoff:
                continue
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            event_type = _text(
                payload.get("type") or payload.get("eventType") or payload.get("event_type")
            ).lower()
            if "person" not in event_type:
                continue
            counts[_text(row.get("camera")) or _text(payload.get("camera")) or "unknown"] = counts.get(
                _text(row.get("camera")) or _text(payload.get("camera")) or "unknown", 0
            ) + 1
        digest["protect_person_events_24h"] = counts
    except Exception:
        digest["protect_person_events_24h"] = {}
    try:
        metas = _load_meta(rc)
        automations = _load_automations(rc)
        stats: Dict[str, Dict[str, Any]] = {}
        for auto_id, definition in automations.items():
            meta = metas.get(auto_id) if isinstance(metas.get(auto_id), dict) else {}
            stats[_text(definition.get("name")) or auto_id] = {
                "run_count": _as_int((meta or {}).get("run_count"), 0),
                "last_answer": _text((meta or {}).get("last_answer")),
                "unanswered_count": _as_int((meta or {}).get("unanswered_count"), 0),
                "enabled": _automation_enabled(definition),
            }
        digest["automation_stats"] = stats
    except Exception:
        digest["automation_stats"] = {}
    return digest


def _tick(client: Any) -> None:
    rc = client if client is not None else _redis()
    _process_pending(rc)
    automations = _load_automations(rc)
    # entity journal (§15.5): runs before the automations early-return so
    # journal_scope="all" keeps observing even with zero automations
    _journal_tick(rc, automations)
    if not automations:
        return
    metas = _load_meta(rc)
    # restore arming trackers persisted by previous ticks
    state_since: Dict[str, float] = {}
    state_seen: Dict[str, str] = {}
    edge_since: Dict[str, float] = {}
    for meta in metas.values():
        saved = meta.get("state_since") if isinstance(meta.get("state_since"), dict) else {}
        for key, ts in saved.items():
            state_since[str(key)] = _as_float(ts, 0.0)
        seen = meta.get("state_seen") if isinstance(meta.get("state_seen"), dict) else {}
        for key, value in seen.items():
            state_seen[str(key)] = _text(value)
        edge = meta.get("edge_since") if isinstance(meta.get("edge_since"), dict) else {}
        for key, ts in edge.items():
            edge_since[str(key)] = _as_float(ts, 0.0)
    # background-refresh the possible-states catalog for entities automations watch
    _refresh_state_catalog_async(rc, _automation_entities(automations))
    for auto_id, definition in automations.items():
        meta = _meta_for(metas, auto_id)
        if not _automation_enabled(definition):
            continue
        cooldown_until = _as_float(meta.get("cooldown_until"), 0.0)
        if cooldown_until > time.time():
            continue
        if meta.get("busy"):
            continue
        due, trigger_detail = _trigger_due(rc, auto_id, definition, meta, state_since, state_seen, edge_since)
        if not due:
            continue
        run_context: Dict[str, Any] = {}
        passed, condition_detail = _evaluate_conditions(rc, auto_id, definition, meta, state_since, run_context)
        if not passed:
            continue
        _log_activity(rc, auto_id, _text(definition.get("name")), "triggered", f"{trigger_detail}; {condition_detail}")
        _execute_automation(rc, auto_id, definition, meta, trigger_detail, run_context)
    # persist arming trackers without clobbering metas freshly saved by runs this tick
    for auto_id in automations:
        own_since = {k: v for k, v in state_since.items() if k.startswith(f"{auto_id}:")}
        own_seen = {k: v for k, v in state_seen.items() if k.startswith(f"{auto_id}:")}
        own_edge = {k: v for k, v in edge_since.items() if k.startswith(f"{auto_id}:")}
        if not own_since and not own_seen and not own_edge:
            continue
        fresh = _load_meta(rc).get(auto_id) or {}
        if own_since:
            fresh.setdefault("state_since", {})
            fresh["state_since"].update(own_since)
        if own_seen:
            fresh.setdefault("state_seen", {})
            fresh["state_seen"].update(own_seen)
        if own_edge:
            fresh.setdefault("edge_since", {})
            fresh["edge_since"].update(own_edge)
        _save_meta(rc, str(auto_id), fresh)


def run(stop_event=None) -> None:
    """Core thread entry point (started by SurfaceRuntimeManager)."""
    own_event = False
    if stop_event is None:
        import threading as _threading

        stop_event = _threading.Event()
        own_event = True
    logger.info("automation ga core runner started")
    rc = _redis()
    try:
        seeded = _seed_builtins(rc)
        if seeded:
            logger.info("automation ga core seeded %d builtin automations (disabled)", len(seeded))
    except Exception:
        logger.exception("builtin seeding failed")
    while not stop_event.is_set():
        poll = _poll_seconds(rc)
        try:
            _tick(rc)
        except Exception:
            logger.exception("automation tick failed")
        if own_event:
            break
        deadline = time.time() + poll
        while not stop_event.is_set() and time.time() < deadline:
            time.sleep(min(1.0, max(0.1, deadline - time.time())))
    logger.info("automation ga core runner stopped")


# ---------------------------------------------------------------------------
# Hydra kernel tools (authoring surface for the LLM)
# ---------------------------------------------------------------------------

def _capabilities_document(client: Any = None, include_entities: bool = True) -> str:
    rc = client if client is not None else _redis()
    entities = _ha_entities(rc)
    sample_rows = []
    for entity_id, row in sorted(entities.items())[:80 if include_entities else 0]:
        friendly = row.get("friendly_name") or ""
        sample_rows.append(f"{entity_id} = {row.get('state')}" + (f" ({friendly})" if friendly else ""))
    entity_block = "(skipped: include_entities=false)" if not include_entities else (
        "\n".join(sample_rows) if sample_rows else "(no cached entities yet)")
    registry = _registry(rc)
    camera_rows = [
        f"{row.get('label')} — {row.get('value')}"
        for row in _camera_device_options(registry)[:40]
    ]
    camera_block = "\n".join(camera_rows) if camera_rows else "(no camera-capable devices in the integration registry)"
    sat_options = _announcement_options()
    sat_rows = [f"{row.get('label')} — {row.get('value')}" for row in sat_options[:30]]
    sat_block = "\n".join(sat_rows) if sat_rows else "(no announcement targets resolved)"
    notify_services = _ha_notify_services(rc)
    notify_block = "\n".join(notify_services[:40]) if notify_services else "(no HA notify services discovered)"
    return f"""AUTOMATION DEFINITION SCHEMA (Generative Agent)
An automation is a JSON object executed by the deterministic Generative Agent runner.

REQUIRED:
  "name": string
  "trigger": object — one of:
    {{"type": "interval", "seconds": 60}}
    {{"type": "entity_state", "entity": "switch.stove", "state": "on", "for_seconds": 900}}   (for_seconds: must stay true this long)
    {{"type": "entity_state", "entity": "sensor.washer", "from_state": "spinning", "to_state": "finished", "for_seconds": 0}}
        (edge trigger: fires once per state TRANSITION, not while the state holds; omit from_state = from any state;
         omit to_state = any change out of from_state; both omitted = any change. Use for 'when the washer finishes'.)
    {{"type": "entity_state", "entity": "sensor.living_temp", "attribute": "current_temperature", "match": "above", "value": 27}}
        (attribute: dotted path into HA attributes, e.g. current_temperature; match: equals | not_equals | contains | above | below;
         cannot be combined with from_state/to_state)
    {{"type": "time", "time": "07:30"}}                                                 (daily)
    {{"type": "protect_event", "camera": "great room", "event_type": "person"}}          (UniFi Protect event)
OPTIONAL:
  "conditions": list (ALL must pass) — one of:
    {{"type": "entity_state", "entity": "...", "state": "...", "for_seconds": 0}}
    {{"type": "entity_state", "entity": "...", "attribute": "current_temperature", "match": "above", "value": 27}}
    {{"type": "camera_people", "camera": "great room", "lookback_seconds": 180, "expect_people": false}}   (UniFi Protect person detection)
    {{"type": "camera_vision", "camera": "front door", "prompt": "Is at least one person visible?", "expect": true, "media_mode": "image"}}
        (snapshots/clips the camera and asks the vision model a YES/NO question;
         "hold_seconds": 900 keeps arming while the verdict passes continuously — the condition only
         passes after that many seconds of one verdict, reset by any failing/unknown check;
         "check_seconds": 60 bounds how often an armed hold re-snapshots, default 60)
    {{"type": "camera_face", "camera": "hallway", "mode": "named", "person": "Alex", "lookback_seconds": 60}}
        (mode any_person = a Protect person event seen on that camera within lookback;
         mode named = snapshot + Face ID identity match on 'person' — needs Face ID enabled,
         degrades to any-person otherwise; when named matches, {{person}} may be used in
         announce/notify/webhook text anywhere in that run)
    {{"type": "presence", "tracker": "alice phone", "state": "home"}}                                     (BLE presence)
    {{"type": "time_window", "after": "22:00", "before": "06:00"}}
  "actions": list — one of:
    {{"type": "call_service", "domain": "switch", "service": "turn_off", "entity_id": "switch.stove", "data": {{}}}}
    {{"type": "announce", "targets": "all_satellites", "message": "...",
     "audio_scene": {{"background": {{"url": "<Tater background-audio URL>", "loop": true, "volume_percent": 60}}, "ducking": {{"target_percent": 35}}}}}}
        (TTS on voice satellites / HA players, with optional background-audio scene;
         message text: use exactly one of  "message": "fixed text"  |
         "messages": ["one of several", "phrasings"] (a random entry each run)  |
         "message_style": "Good morning, the house is armed." (the base LLM writes a fresh variation of it each run — same meaning and tone, different words))
    {{"type": "ask_yes_no", "targets": "all_satellites", "message": "...", "timeout_seconds": 120,
      "yes_actions": [...], "no_actions": [...], "unanswered_actions": [...]}}           (announces, listens for a spoken yes/no)
    {{"type": "notify", "platform": "webui", "title": "...", "message": "...", "priority": "normal"}}    (priority: low | normal | high | urgent)
    {{"type": "webhook", "url": "https://example.internal/escalate", "payload": {{"automation": "{{{{"person"}}}}"}}}}
        (one outbound HTTP call: POST, PUT, PATCH or GET; payload/headers templated with {{person}}/{{vision}};
         send pushes via Home Assistant with call_service instead: domain "notify", service "notify.mobile_app_<phone>")
    {{"type": "wait", "seconds": 5}}
    {{"type": "camera_ai", "camera": "front door", "media_mode": "image", "vision_prompt": "...", "face_id": false,
     "announce": {{"message": "At the door: {{vision}} — {{person}}", "targets": "all_satellites"}},
     "notify": {{"platform": "webui", "message": "{{vision}}", "priority": "high"}}}}
        (describes a camera snapshot/clip with the vision model; {{vision}}/{{person}} template placeholders; needs announce and/or notify)
    {{"type": "device", "provider": "homeassistant", "action": "turn_off", "device": "living room lamp", "data": {{}}}}
        (non-HA integration device action from the Tater integration registry; omit 'device' to hit every compatible device)
  "mode": "single" (default — no overlap) | "parallel"
  "cooldown_seconds": minimum time between runs (default from settings)
  "enabled": true/false

RULES:
- Call automation_entity_states before writing entity_state specs: it returns the states an
  entity has actually shown (HA history), recent transitions, and fallback options — never
  invent state names for a sensor.
- camera_ai and camera_vision run the vision model and may block the runner for up to ~90s.
- camera devices are looked up in the integration registry: encoded "provider|device_id", or a name/room substring.
- ask_yes_no branches may only contain call_service/announce/notify/webhook/wait.
Validate with automation_validate, then save with automation_create.

CURRENT HOME ASSISTANT ENTITIES (entity_id = state):
{entity_block}

CAMERA DEVICES (name — provider|device_id):
{camera_block}

ANNOUNCEMENT TARGETS (label — target string for announce/ask_yes_no 'targets'):
{sat_block}

HA NOTIFY SERVICES (discovered; companion-app phone pushes are here. Send via
call_service with domain "notify" and service "<the part after notify.>"):
{notify_block}
"""


def get_hydra_kernel_tools(platform: str = "", redis_client: Any = None, core_key: str = "") -> List[Dict[str, str]]:
    rows = [
        {
            "id": "automation_capabilities",
            "description": "Returns the automation definition schema, rules, the current list of Home Assistant entity ids with states, camera-capable devices, and a digest of recent activity (entities that changed state recently, currently unavailable entities, camera person-event counts, automation outcome stats). Call this before creating or editing automations.",
            "usage": '{"function":"automation_capabilities","arguments":{"include_entities":true}}',
        },
        {
            "id": "automation_entity_states",
            "description": "Returns the states a Home Assistant entity has actually shown (from HA history), its recent state transitions, and fallback options. Call it before writing an entity_state trigger or condition so the states you pick are real.",
            "usage": '{"function":"automation_entity_states","arguments":{"entity":"sensor.washer"}}',
        },
        {
            "id": "automation_create",
            "description": "Create a new automation from a definition object. The runner starts executing it within seconds if enabled.",
            "usage": '{"function":"automation_create","arguments":{"definition":{...}}}',
        },
        {
            "id": "automation_validate",
            "description": "Validate an automation definition without saving it. Returns errors and warnings.",
            "usage": '{"function":"automation_validate","arguments":{"definition":{...}}}',
        },
        {
            "id": "automation_list",
            "description": "List all automations with their ids, status, trigger, and last run result.",
            "usage": '{"function":"automation_list","arguments":{}}',
        },
        {
            "id": "automation_get",
            "description": "Return the full definition of one automation by id.",
            "usage": '{"function":"automation_get","arguments":{"id":"a_1234abcd"}}',
        },
        {
            "id": "automation_update",
            "description": "Replace the definition of an existing automation by id (full definition required).",
            "usage": '{"function":"automation_update","arguments":{"id":"a_1234abcd","definition":{...}}}',
        },
        {
            "id": "automation_delete",
            "description": "Delete an automation by id.",
            "usage": '{"function":"automation_delete","arguments":{"id":"a_1234abcd"}}',
        },
        {
            "id": "automation_toggle",
            "description": "Enable or disable an automation by id.",
            "usage": '{"function":"automation_toggle","arguments":{"id":"a_1234abcd","enabled":true}}',
        },
        {
            "id": "automation_run",
            "description": "Trigger an automation immediately, ignoring its trigger but respecting conditions.",
            "usage": '{"function":"automation_run","arguments":{"id":"a_1234abcd"}}',
        },
        {
            "id": "automation_activity",
            "description": "Recent activity log entries (triggered/runs/answers/errors), optionally filtered by automation id.",
            "usage": '{"function":"automation_activity","arguments":{"limit":20}}',
        },
    ]
    return rows


def run_hydra_kernel_tool(
    tool_id: str = "",
    args: Optional[Dict[str, Any]] = None,
    platform: str = "",
    scope: str = "",
    origin: Optional[Dict[str, Any]] = None,
    llm_client: Any = None,
    redis_client: Any = None,
    core_key: str = "",
) -> Optional[Dict[str, Any]]:
    rc = redis_client if redis_client is not None else _redis()
    args = dict(args or {})
    tool = _text(tool_id)

    if tool == "automation_capabilities":
        include_entities = bool(args.get("include_entities", True))
        doc = _capabilities_document(rc, include_entities=include_entities)
        recent = _activity_digest(rc)
        if include_entities:
            return {"ok": True, "facts": ["Automation definition schema and recent-activity digest returned"], "data": {"schema": doc, "recent": recent}, "say_hint": "Use the schema to build the automation definition."}
        return {"ok": True, "facts": ["Automation definition schema and recent-activity digest returned; entity listing skipped — call with include_entities true for it."], "data": {"schema": doc, "recent": recent}, "say_hint": "Use the schema to build the automation definition."}

    if tool == "automation_entity_states":
        requested = [_text(args.get("entity"))] if _text(args.get("entity")) else []
        raw_list = args.get("entities")
        if isinstance(raw_list, list):
            requested.extend(_text(item) for item in raw_list)
        requested = [entity_id for entity_id in dict.fromkeys(requested) if entity_id]
        if not requested:
            return {
                "ok": False,
                "error": {"code": "missing_entity", "message": "Pass 'entity' (or 'entities') — the entity id to inspect."},
                "say_hint": "Ask which device the user means, or list entity ids with automation_capabilities.",
            }
        rows = _ha_entities(rc)
        resolved: List[str] = []
        for entity_id in requested:
            if entity_id in rows:
                resolved.append(entity_id)
                continue
            lowered = entity_id.casefold()
            matches = [
                candidate
                for candidate in rows
                if lowered in candidate.casefold()
                or _text(rows[candidate].get("friendly_name")).casefold() == lowered
            ]
            resolved.extend(matches[:3])
        resolved = [entity_id for entity_id in dict.fromkeys(resolved) if entity_id][:10]
        if not resolved:
            return {
                "ok": False,
                "error": {"code": "not_found", "message": f"None of {requested} match a Home Assistant entity with a cached state."},
                "say_hint": "List entities with automation_capabilities to find the right entity id.",
            }
        try:
            _refresh_state_catalog(rc, resolved)
        except Exception as exc:
            logger.warning("state catalog refresh failed in automation_entity_states: %s", exc)
        data: Dict[str, Dict[str, Any]] = {}
        for entity_id in resolved:
            states, source = _state_catalog_states(rc, entity_id, rows=rows)
            row = rows.get(entity_id)
            data[entity_id] = {
                "friendly_name": _text(row.get("friendly_name")) if row else "",
                "current_state": _text(row.get("state")) if row else "",
                "possible_states": states,
                "state_source": source,
                "recent_transitions": [
                    {"from": frm, "to": to, "at": _now_label(ts) if ts else ""}
                    for frm, to, ts in _state_catalog_transitions(rc, entity_id)
                ],
            }
        facts = [
            f"{entity_id}: {', '.join((data[entity_id]['possible_states'] or ['(no states seen)'])[:8])}"
            for entity_id in resolved
        ]
        return {
            "ok": True,
            "facts": facts,
            "data": {"entities": data},
            "say_hint": "Use these real states in entity_state triggers/conditions; prefer from_state/to_state for 'when it changes' automations.",
        }

    if tool == "automation_validate":
        errors, warnings = validate_definition(args.get("definition"), rc)
        if errors:
            return {"ok": False, "error": {"code": "invalid_definition", "message": "; ".join(errors)}, "facts": errors, "say_hint": "Fix the definition errors and validate again."}
        return {"ok": True, "facts": ["Definition is valid"] + warnings, "data": {"warnings": warnings}, "say_hint": "The definition validates. Offer to save it with automation_create."}

    if tool == "automation_create":
        definition = args.get("definition")
        if isinstance(definition, str):
            definition = _json_loads(definition, None)
        errors, warnings = validate_definition(definition, rc)
        if errors:
            return {"ok": False, "error": {"code": "invalid_definition", "message": "; ".join(errors)}, "facts": errors, "say_hint": "Fix the definition errors and try again."}
        auto_id = f"a_{uuid.uuid4().hex[:8]}"
        definition = dict(definition)
        definition["id"] = auto_id
        definition.setdefault("enabled", True)
        rc.hset(AUTOMATIONS_KEY, auto_id, json.dumps(definition, separators=(",", ":"), default=str))
        _log_activity(rc, auto_id, _text(definition.get("name")), "created", json.dumps(definition.get("trigger", {}), default=str))
        return {
            "ok": True,
            "facts": [f"Automation '{_text(definition.get('name'))}' created as {auto_id} and enabled"] + warnings,
            "data": {"id": auto_id, "definition": definition},
            "say_hint": "Confirm what the automation will do and mention it can be paused or deleted by asking.",
        }

    if tool == "automation_list":
        automations = _load_automations(rc)
        metas = _load_meta(rc)
        rows = []
        for auto_id, definition in automations.items():
            meta = _meta_for(metas, auto_id)
            rows.append(
                {
                    "id": auto_id,
                    "name": _text(definition.get("name")),
                    "enabled": _automation_enabled(definition),
                    "trigger": definition.get("trigger"),
                    "last_run_at": meta.get("last_run_at"),
                    "last_result": _text(meta.get("last_result")),
                }
            )
        if not rows:
            return {"ok": True, "facts": ["No automations exist yet"], "data": {"automations": []}, "say_hint": "Tell the user they can create one by describing what it should do."}
        facts = [f"{len(rows)} automation(s): " + ", ".join(f"{row['name']}{' (disabled)' if not row['enabled'] else ''}" for row in rows)]
        return {"ok": True, "facts": facts, "data": {"automations": rows}, "say_hint": "Summarize the automations and their status."}

    if tool == "automation_get":
        auto_id = _text(args.get("id"))
        definition = _load_automations(rc).get(auto_id)
        if not definition:
            return {"ok": False, "error": {"code": "not_found", "message": f"No automation '{auto_id}'"}, "say_hint": "List automations to find the right id."}
        return {"ok": True, "facts": [f"Automation {auto_id}"], "data": {"definition": definition}, "say_hint": "Describe the definition."}

    if tool == "automation_update":
        auto_id = _text(args.get("id"))
        automations = _load_automations(rc)
        if auto_id not in automations:
            return {"ok": False, "error": {"code": "not_found", "message": f"No automation '{auto_id}'"}, "say_hint": "List automations to find the right id."}
        definition = args.get("definition")
        if isinstance(definition, str):
            definition = _json_loads(definition, None)
        errors, warnings = validate_definition(definition, rc)
        if errors:
            return {"ok": False, "error": {"code": "invalid_definition", "message": "; ".join(errors)}, "facts": errors, "say_hint": "Fix the definition errors and try again."}
        existing = automations[auto_id]
        if _bool(existing.get("builtin")):
            # builtin identity is not user-editable: an update may tune the rest
            # but cannot un-builtin it or orphan its id suffix
            definition = dict(definition)
            definition["builtin"] = True
            definition["builtin_id"] = _text(existing.get("builtin_id"))
        definition = dict(definition)
        definition["id"] = auto_id
        rc.hset(AUTOMATIONS_KEY, auto_id, json.dumps(definition, separators=(",", ":"), default=str))
        _log_activity(rc, auto_id, _text(definition.get("name")), "updated", "")
        return {"ok": True, "facts": [f"Automation {auto_id} updated"] + warnings, "data": {"definition": definition}, "say_hint": "Summarize the change."}

    if tool == "automation_delete":
        auto_id = _text(args.get("id"))
        automations = _load_automations(rc)
        if auto_id not in automations:
            return {"ok": False, "error": {"code": "not_found", "message": f"No automation '{auto_id}'"}, "say_hint": "List automations to find the right id."}
        name = _text(automations[auto_id].get("name"))
        if _bool(automations[auto_id].get("builtin")):
            return {"ok": False, "error": {"code": "builtin", "message": f"'{name}' is a builtin automation — edit or disable it instead of deleting it."}, "say_hint": "Report that builtins are edited or disabled, not deleted."}
        rc.hdel(AUTOMATIONS_KEY, auto_id)
        rc.hdel(META_KEY, auto_id)
        _log_activity(rc, auto_id, name, "deleted", "")
        return {"ok": True, "facts": [f"Deleted automation '{name}' ({auto_id})"], "say_hint": "Confirm deletion."}

    if tool == "automation_toggle":
        auto_id = _text(args.get("id"))
        automations = _load_automations(rc)
        if auto_id not in automations:
            return {"ok": False, "error": {"code": "not_found", "message": f"No automation '{auto_id}'"}, "say_hint": "List automations to find the right id."}
        enabled = args.get("enabled")
        if enabled is None:
            enabled = not _automation_enabled(automations[auto_id])
        if bool(enabled):
            # toggling on clears any auto-widened cooldown (damping reset)
            _undamp_automation(rc, auto_id)
            automations = _load_automations(rc)
        definition = dict(automations[auto_id])
        definition["enabled"] = bool(enabled)
        rc.hset(AUTOMATIONS_KEY, auto_id, json.dumps(definition, separators=(",", ":"), default=str))
        state_word = "enabled" if _automation_enabled(definition) else "disabled"
        _log_activity(rc, auto_id, _text(definition.get("name")), state_word, "")
        return {"ok": True, "facts": [f"Automation '{_text(definition.get('name'))}' {state_word}"], "say_hint": "Confirm the new state."}

    if tool == "automation_run":
        auto_id = _text(args.get("id"))
        automations = _load_automations(rc)
        if auto_id not in automations:
            return {"ok": False, "error": {"code": "not_found", "message": f"No automation '{auto_id}'"}, "say_hint": "List automations to find the right id."}
        definition = automations[auto_id]
        metas = _load_meta(rc)
        meta = _meta_for(metas, auto_id)
        state_since: Dict[str, float] = {}
        passed, detail = _evaluate_conditions(rc, auto_id, definition, meta, state_since, {})
        if not passed:
            return {"ok": False, "error": {"code": "conditions_not_met", "message": detail}, "say_hint": f"The automation did not run: {detail}."}
        _execute_automation(rc, auto_id, definition, meta, "manual run", {})
        return {"ok": True, "facts": [f"Automation '{_text(definition.get('name'))}' ran: conditions passed ({detail})"], "say_hint": "Report what the automation did."}

    if tool == "automation_activity":
        limit = min(100, max(1, _as_int(args.get("limit"), 20)))
        rows = _read_activity(rc, _text(args.get("id")), limit)
        facts = [f"{len(rows)} activity entries"] if rows else ["No activity yet"]
        return {"ok": True, "facts": facts, "data": {"activity": rows}, "say_hint": "Summarize the recent activity."}

    return None


# ---------------------------------------------------------------------------
# system prompt fragments
# ---------------------------------------------------------------------------

def get_hydra_system_prompt_fragments(
    platform: str = "",
    scope: str = "",
    origin: Optional[Dict[str, Any]] = None,
    redis_client: Any = None,
    memory_context: Any = None,
    role: str = "",
    core_key: str = "",
) -> List[str]:
    fragments: List[str] = []
    fragments.append(
        "Generative Agent Automation Core: you can create, inspect, and manage home automations "
        "through the automation_* kernel tools. Before creating or editing an automation, call "
        "automation_capabilities to load the definition schema and current entity ids, and "
        "validate definitions with automation_validate before saving with automation_create. "
        "Automations run deterministically on a schedule/trigger — they do not need the user to ask again. "
        "Camera automations (camera_vision conditions, camera_ai actions) take a few extra seconds to run."
    )
    try:
        rc = redis_client if redis_client is not None else _redis()
        pending = _pending_summaries(rc)
    except Exception:
        pending = []
    if pending:
        first = pending[0]
        fragments.append(
            f"ACTIVE AUTOMATION QUESTION: an automation asked '{first['question']}' and is waiting for a spoken "
            f"answer (about {first['seconds_left']}s left). If the user's message answers it (yes/no), acknowledge "
            "briefly — the Generative Agent Automation Core executes the matching action itself."
        )
    return fragments


# ---------------------------------------------------------------------------
# form editor: parsing
# ---------------------------------------------------------------------------

def _payload_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    values = payload.get("values")
    return values if isinstance(values, dict) else {}


def _value(values: Dict[str, Any], payload: Dict[str, Any], key: str, default: Any = "") -> Any:
    value = values[key] if key in values else payload.get(key, default)
    if isinstance(value, dict):
        for inner in ("value", "id", "key", "target"):
            if inner in value:
                return value[inner]
    return value


_TTS_AUDIO_FORM_KEYS = (
    "tts_audio_enabled",
    "tts_background_audio_source",
    "tts_background_audio_upload",
    "tts_background_audio_existing_url",
    "tts_background_audio_url",
    "tts_background_volume_percent",
    "tts_start_delay_seconds",
    "tts_ducking_target_percent",
    "tts_ducking_attack_ms",
    "tts_ducking_release_ms",
    "tts_background_fade_ms",
    "tts_background_loop",
)


def _tts_audio_scene_from_form(
    values: Dict[str, Any],
    payload: Dict[str, Any],
    *,
    existing: Any = None,
) -> Dict[str, Any]:
    current = _normalize_tts_audio_scene(existing)
    current_background = (
        current.get("background") if isinstance(current.get("background"), dict) else {}
    )
    current_foreground = (
        current.get("foreground") if isinstance(current.get("foreground"), dict) else {}
    )
    current_ducking = current.get("ducking") if isinstance(current.get("ducking"), dict) else {}
    current_finish = current.get("finish") if isinstance(current.get("finish"), dict) else {}
    audio_enabled = _bool(
        _value(values, payload, "tts_audio_enabled", bool(current_background.get("url"))),
        bool(current_background.get("url")),
    )
    if not audio_enabled:
        return {}

    existing_url = _text(
        _value(values, payload, "tts_background_audio_existing_url", current_background.get("url"))
    )
    custom_url = _text(_value(values, payload, "tts_background_audio_url"))
    uploaded_audio = _value(values, payload, "tts_background_audio_upload")
    source = _text(_value(values, payload, "tts_background_audio_source")).lower()
    if not source:
        if isinstance(uploaded_audio, dict) and uploaded_audio.get("data_b64"):
            source = "upload"
        elif custom_url:
            source = "custom"
        elif existing_url:
            source = _background_audio_source_from_url(existing_url)
        else:
            source = "preset:morning_glow"

    if source.startswith("preset:"):
        background_url = _background_audio_preset_url(source.split(":", 1)[1])
    elif source == "upload":
        if isinstance(uploaded_audio, dict) and uploaded_audio.get("data_b64"):
            background_url = _store_background_audio_upload(uploaded_audio)
        elif existing_url and "/api/ai-tasks/background-audio/uploads/" in existing_url:
            background_url = existing_url
        else:
            raise ValueError("Choose a background audio file to upload.")
    elif source == "custom":
        background_url = custom_url
        if not background_url and existing_url and _background_audio_source_from_url(existing_url) == "custom":
            background_url = existing_url
        if not background_url.lower().startswith(("http://", "https://")):
            raise ValueError("Background Audio URL must start with http:// or https://.")
    else:
        raise ValueError("Choose a valid background audio source.")

    return _normalize_tts_audio_scene(
        {
            "background": {
                "url": background_url,
                "loop": _bool(
                    _value(values, payload, "tts_background_loop", current_background.get("loop", True)),
                    True,
                ),
                "volume_percent": _int(
                    _value(
                        values,
                        payload,
                        "tts_background_volume_percent",
                        current_background.get("volume_percent"),
                    ),
                    60,
                    maximum=100,
                ),
            },
            "foreground": {
                "start_delay_ms": _seconds_to_milliseconds(
                    _value(
                        values,
                        payload,
                        "tts_start_delay_seconds",
                        _int(current_foreground.get("start_delay_ms"), 0, maximum=30000)
                        / 1000.0,
                    )
                ),
            },
            "ducking": {
                "target_percent": _int(
                    _value(
                        values,
                        payload,
                        "tts_ducking_target_percent",
                        current_ducking.get("target_percent"),
                    ),
                    35,
                    maximum=100,
                ),
                "attack_ms": _int(
                    _value(values, payload, "tts_ducking_attack_ms", current_ducking.get("attack_ms")),
                    150,
                    maximum=10000,
                ),
                "release_ms": _int(
                    _value(
                        values,
                        payload,
                        "tts_ducking_release_ms",
                        current_ducking.get("release_ms"),
                    ),
                    350,
                    maximum=10000,
                ),
            },
            "finish": {
                "fade_ms": _int(
                    _value(values, payload, "tts_background_fade_ms", current_finish.get("fade_ms")),
                    500,
                    maximum=10000,
                ),
            },
        }
    )


def _announcement_audio_fields(scene: Any, show_tts: Dict[str, Any]) -> List[Dict[str, Any]]:
    normalized = _normalize_tts_audio_scene(scene)
    background = normalized.get("background") if isinstance(normalized.get("background"), dict) else {}
    foreground = normalized.get("foreground") if isinstance(normalized.get("foreground"), dict) else {}
    ducking = normalized.get("ducking") if isinstance(normalized.get("ducking"), dict) else {}
    finish = normalized.get("finish") if isinstance(normalized.get("finish"), dict) else {}
    background_url = _text(background.get("url"))
    source = _background_audio_source_from_url(background_url) if background_url else "preset:morning_glow"
    show_for_audio = [show_tts, {"source_key": "tts_audio_enabled", "equals": "enabled"}]
    source_options = [
        {
            "value": f"preset:{preset['id']}",
            "label": preset["label"],
            "description": preset["description"],
            "icon": "♪",
        }
        for preset in _BACKGROUND_AUDIO_PRESETS
    ]
    source_options.extend(
        [
            {
                "value": "upload",
                "label": "Upload Audio",
                "description": "Use your own WAV, MP3, or FLAC file.",
                "icon": "↑",
            },
            {
                "value": "custom",
                "label": "Audio URL",
                "description": "Use a stable HTTP(S) audio URL.",
                "icon": "⌁",
            },
        ]
    )
    return [
        {
            "key": "tts_audio_enabled",
            "label": "Background Audio",
            "type": "select",
            "presentation": "cards",
            "options": [
                {
                    "value": "disabled",
                    "label": "No Background Audio",
                    "description": "Play the announcement by itself.",
                    "icon": "○",
                },
                {
                    "value": "enabled",
                    "label": "Play Background Audio",
                    "description": "Mix a looping audio bed underneath TTS on compatible Tater satellites.",
                    "icon": "♪",
                },
            ],
            "value": "enabled" if background_url else "disabled",
            "show_when": show_tts,
            "full_width": True,
        },
        {
            "key": "tts_background_audio_source",
            "label": "Background Track",
            "type": "select",
            "presentation": "cards",
            "options": source_options,
            "value": source,
            "show_when_all": show_for_audio,
            "full_width": True,
        },
        {
            "key": "tts_background_audio_upload",
            "label": "Upload Background Audio",
            "type": "file",
            "accept": ".wav,.mp3,.flac,audio/wav,audio/mpeg,audio/flac",
            "file_encoding": "base64",
            "max_bytes": _BACKGROUND_AUDIO_MAX_UPLOAD_BYTES,
            "description": "WAV, MP3, or FLAC up to 16 MB. Tater stores it in Agent Lab.",
            "value": "",
            "show_when_all": [
                *show_for_audio,
                {"source_key": "tts_background_audio_source", "equals": "upload"},
            ],
            "full_width": True,
        },
        {
            "key": "tts_background_audio_existing_url",
            "label": "Existing Background Audio URL",
            "type": "hidden",
            "value": background_url,
        },
        {
            "key": "tts_background_audio_url",
            "label": "Background Audio URL",
            "type": "text",
            "description": "A stable HTTP(S) URL for WAV, MP3, or FLAC audio.",
            "value": background_url if source == "custom" else "",
            "show_when_all": [
                *show_for_audio,
                {"source_key": "tts_background_audio_source", "equals": "custom"},
            ],
            "full_width": True,
        },
        {
            "key": "tts_background_volume_percent",
            "label": "Background Volume (%)",
            "type": "number",
            "min": 0,
            "max": 100,
            "value": _int(background.get("volume_percent"), 60, maximum=100),
            "show_when_all": show_for_audio,
        },
        {
            "key": "tts_start_delay_seconds",
            "label": "Music Lead-In Before Speech",
            "type": "range",
            "min": 0,
            "max": 30,
            "step": 0.25,
            "suffix": " s",
            "description": "How long the background music plays before Tater begins speaking.",
            "value": round(
                _int(foreground.get("start_delay_ms"), 0, maximum=30000) / 1000.0,
                2,
            ),
            "show_when_all": show_for_audio,
            "full_width": True,
        },
        {
            "key": "tts_ducking_target_percent",
            "label": "Volume During Speech (%)",
            "type": "number",
            "min": 0,
            "max": 100,
            "description": "Percentage of the background volume retained while Tater is speaking.",
            "value": _int(ducking.get("target_percent"), 35, maximum=100),
            "show_when_all": show_for_audio,
        },
        {
            "key": "tts_ducking_attack_ms",
            "label": "Duck Attack (ms)",
            "type": "number",
            "min": 0,
            "max": 10000,
            "value": _int(ducking.get("attack_ms"), 150, maximum=10000),
            "show_when_all": show_for_audio,
        },
        {
            "key": "tts_ducking_release_ms",
            "label": "Duck Release (ms)",
            "type": "number",
            "min": 0,
            "max": 10000,
            "value": _int(ducking.get("release_ms"), 350, maximum=10000),
            "show_when_all": show_for_audio,
        },
        {
            "key": "tts_background_fade_ms",
            "label": "Final Fade-Out (ms)",
            "type": "number",
            "min": 0,
            "max": 10000,
            "value": _int(finish.get("fade_ms"), 500, maximum=10000),
            "show_when_all": show_for_audio,
        },
        {
            "key": "tts_background_loop",
            "label": "Track Playback",
            "type": "select",
            "presentation": "cards",
            "options": [
                {
                    "value": "enabled",
                    "label": "Loop Until Finished",
                    "description": "Repeat the track until the announcement ends.",
                    "icon": "↻",
                },
                {
                    "value": "disabled",
                    "label": "Play Once",
                    "description": "Do not restart the track if it ends first.",
                    "icon": "▶",
                },
            ],
            "value": "enabled" if _bool(background.get("loop"), True) else "disabled",
            "show_when_all": show_for_audio,
            "full_width": True,
        },
    ]


# ---------------------------------------------------------------------------
# form editor: definition <-> form mapping
# ---------------------------------------------------------------------------

def _definition_form_supported(definition: Any) -> bool:
    """True when the form editor can round-trip this definition without losing capability."""
    if not isinstance(definition, dict):
        return False
    trigger = definition.get("trigger")
    if isinstance(trigger, dict) and _text(trigger.get("type")) == "entity_state":
        # edge triggers with attribute matching exceed the form's single-action subset
        if (_text(trigger.get("from_state")) or _text(trigger.get("to_state"))) and _text(trigger.get("attribute")):
            return False
    actions = definition.get("actions")
    if not isinstance(actions, list) or len(actions) != 1:
        return False
    action = actions[0]
    if not isinstance(action, dict):
        return False
    action_type = _token(action.get("type"))
    if action_type == "wait":
        return False
    if action_type == "ask_yes_no":
        yes_actions = action.get("yes_actions") if isinstance(action.get("yes_actions"), list) else []
        if len(yes_actions) != 1:
            return False
        yes = yes_actions[0]
        if not isinstance(yes, dict) or _token(yes.get("type")) not in ("call_service", "announce"):
            return False
        if _token(yes.get("type")) == "announce" and _announce_message_mode(yes) != "fixed":
            return False
        for branch in ("no_actions", "unanswered_actions"):
            branch_actions = action.get(branch)
            if isinstance(branch_actions, list) and branch_actions:
                return False
        return True
    if action_type not in ("call_service", "announce", "notify", "camera_ai", "device"):
        return False
    conditions = definition.get("conditions")
    if not isinstance(conditions, list) or len(conditions) > _FORM_MAX_CONDITIONS:
        return False
    for condition in conditions:
        if not isinstance(condition, dict):
            return False
        if _token(condition.get("type")) not in CONDITION_TYPES:
            return False
    return True


def _blank_definition() -> Dict[str, Any]:
    return {
        "name": "",
        "enabled": True,
        "mode": "single",
        "cooldown_seconds": 1800,
        "trigger": {"type": "entity_state"},
        "conditions": [],
        "actions": [{"type": "announce"}],
    }


def _announce_message_mode(action: Dict[str, Any]) -> str:
    """Form mode for an announce action's message source."""
    if isinstance(action, dict) and isinstance(action.get("messages"), list) and any(_text(item) for item in action.get("messages")):
        return "random_list"
    if isinstance(action, dict) and _text(action.get("message_style")):
        return "llm_style"
    return "fixed"


def _previous_announce_scenes(definition: Dict[str, Any]) -> Tuple[Any, Any]:
    """Returns (announce audio_scene, camera_ai announce audio_scene) from a previous definition."""
    announce_scene = None
    camera_scene = None
    for prior in definition.get("actions") if isinstance(definition.get("actions"), list) else []:
        if not isinstance(prior, dict):
            continue
        prior_type = _token(prior.get("type"))
        if prior_type == "announce":
            announce_scene = prior.get("audio_scene")
        elif prior_type == "camera_ai":
            announce_spec = prior.get("announce") if isinstance(prior.get("announce"), dict) else {}
            camera_scene = announce_spec.get("audio_scene")
    return announce_scene, camera_scene


def _definition_from_form(
    values: Dict[str, Any],
    payload: Dict[str, Any],
    existing: Optional[Dict[str, Any]] = None,
    client: Any = None,
) -> Dict[str, Any]:
    """Build a validated automation definition from form values. Raises ValueError on bad input."""
    previous = existing if isinstance(existing, dict) else {}
    rc = client if client is not None else _redis()

    def v(key: str, default: Any = "") -> Any:
        return _value(values, payload, key, default)

    name = _text(v("name"))
    if not name:
        raise ValueError("Give the automation a name.")
    enabled = _bool(v("enabled", "enabled"), True)
    mode = _token(v("mode", "single")) or "single"
    if mode not in ("single", "parallel"):
        mode = "single"
    cooldown = _int(v("cooldown_seconds", 1800), 1800, minimum=0, maximum=86400)

    trigger_type = _token(v("trigger_type", "entity_state")) or "entity_state"
    if trigger_type == "interval":
        trigger: Dict[str, Any] = {"type": "interval", "seconds": _int(v("trigger_seconds", 60), 60, minimum=3, maximum=86400)}
    elif trigger_type == "entity_state":
        entity_id = _text(v("trigger_entity"))
        if entity_id == _FORM_CUSTOM_SENTINEL:
            entity_id = _text(v("trigger_entity_custom"))
        if not entity_id:
            raise ValueError("Choose the device to watch, or pick 'Custom entity ID…' and type an entity id.")
        trigger = {"type": "entity_state", "entity": entity_id}
        trigger_mode = _token(v("trigger_mode", "hold")) or "hold"
        if trigger_mode == "attribute":
            attribute = _text(v("trigger_attribute"))
            if not attribute:
                raise ValueError("Enter the attribute to match, e.g. current_temperature.")
            match = _token(v("trigger_match", "equals")) or "equals"
            if match not in ENTITY_MATCH_OPS:
                match = "equals"
            value = _text(v("trigger_value"))
            if not value:
                raise ValueError("Enter the value to compare the attribute against.")
            trigger["attribute"] = attribute
            trigger["match"] = match
            trigger["value"] = value
        elif trigger_mode == "change":
            from_state = _text(v("trigger_from_state"))
            to_state = _text(v("trigger_to_state"))
            if from_state == _FORM_CUSTOM_SENTINEL:
                from_state = ""
            if to_state == _FORM_CUSTOM_SENTINEL:
                to_state = ""
            if from_state:
                trigger["from_state"] = from_state
            if to_state:
                trigger["to_state"] = to_state
            edge_for_seconds = _int(v("trigger_edge_for_seconds", 0), 0, minimum=0, maximum=86400)
            if edge_for_seconds:
                trigger["for_seconds"] = edge_for_seconds
        else:
            state = _text(v("trigger_state"))
            if state == _FORM_CUSTOM_SENTINEL:
                state = _text(v("trigger_state_custom"))
            if not state:
                raise ValueError("Choose the state that should trigger, e.g. on or off.")
            trigger["state"] = state
            for_seconds = _int(v("trigger_for_seconds", 0), 0, minimum=0, maximum=86400)
            if for_seconds:
                trigger["for_seconds"] = for_seconds
    elif trigger_type == "time":
        if _hhmm_to_minutes(v("trigger_time")) is None:
            raise ValueError("Enter the time in 24-hour format, e.g. 07:30.")
        trigger = {"type": "time", "time": _text(v("trigger_time"))}
    elif trigger_type == "protect_event":
        trigger = {
            "type": "protect_event",
            "camera": _text(v("trigger_camera")),
            "event_type": _text(v("trigger_event_type", "person")) or "person",
        }
    else:
        raise ValueError("Choose a trigger type.")

    conditions: List[Dict[str, Any]] = []
    for index in (1, 2, 3):
        prefix = f"condition{index}_"
        condition_type = _token(v(f"{prefix}type", "none")) or "none"
        if condition_type == "entity_state":
            entity_id = _text(v(f"{prefix}entity"))
            if entity_id == _FORM_CUSTOM_SENTINEL:
                entity_id = _text(v(f"{prefix}entity_custom"))
            if not entity_id:
                raise ValueError(f"Condition {index}: choose the device to check.")
            condition = {"type": "entity_state", "entity": entity_id}
            attribute = _text(v(f"{prefix}attribute"))
            if attribute:
                match = _token(v(f"{prefix}match", "equals")) or "equals"
                value = _text(v(f"{prefix}value"))
                if not value:
                    raise ValueError(f"Condition {index}: enter the value to compare the attribute against.")
                condition["attribute"] = attribute
                condition["match"] = match
                condition["value"] = value
            else:
                state = _text(v(f"{prefix}state"))
                if state == _FORM_CUSTOM_SENTINEL:
                    state = _text(v(f"{prefix}state_custom"))
                if not state:
                    raise ValueError(f"Condition {index}: choose the state to require.")
                condition["state"] = state
            for_seconds = _int(v(f"{prefix}for_seconds", 0), 0, minimum=0, maximum=86400)
            if for_seconds:
                condition["for_seconds"] = for_seconds
            conditions.append(condition)
        elif condition_type == "camera_people":
            conditions.append(
                {
                    "type": "camera_people",
                    "camera": _text(v(f"{prefix}camera")),
                    "lookback_seconds": _int(v(f"{prefix}lookback", 180), 180, minimum=5, maximum=3600),
                    "expect_people": _bool(v(f"{prefix}expect_people"), False),
                }
            )
        elif condition_type == "camera_vision":
            conditions.append(
                {
                    "type": "camera_vision",
                    "camera": _text(v(f"{prefix}camera_vision_camera")),
                    "prompt": _text(v(f"{prefix}prompt")) or "Is at least one person visible in this image?",
                    "expect": _bool(v(f"{prefix}expect", "true"), True),
                    "media_mode": _token(v(f"{prefix}media_mode", "image")) or "image",
                }
            )
        elif condition_type == "presence":
            tracker = _text(v(f"{prefix}tracker"))
            if not tracker:
                raise ValueError(f"Condition {index}: enter the presence tracker name.")
            conditions.append(
                {
                    "type": "presence",
                    "tracker": tracker,
                    "state": _text(v(f"{prefix}state_presence", "home")) or "home",
                }
            )
        elif condition_type == "time_window":
            after = _text(v(f"{prefix}after"))
            before = _text(v(f"{prefix}before"))
            if _hhmm_to_minutes(after) is None and _hhmm_to_minutes(before) is None:
                raise ValueError(f"Condition {index}: enter a time like 22:00.")
            condition = {"type": "time_window"}
            if after:
                condition["after"] = after
            if before:
                condition["before"] = before
            conditions.append(condition)
        # "none" (or unknown) → skip the slot

    action_type = _token(v("action_type", "announce")) or "announce"
    previous_announce_scene, previous_camera_scene = _previous_announce_scenes(previous)

    if action_type == "announce":
        action: Dict[str, Any] = {
            "type": "announce",
            "targets": _list(v("action_announce_targets")),
        }
        message_mode = _token(v("action_announce_message_mode", "fixed")) or "fixed"
        if message_mode == "random_list":
            raw_lines = v("action_announce_messages")
            lines = raw_lines.splitlines() if isinstance(raw_lines, str) else [str(item) for item in (raw_lines if isinstance(raw_lines, list) else [])]
            options = [line.strip() for line in lines if line.strip()]
            if not options:
                raise ValueError("Enter at least one announcement line (one per line).")
            action["messages"] = options
        elif message_mode == "llm_style":
            style = _text(v("action_announce_message_style"))
            if not style:
                raise ValueError("Enter the reference message (or its style) for the base LLM.")
            action["message_style"] = style
        else:
            message = _text(v("action_announce_message"))
            if not message:
                raise ValueError("Enter the announcement text.")
            action["message"] = message
        scene = _tts_audio_scene_from_form(values, payload, existing=previous_announce_scene)
        if scene:
            action["audio_scene"] = scene
    elif action_type == "call_service":
        domain = _text(v("action_domain"))
        service = _text(v("action_service"))
        if not domain or not service:
            raise ValueError("Enter the Home Assistant domain and service, e.g. switch / turn_off.")
        action = {"type": "call_service", "domain": domain, "service": service}
        entity_id = _text(v("action_entity_id"))
        if entity_id:
            action["entity_id"] = entity_id
        data = _json_object(v("action_data_json"))
        if data:
            action["data"] = data
    elif action_type == "device":
        provider = _text(v("action_device_provider"))
        operation = _text(v("action_operation"))
        if not provider or not operation:
            raise ValueError("Choose the integration and the device action.")
        action = {"type": "device", "provider": provider, "action": operation}
        device_ref = _text(v("action_device"))
        if device_ref:
            action["device"] = device_ref
        data = _json_object(v("action_device_data_json"))
        if data:
            action["data"] = data
    elif action_type == "camera_ai":
        camera = _text(v("action_camera"))
        if not camera:
            raise ValueError("Choose the camera to describe.")
        action = {
            "type": "camera_ai",
            "camera": camera,
            "media_mode": _token(v("action_media_mode", "image")) or "image",
        }
        vision_prompt = _text(v("action_vision_prompt"))
        if vision_prompt:
            action["vision_prompt"] = vision_prompt
        if _bool(v("action_face_id", "disabled"), False):
            action["face_id"] = True
        vision_fallback = _text(v("action_vision_fallback"))
        if vision_fallback:
            action["vision_fallback"] = vision_fallback
        announce_message = _text(v("action_camera_announce_message"))
        if announce_message:
            action["announce"] = {
                "message": announce_message,
                "targets": _list(v("action_camera_announce_targets")),
            }
            if isinstance(previous_camera_scene, dict) and previous_camera_scene:
                action["announce"]["audio_scene"] = previous_camera_scene
        if _bool(v("action_camera_notify", "disabled"), False):
            destination = _decode_notification_target(v("action_camera_notify_destination"))
            if not destination:
                raise ValueError("Choose the notification destination for the camera result.")
            message = _text(v("action_camera_notify_message"))
            if not message:
                raise ValueError("Enter the notification message — {vision} inserts the camera description.")
            notify_spec: Dict[str, Any] = {"platform": destination["platform"], "message": message}
            title = _text(v("action_camera_notify_title"))
            if title:
                notify_spec["title"] = title
            if destination.get("targets"):
                notify_spec["targets"] = destination["targets"]
            priority = _token(v("action_camera_notify_priority", "normal")) or "normal"
            if priority in NOTIFY_PRIORITIES and priority != "normal":
                notify_spec["priority"] = priority
            action["notify"] = notify_spec
        if not action.get("announce") and not action.get("notify"):
            raise ValueError("Give the camera result somewhere to go: an announcement message and/or a notification.")
    elif action_type == "notify":
        message = _text(v("action_notify_message"))
        title = _text(v("action_notify_title"))
        if not message and not title:
            raise ValueError("Enter the notification message or a title.")
        action = {"type": "notify"}
        destination = _decode_notification_target(v("action_notify_destination"))
        if destination:
            action["platform"] = destination["platform"]
            if destination.get("targets"):
                action["targets"] = destination["targets"]
        if title:
            action["title"] = title
        if message:
            action["message"] = message
        priority = _token(v("action_notify_priority", "normal")) or "normal"
        if priority in NOTIFY_PRIORITIES and priority != "normal":
            action["priority"] = priority
    elif action_type == "ask_yes_no":
        message = _text(v("action_ask_message"))
        if not message:
            raise ValueError("Enter the question Tater should ask out loud.")
        timeout = _int(v("action_ask_timeout", 120), 120, minimum=15, maximum=900)
        yes_type = _token(v("action_yes_type", "announce")) or "announce"
        if yes_type == "call_service":
            domain = _text(v("action_yes_domain"))
            service = _text(v("action_yes_service"))
            if not domain or not service:
                raise ValueError("Enter the service to call on 'yes', e.g. switch / turn_off.")
            yes_action: Dict[str, Any] = {"type": "call_service", "domain": domain, "service": service}
            entity_id = _text(v("action_yes_entity_id"))
            if entity_id:
                yes_action["entity_id"] = entity_id
            data = _json_object(v("action_yes_data_json"))
            if data:
                yes_action["data"] = data
        elif yes_type == "announce":
            yes_message = _text(v("action_yes_message"))
            if not yes_message:
                raise ValueError("Enter the confirmation announcement for 'yes'.")
            yes_action = {
                "type": "announce",
                "message": yes_message,
                "targets": _list(v("action_yes_targets")),
            }
        else:
            raise ValueError("Choose what happens when Tater hears 'yes'.")
        action = {
            "type": "ask_yes_no",
            "message": message,
            "targets": _list(v("action_ask_targets")),
            "timeout_seconds": timeout,
            "yes_actions": [yes_action],
        }
    else:
        raise ValueError("Choose an action type.")

    definition: Dict[str, Any] = {
        "name": name,
        "enabled": enabled,
        "mode": mode,
        "cooldown_seconds": cooldown,
        "trigger": trigger,
        "conditions": conditions,
        "actions": [action],
    }
    if previous:
        definition["id"] = _text(previous.get("id"))
    errors, _warnings = validate_definition(definition, rc)
    if errors:
        raise ValueError(" ".join(errors))
    return definition


# ---------------------------------------------------------------------------
# form editor: field rendering
# ---------------------------------------------------------------------------

def _provider_options(registry: Dict[str, Any]) -> List[Dict[str, Any]]:
    options: List[Dict[str, Any]] = []
    seen: set[str] = set()
    integrations = registry.get("integrations") if isinstance(registry.get("integrations"), dict) else {}
    for device in registry.get("devices") or []:
        if not isinstance(device, dict):
            continue
        provider = _token(device.get("integration_id"))
        if not provider or provider in seen:
            continue
        seen.add(provider)
        info = integrations.get(provider) if isinstance(integrations.get(provider), dict) else {}
        label = _text(info.get("name") or info.get("label")) or provider.replace("_", " ").title()
        options.append({"value": provider, "label": label})
    options.sort(key=lambda row: (_text(row.get("label")).casefold(), _text(row.get("value"))))
    return options


def _provider_device_options(registry: Dict[str, Any], provider: Any) -> List[Dict[str, Any]]:
    wanted = _token(provider)
    options: List[Dict[str, Any]] = []
    for device in registry.get("devices") or []:
        if not isinstance(device, dict) or _token(device.get("integration_id")) != wanted:
            continue
        option = _device_option(device)
        if option.get("value"):
            options.append(option)
    options.sort(key=lambda row: (_text(row.get("label")).casefold(), _text(row.get("value"))))
    return options


def _provider_action_options(registry: Dict[str, Any], provider: Any) -> List[Dict[str, Any]]:
    wanted = _token(provider)
    operations: Dict[str, Dict[str, str]] = {}
    for device in registry.get("devices") or []:
        if not isinstance(device, dict) or _token(device.get("integration_id")) != wanted:
            continue
        for operation in _device_actions(device):
            token_operation = _token(operation)
            if not token_operation or token_operation in operations:
                continue
            operations[token_operation] = {
                "value": operation,
                "label": _ACTION_LABELS.get(token_operation, operation.replace("_", " ").title()),
            }
    return [operations[key] for key in sorted(operations)]


def _match_options() -> List[Dict[str, str]]:
    return [{"value": op, "label": op.replace("_", " ").title()} for op in ENTITY_MATCH_OPS]


def _editor_fields(
    definition: Dict[str, Any],
    registry: Dict[str, Any],
    client: Any = None,
    *,
    announcement_catalog: Optional[List[Dict[str, Any]]] = None,
    notification_catalog: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    trigger = definition.get("trigger") if isinstance(definition.get("trigger"), dict) else {}
    trigger_type = _token(trigger.get("type")) or "entity_state"
    actions = definition.get("actions") if isinstance(definition.get("actions"), list) else []
    action = actions[0] if actions and isinstance(actions[0], dict) else {}
    action_type = _token(action.get("type")) or "announce"
    announce_scene = action.get("audio_scene") if _token(action.get("type")) == "announce" else None
    camera_announce = action.get("announce") if isinstance(action.get("announce"), dict) else {}
    camera_notify = action.get("notify") if isinstance(action.get("notify"), dict) else {}
    yes_actions = action.get("yes_actions") if isinstance(action.get("yes_actions"), list) else []
    yes_action = yes_actions[0] if yes_actions and isinstance(yes_actions[0], dict) else {}
    yes_type = _token(yes_action.get("type")) or "announce"
    announcement_catalog = list(announcement_catalog or [])
    notification_catalog = list(notification_catalog or [])
    camera_options = [{"value": "", "label": "Choose a camera…"}] + _camera_device_options(registry)
    media_options, media_dependency = _camera_media_mode_dependency(
        registry,
        source_key="action_camera",
        current_device=action.get("camera") if action_type == "camera_ai" else "",
        current_mode=action.get("media_mode") if action_type == "camera_ai" else "image",
    )
    show = lambda key, equals: {"show_when": {"source_key": key, "equals": equals}}  # noqa: E731
    show_any = lambda key, options: {"show_when": {"source_key": key, "any_of": list(options)}}  # noqa: E731

    entity_rows = _ha_entities(client)
    watched_entities = _definition_watch_entities(definition)
    entity_options = _ha_entity_options(client, rows=entity_rows, current_values=watched_entities)
    dep_entity_ids = [row.get("value", "") for row in entity_options]
    if len(dep_entity_ids) > 400:
        # keep the payload bounded on very large HA instances: narrow state options
        # to the entities this definition actually watches
        dep_entity_ids = watched_entities
    ensure_map: Dict[str, List[str]] = {}
    spec_list: List[Any] = [trigger]
    raw_conditions = definition.get("conditions")
    if isinstance(raw_conditions, list):
        spec_list.extend(spec for spec in raw_conditions if isinstance(spec, dict))
    for spec in spec_list:
        if not isinstance(spec, dict) or _text(spec.get("type")) != "entity_state":
            continue
        values = ensure_map.setdefault(_text(spec.get("entity")), [])
        for key in ("state", "from_state", "to_state"):
            token = _text(spec.get(key))
            if token and token not in values:
                values.append(token)
    dep_any_base = _entity_state_dependency(
        client,
        dep_entity_ids,
        source_key="",
        empty_label="Any state",
        fallback_default=[{"value": "", "label": "Any state"}],
        ensure=ensure_map,
        rows=entity_rows,
    )
    dep_state_base = _entity_state_dependency(
        client,
        dep_entity_ids,
        source_key="",
        fallback_default=[{"value": _FORM_CUSTOM_SENTINEL, "label": "Type a state…"}],
        ensure=ensure_map,
        rows=entity_rows,
    )
    trigger_mode = (
        "attribute"
        if _text(trigger.get("attribute"))
        else ("change" if (_text(trigger.get("from_state")) or _text(trigger.get("to_state"))) else "hold")
    )
    activity_hint = _entity_activity_hint(client, _text(trigger.get("entity")), rows=entity_rows)

    fields: List[Dict[str, Any]] = [
        {"key": "heading_basics", "label": "1. Basics", "type": "section_heading"},
        {
            "key": "name",
            "label": "Name",
            "type": "text",
            "value": _text(definition.get("name")),
            "full_width": True,
            "description": "A short name, e.g. 'Stove off check'.",
        },
        {
            "key": "enabled",
            "label": "State",
            "type": "select",
            "presentation": "cards",
            "value": "enabled" if _automation_enabled(definition) else "disabled",
            "options": [
                {"value": "enabled", "label": "Enabled", "icon": "✓"},
                {"value": "disabled", "label": "Disabled", "icon": "○"},
            ],
        },
        {
            "key": "mode",
            "label": "Overlap",
            "type": "select",
            "value": _token(definition.get("mode")) or "single",
            "options": [
                {"value": "single", "label": "One at a time (single)"},
                {"value": "parallel", "label": "Allow overlap (parallel)"},
            ],
            "description": "Single waits for the previous run to finish before starting again.",
        },
        {
            "key": "cooldown_seconds",
            "label": "Cooldown (seconds)",
            "type": "number",
            "min": 0,
            "max": 86400,
            "value": _int(definition.get("cooldown_seconds"), 1800),
            "description": "Minimum time between runs (extended while a question is pending).",
        },
        {"key": "heading_trigger", "label": "2. Trigger", "type": "section_heading"},
        {
            "key": "trigger_type",
            "label": "When should this automation run?",
            "type": "select",
            "presentation": "cards",
            "full_width": True,
            "value": trigger_type,
            "options": [
                {"value": "entity_state", "label": "Entity state", "description": "A Home Assistant entity reaches a state or attribute value.", "icon": "◧"},
                {"value": "interval", "label": "Interval", "description": "Repeat every N seconds.", "icon": "↻"},
                {"value": "time", "label": "Time of day", "description": "Once a day at a set time.", "icon": "◷"},
                {"value": "protect_event", "label": "Protect event", "description": "A UniFi Protect camera detects something.", "icon": "⏺"},
            ],
        },
        {
            "key": "trigger_seconds",
            "label": "Every (seconds)",
            "type": "number",
            "min": 3,
            "max": 86400,
            "value": _int(trigger.get("seconds"), 60),
            **show("trigger_type", "interval"),
        },
        {
            "key": "trigger_entity",
            "label": "Device",
            "type": "select",
            "options": [{"value": "", "label": "Choose a device…"}] + entity_options + [{"value": _FORM_CUSTOM_SENTINEL, "label": "Custom entity ID…"}],
            "value": _text(trigger.get("entity")),
            "full_width": True,
            **show("trigger_type", "entity_state"),
        },
        {
            "key": "trigger_entity_custom",
            "label": "Entity ID",
            "type": "text",
            "value": "",
            "placeholder": "sensor.washer",
            **show("trigger_entity", _FORM_CUSTOM_SENTINEL),
        },
        {
            "key": "trigger_mode",
            "label": "What should happen?",
            "type": "select",
            "presentation": "cards",
            "full_width": True,
            "value": trigger_mode,
            "options": [
                {"value": "change", "label": "It changes", "description": "Fire once when the state changes (from → to).", "icon": "⇄"},
                {"value": "hold", "label": "It has a state", "description": "Fire while the state matches.", "icon": "◼"},
                {"value": "attribute", "label": "Advanced", "description": "Match an attribute like temperature or brightness.", "icon": "⚙"},
            ],
            "description": activity_hint or "States come from this device's history — pick one, or choose 'Custom entity ID…' for anything else.",
            **show("trigger_type", "entity_state"),
        },
        {
            "key": "trigger_from_state",
            "label": "From state",
            "type": "select",
            "options": [],
            "dependent_options": dict(dep_any_base, source_key="trigger_entity"),
            "value": _text(trigger.get("from_state")),
            **show_any("trigger_mode", ("change",)),
        },
        {
            "key": "trigger_to_state",
            "label": "To state",
            "type": "select",
            "options": [],
            "dependent_options": dict(dep_any_base, source_key="trigger_entity"),
            "value": _text(trigger.get("to_state")),
            "description": "Both 'Any state' = fire on every change",
            **show_any("trigger_mode", ("change",)),
        },
        {
            "key": "trigger_edge_for_seconds",
            "label": "Hold the new state for (seconds)",
            "type": "number",
            "min": 0,
            "max": 86400,
            "value": _int(trigger.get("for_seconds"), 0) if trigger_mode == "change" else 0,
            "description": "0 = fire immediately on the change",
            **show_any("trigger_mode", ("change",)),
        },
        {
            "key": "trigger_state",
            "label": "State",
            "type": "select",
            "options": [],
            "dependent_options": dict(dep_state_base, source_key="trigger_entity"),
            "value": _text(trigger.get("state")),
            **show_any("trigger_mode", ("hold",)),
        },
        {
            "key": "trigger_state_custom",
            "label": "State (type it)",
            "type": "text",
            "value": "",
            **show("trigger_state", _FORM_CUSTOM_SENTINEL),
        },
        {
            "key": "trigger_for_seconds",
            "label": "Must hold for (seconds)",
            "type": "number",
            "min": 0,
            "max": 86400,
            "value": _int(trigger.get("for_seconds"), 0) if trigger_mode == "hold" else 0,
            "description": "0 = run as soon as it becomes true",
            **show_any("trigger_mode", ("hold",)),
        },
        {
            "key": "trigger_attribute",
            "label": "Attribute",
            "type": "text",
            "value": _text(trigger.get("attribute")),
            "description": "Attribute path to check instead of the state, e.g. current_temperature",
            **show_any("trigger_mode", ("attribute",)),
        },
        {
            "key": "trigger_match",
            "label": "Attribute match",
            "type": "select",
            "value": _token(trigger.get("match")) or "equals",
            "options": _match_options(),
            **show_any("trigger_mode", ("attribute",)),
        },
        {
            "key": "trigger_value",
            "label": "Value",
            "type": "text",
            "value": _text(trigger.get("value")),
            "description": "State or attribute value to compare against",
            **show_any("trigger_mode", ("attribute",)),
        },
        {
            "key": "trigger_time",
            "label": "Time (24h)",
            "type": "text",
            "value": _text(trigger.get("time")),
            "description": "e.g. 07:30",
            **show("trigger_type", "time"),
        },
        {
            "key": "trigger_camera",
            "label": "Camera",
            "type": "select",
            "options": camera_options,
            "value": _text(trigger.get("camera")),
            "full_width": True,
            "description": "Leave blank to match any Protect camera",
            **show("trigger_type", "protect_event"),
        },
        {
            "key": "trigger_event_type",
            "label": "Event type",
            "type": "select",
            "value": _token(trigger.get("event_type")) or "person",
            "options": [
                {"value": "person", "label": "Person"},
                {"value": "motion", "label": "Motion"},
                {"value": "vehicle", "label": "Vehicle"},
                {"value": "package", "label": "Package"},
            ],
            **show("trigger_type", "protect_event"),
        },
        {"key": "heading_conditions", "label": "3. Conditions (all must pass)", "type": "section_heading"},
    ]

    conditions = definition.get("conditions") if isinstance(definition.get("conditions"), list) else []
    for index in (1, 2, 3):
        raw = conditions[index - 1] if 0 <= index - 1 < len(conditions) and isinstance(conditions[index - 1], dict) else {}
        prefix = f"condition{index}_"
        gate = lambda equals, _prefix=prefix: {"show_when": {"source_key": f"{_prefix}type", "equals": equals}}  # noqa: E731
        fields.append(
            {
                "key": f"{prefix}type",
                "label": f"Condition {index}",
                "type": "select",
                "full_width": True,
                "value": _token(raw.get("type")) or "none",
                "options": [
                    {"value": "none", "label": "No condition"},
                    {"value": "entity_state", "label": "Entity state"},
                    {"value": "camera_people", "label": "People on camera"},
                    {"value": "camera_vision", "label": "Camera vision question"},
                    {"value": "presence", "label": "Presence"},
                    {"value": "time_window", "label": "Time window"},
                ],
            }
        )
        fields.extend(
            [
                {
                    "key": f"{prefix}entity",
                    "label": "Device",
                    "type": "select",
                    "options": [{"value": "", "label": "Choose a device…"}] + entity_options + [{"value": _FORM_CUSTOM_SENTINEL, "label": "Custom entity ID…"}],
                    "value": _text(raw.get("entity")),
                    "full_width": True,
                    **gate("entity_state"),
                },
                {
                    "key": f"{prefix}entity_custom",
                    "label": "Entity ID",
                    "type": "text",
                    "value": "",
                    **show(f"{prefix}entity", _FORM_CUSTOM_SENTINEL),
                },
                {
                    "key": f"{prefix}state",
                    "label": "State",
                    "type": "select",
                    "options": [],
                    "dependent_options": dict(dep_state_base, source_key=f"{prefix}entity"),
                    "value": _text(raw.get("state")),
                    **gate("entity_state"),
                },
                {
                    "key": f"{prefix}state_custom",
                    "label": "State (type it)",
                    "type": "text",
                    "value": "",
                    **show(f"{prefix}state", _FORM_CUSTOM_SENTINEL),
                },
                {
                    "key": f"{prefix}attribute",
                    "label": "Attribute (optional)",
                    "type": "text",
                    "value": _text(raw.get("attribute")),
                    **gate("entity_state"),
                },
                {
                    "key": f"{prefix}match",
                    "label": "Attribute match",
                    "type": "select",
                    "value": _token(raw.get("match")) or "equals",
                    "options": _match_options(),
                    **gate("entity_state"),
                },
                {
                    "key": f"{prefix}value",
                    "label": "Value",
                    "type": "text",
                    "value": _text(raw.get("value")),
                    **gate("entity_state"),
                },
                {
                    "key": f"{prefix}for_seconds",
                    "label": "Must hold for (seconds)",
                    "type": "number",
                    "min": 0,
                    "max": 86400,
                    "value": _int(raw.get("for_seconds"), 0),
                    **gate("entity_state"),
                },
                {
                    "key": f"{prefix}camera",
                    "label": "Camera",
                    "type": "select",
                    "options": camera_options,
                    "value": _text(raw.get("camera")),
                    "full_width": True,
                    **gate("camera_people"),
                },
                {
                    "key": f"{prefix}lookback",
                    "label": "Look back (seconds)",
                    "type": "number",
                    "min": 5,
                    "max": 3600,
                    "value": _int(raw.get("lookback_seconds"), 180),
                    **gate("camera_people"),
                },
                {
                    "key": f"{prefix}expect_people",
                    "label": "People required?",
                    "type": "select",
                    "presentation": "cards",
                    "value": "true" if _bool(raw.get("expect_people"), False) else "false",
                    "options": [
                        {"value": "true", "label": "People present", "icon": "●"},
                        {"value": "false", "label": "Nobody present", "icon": "○"},
                    ],
                    **gate("camera_people"),
                },
                {
                    "key": f"{prefix}camera_vision_camera",
                    "label": "Camera",
                    "type": "select",
                    "options": camera_options,
                    "value": _text(raw.get("camera")),
                    "full_width": True,
                    **gate("camera_vision"),
                },
                {
                    "key": f"{prefix}prompt",
                    "label": "Vision question",
                    "type": "textarea",
                    "value": _text(raw.get("prompt")),
                    "full_width": True,
                    "description": "The camera is asked this question and must answer YES or NO.",
                    **gate("camera_vision"),
                },
                {
                    "key": f"{prefix}expect",
                    "label": "Expected answer",
                    "type": "select",
                    "presentation": "cards",
                    "value": "true" if _bool(raw.get("expect"), True) else "false",
                    "options": [
                        {"value": "true", "label": "YES", "icon": "✓"},
                        {"value": "false", "label": "NO", "icon": "✗"},
                    ],
                    **gate("camera_vision"),
                },
                {
                    "key": f"{prefix}media_mode",
                    "label": "Media",
                    "type": "select",
                    "value": _token(raw.get("media_mode")) or "image",
                    "options": [
                        {"value": "image", "label": "Snapshot (image)"},
                        {"value": "video", "label": "Video clip"},
                    ],
                    **gate("camera_vision"),
                },
                {
                    "key": f"{prefix}tracker",
                    "label": "Tracker name",
                    "type": "text",
                    "value": _text(raw.get("tracker")),
                    **gate("presence"),
                },
                {
                    "key": f"{prefix}state_presence",
                    "label": "State",
                    "type": "text",
                    "value": _text(raw.get("state")) or "home",
                    "description": "e.g. home",
                    **gate("presence"),
                },
                {
                    "key": f"{prefix}after",
                    "label": "After (24h)",
                    "type": "text",
                    "value": _text(raw.get("after")),
                    "description": "e.g. 22:00",
                    **gate("time_window"),
                },
                {
                    "key": f"{prefix}before",
                    "label": "Before (24h)",
                    "type": "text",
                    "value": _text(raw.get("before")),
                    "description": "e.g. 06:00",
                    **gate("time_window"),
                },
            ]
        )

    fields.append(
        {"key": "heading_action", "label": "4. Action", "type": "section_heading"}
    )
    fields.append(
        {
            "key": "action_type",
            "label": "What should it do?",
            "type": "select",
            "presentation": "cards",
            "full_width": True,
            "value": action_type,
            "options": [
                {"value": "announce", "label": "Announce", "description": "Speak a message on satellites / speakers.", "icon": "♪"},
                {"value": "call_service", "label": "HA service", "description": "Call a Home Assistant service directly.", "icon": "⌂"},
                {"value": "device", "label": "Device action", "description": "Control any Tater integration device.", "icon": "⚙"},
                {"value": "camera_ai", "label": "Camera AI", "description": "Describe the camera scene with the vision model and deliver it.", "icon": "📷"},
                {"value": "notify", "label": "Notify", "description": "Send a notification (web, Discord, ntfy, …).", "icon": "◉"},
                {"value": "ask_yes_no", "label": "Ask yes/no", "description": "Ask a spoken question and act on the answer.", "icon": "?"},
            ],
        }
    )
    show_action = lambda equals: {"source_key": "action_type", "equals": equals}  # noqa: E731

    fields.extend(
        [
            {
                "key": "action_announce_message_mode",
                "label": "Announcement text",
                "type": "select",
                "presentation": "cards",
                "full_width": True,
                "value": _announce_message_mode(action) if action_type == "announce" else "fixed",
                "options": [
                    {"value": "fixed", "label": "Fixed text", "icon": "✎", "description": "Say this exact text every run."},
                    {"value": "random_list", "label": "Random from list", "icon": "🎲", "description": "One line of your list is picked at random each run."},
                    {"value": "llm_style", "label": "LLM writes fresh text", "icon": "✨", "description": "The base LLM writes a fresh variation of your text each run."},
                ],
                "show_when": show_action("announce"),
            },
            {
                "key": "action_announce_message",
                "label": "Announcement",
                "type": "textarea",
                "value": _text(action.get("message")) if action_type == "announce" else "",
                "full_width": True,
                "show_when_all": [
                    show_action("announce"),
                    {"source_key": "action_announce_message_mode", "equals": "fixed"},
                ],
            },
            {
                "key": "action_announce_messages",
                "label": "Announcement lines (one per line)",
                "type": "textarea",
                "value": "\n".join(_text(item) for item in _list(action.get("messages")) if _text(item)) if action_type == "announce" else "",
                "full_width": True,
                "description": "A different line is picked at random each run.",
                "show_when_all": [
                    show_action("announce"),
                    {"source_key": "action_announce_message_mode", "equals": "random_list"},
                ],
            },
            {
                "key": "action_announce_message_style",
                "label": "Reference for fresh text",
                "type": "textarea",
                "value": _text(action.get("message_style")) if action_type == "announce" else "",
                "full_width": True,
                "description": "Write the announcement you want (or its style, e.g. 'a short, cheerful good-morning greeting'). Each run the base LLM writes a fresh variation — same meaning and tone, different words.",
                "show_when_all": [
                    show_action("announce"),
                    {"source_key": "action_announce_message_mode", "equals": "llm_style"},
                ],
            },
            {
                "key": "action_announce_targets",
                "label": "Speakers",
                "type": "multiselect",
                "options": announcement_catalog,
                "value": _list(action.get("targets")) if action_type == "announce" else [],
                "show_when": show_action("announce"),
                "full_width": True,
            },
            *_announcement_audio_fields(announce_scene, show_action("announce")),
            {
                "key": "action_domain",
                "label": "Domain",
                "type": "text",
                "value": _text(action.get("domain")) if action_type == "call_service" else "",
                "description": "e.g. switch",
                **show("action_type", "call_service"),
            },
            {
                "key": "action_service",
                "label": "Service",
                "type": "text",
                "value": _text(action.get("service")) if action_type == "call_service" else "",
                "description": "e.g. turn_off",
                **show("action_type", "call_service"),
            },
            {
                "key": "action_entity_id",
                "label": "Entity ID (optional)",
                "type": "text",
                "value": _text(action.get("entity_id")) if action_type == "call_service" else "",
                "full_width": True,
                **show("action_type", "call_service"),
            },
            {
                "key": "action_data_json",
                "label": "Data (JSON)",
                "type": "textarea",
                "value": json.dumps(action.get("data"), separators=(",", ":")) if action_type in ("call_service", "device") and action.get("data") else "",
                "full_width": True,
                "description": "Optional extra service data as JSON, e.g. {\"transition\": 2}",
                **show("action_type", "call_service"),
            },
        ]
    )

    provider_value = _token(action.get("provider")) if action_type == "device" else ""
    operation_value = _token(action.get("action")) if action_type == "device" else ""
    device_by_provider: Dict[str, List[Dict[str, Any]]] = {}
    operation_by_provider: Dict[str, List[Dict[str, Any]]] = {}
    for provider_row in _provider_options(registry):
        provider_key = _text(provider_row.get("value"))
        device_rows = _provider_device_options(registry, provider_key)
        device_by_provider[provider_key] = (
            [{"value": "", "label": "Any compatible device"}] + device_rows
        )
        operation_rows = _provider_action_options(registry, provider_key)
        if operation_rows:
            operation_by_provider[provider_key] = operation_rows
    default_provider_rows = device_by_provider.get(provider_value, [])
    default_operation_rows = operation_by_provider.get(provider_value, [])

    fields.extend(
        [
            {
                "key": "action_device_provider",
                "label": "Integration",
                "type": "select",
                "options": _provider_options(registry),
                "value": provider_value,
                "full_width": True,
                **show("action_type", "device"),
            },
            {
                "key": "action_device",
                "label": "Device",
                "type": "select",
                "options": default_provider_rows,
                "value": _text(action.get("device")) if action_type == "device" else "",
                "full_width": True,
                "dependent_options": {
                    "source_key": "action_device_provider",
                    "options_by_source": device_by_provider,
                    "default_options": [],
                },
                **show("action_type", "device"),
            },
            {
                "key": "action_operation",
                "label": "Action",
                "type": "select",
                "options": default_operation_rows,
                "value": operation_value,
                "full_width": True,
                "dependent_options": {
                    "source_key": "action_device_provider",
                    "options_by_source": operation_by_provider,
                    "default_options": [],
                },
                **show("action_type", "device"),
            },
            {
                "key": "action_device_data_json",
                "label": "Data (JSON)",
                "type": "textarea",
                "value": json.dumps(action.get("data"), separators=(",", ":")) if action_type == "device" and action.get("data") else "",
                "full_width": True,
                "description": "Optional action payload as JSON",
                **show("action_type", "device"),
            },
        ]
    )

    fields.extend(
        [
            {
                "key": "action_camera",
                "label": "Camera",
                "type": "select",
                "options": camera_options,
                "value": _text(action.get("camera")) if action_type == "camera_ai" else "",
                "full_width": True,
                **show("action_type", "camera_ai"),
            },
            {
                "key": "action_media_mode",
                "label": "Media",
                "type": "select",
                "options": media_options,
                "value": _token(action.get("media_mode")) if action_type == "camera_ai" else "image",
                "dependent_options": media_dependency,
                **show("action_type", "camera_ai"),
            },
            {
                "key": "action_vision_prompt",
                "label": "Vision prompt",
                "type": "textarea",
                "value": _text(action.get("vision_prompt")) if action_type == "camera_ai" else "",
                "full_width": True,
                "description": "What the vision model should describe. Blank uses a generic scene description.",
                **show("action_type", "camera_ai"),
            },
            {
                "key": "action_face_id",
                "label": "Face ID",
                "type": "select",
                "presentation": "cards",
                "value": "enabled" if _bool(action.get("face_id"), False) else "disabled",
                "options": [
                    {"value": "enabled", "label": "Recognize people", "description": "Add known Face ID matches to the prompt and result.", "icon": "👤"},
                    {"value": "disabled", "label": "No Face ID", "icon": "○"},
                ],
                **show("action_type", "camera_ai"),
            },
            {
                "key": "action_vision_fallback",
                "label": "Fallback text",
                "type": "text",
                "value": _text(action.get("vision_fallback")) if action_type == "camera_ai" else "",
                "full_width": True,
                "description": "Used when the vision model returns nothing",
                **show("action_type", "camera_ai"),
            },
            {
                "key": "action_camera_announce_message",
                "label": "Announce result",
                "type": "textarea",
                "value": _text(camera_announce.get("message")) if action_type == "camera_ai" else "",
                "full_width": True,
                "description": "Leave blank to skip the announcement. {vision} = description, {person} = recognized people",
                **show("action_type", "camera_ai"),
            },
            {
                "key": "action_camera_announce_targets",
                "label": "Speakers",
                "type": "multiselect",
                "options": announcement_catalog,
                "value": _list(camera_announce.get("targets")) if action_type == "camera_ai" else [],
                "show_when": show_action("camera_ai"),
                "full_width": True,
            },
            {
                "key": "action_camera_notify",
                "label": "Also send a notification?",
                "type": "select",
                "presentation": "cards",
                "value": "enabled" if camera_notify else "disabled",
                "options": [
                    {"value": "enabled", "label": "Yes, notify", "icon": "◉"},
                    {"value": "disabled", "label": "No notification", "icon": "○"},
                ],
                **show("action_type", "camera_ai"),
            },
        ]
    )
    camera_notify_gate = [
        show_action("camera_ai"),
        {"source_key": "action_camera_notify", "equals": "enabled"},
    ]
    fields.extend(
        [
            {
                "key": "action_camera_notify_destination",
                "label": "Destination",
                "type": "select",
                "options": notification_catalog,
                "value": _encode_notification_target(camera_notify.get("platform"), camera_notify.get("targets")) if camera_notify else "",
                "full_width": True,
                "show_when_all": camera_notify_gate,
            },
            {
                "key": "action_camera_notify_title",
                "label": "Title",
                "type": "text",
                "value": _text(camera_notify.get("title")) if camera_notify else "",
                "show_when_all": camera_notify_gate,
            },
            {
                "key": "action_camera_notify_message",
                "label": "Message",
                "type": "textarea",
                "value": _text(camera_notify.get("message")) if camera_notify else "",
                "full_width": True,
                "description": "{vision} = description, {person} = recognized people",
                "show_when_all": camera_notify_gate,
            },
            {
                "key": "action_camera_notify_priority",
                "label": "Priority",
                "type": "select",
                "value": _token(camera_notify.get("priority")) if camera_notify and camera_notify.get("priority") else "normal",
                "options": [{"value": p, "label": p.title()} for p in NOTIFY_PRIORITIES],
                "show_when_all": camera_notify_gate,
            },
        ]
    )

    fields.extend(
        [
            {
                "key": "action_notify_destination",
                "label": "Destination",
                "type": "select",
                "options": notification_catalog,
                "value": _encode_notification_target(action.get("platform"), action.get("targets")) if action_type == "notify" else "",
                "full_width": True,
                "description": "Blank uses the webui defaults",
                **show("action_type", "notify"),
            },
            {
                "key": "action_notify_title",
                "label": "Title",
                "type": "text",
                "value": _text(action.get("title")) if action_type == "notify" else "",
                **show("action_type", "notify"),
            },
            {
                "key": "action_notify_message",
                "label": "Message",
                "type": "textarea",
                "value": _text(action.get("message")) if action_type == "notify" else "",
                "full_width": True,
                **show("action_type", "notify"),
            },
            {
                "key": "action_notify_priority",
                "label": "Priority",
                "type": "select",
                "value": _token(action.get("priority")) if action_type == "notify" and action.get("priority") else "normal",
                "options": [{"value": p, "label": p.title()} for p in NOTIFY_PRIORITIES],
                **show("action_type", "notify"),
            },
        ]
    )

    ask_gate = show_action("ask_yes_no")
    yes_gate_service = [ask_gate, {"source_key": "action_yes_type", "equals": "call_service"}]
    yes_gate_announce = [ask_gate, {"source_key": "action_yes_type", "equals": "announce"}]
    fields.extend(
        [
            {
                "key": "action_ask_message",
                "label": "Question",
                "type": "textarea",
                "value": _text(action.get("message")) if action_type == "ask_yes_no" else "",
                "full_width": True,
                "show_when": ask_gate,
            },
            {
                "key": "action_ask_targets",
                "label": "Speakers",
                "type": "multiselect",
                "options": announcement_catalog,
                "value": _list(action.get("targets")) if action_type == "ask_yes_no" else [],
                "show_when": ask_gate,
                "full_width": True,
            },
            {
                "key": "action_ask_timeout",
                "label": "Wait for answer (seconds)",
                "type": "number",
                "min": 15,
                "max": 900,
                "value": _int(action.get("timeout_seconds"), 120) if action_type == "ask_yes_no" else 120,
                "show_when": ask_gate,
            },
            {
                "key": "action_yes_type",
                "label": "When Tater hears 'yes'…",
                "type": "select",
                "presentation": "cards",
                "value": yes_type,
                "options": [
                    {"value": "announce", "label": "Announce a confirmation", "icon": "♪"},
                    {"value": "call_service", "label": "Call an HA service", "icon": "⌂"},
                ],
                "show_when": ask_gate,
                "full_width": True,
            },
            {
                "key": "action_yes_message",
                "label": "Confirmation announcement",
                "type": "textarea",
                "value": _text(yes_action.get("message")) if yes_type == "announce" else "",
                "full_width": True,
                "show_when_all": yes_gate_announce,
            },
            {
                "key": "action_yes_targets",
                "label": "Speakers",
                "type": "multiselect",
                "options": announcement_catalog,
                "value": _list(yes_action.get("targets")) if yes_type == "announce" else [],
                "show_when_all": yes_gate_announce,
                "full_width": True,
            },
            {
                "key": "action_yes_domain",
                "label": "Domain",
                "type": "text",
                "value": _text(yes_action.get("domain")) if yes_type == "call_service" else "",
                "show_when_all": yes_gate_service,
            },
            {
                "key": "action_yes_service",
                "label": "Service",
                "type": "text",
                "value": _text(yes_action.get("service")) if yes_type == "call_service" else "",
                "show_when_all": yes_gate_service,
            },
            {
                "key": "action_yes_entity_id",
                "label": "Entity ID (optional)",
                "type": "text",
                "value": _text(yes_action.get("entity_id")) if yes_type == "call_service" else "",
                "full_width": True,
                "show_when_all": yes_gate_service,
            },
            {
                "key": "action_yes_data_json",
                "label": "Data (JSON)",
                "type": "textarea",
                "value": json.dumps(yes_action.get("data"), separators=(",", ":")) if yes_type == "call_service" and yes_action.get("data") else "",
                "full_width": True,
                "show_when_all": yes_gate_service,
            },
        ]
    )
    return fields


# ---------------------------------------------------------------------------
# form editor: item cards + tab data + actions
# ---------------------------------------------------------------------------

def _trigger_label(trigger: Any) -> str:
    trigger = trigger if isinstance(trigger, dict) else {}
    trigger_type = _token(trigger.get("type"))
    if trigger_type == "interval":
        return f"Every {_int(trigger.get('seconds'), 60):.0f}s"
    if trigger_type == "entity_state":
        entity_id = _text(trigger.get("entity"))
        attribute = _text(trigger.get("attribute"))
        if attribute:
            match = _token(trigger.get("match")) or "equals"
            return f"{entity_id} {attribute} {match.replace('_', ' ')} {_text(trigger.get('value'))}"
        state = _text(trigger.get("state"))
        for_seconds = _int(trigger.get("for_seconds"), 0)
        suffix = f" (hold {for_seconds:.0f}s)" if for_seconds else ""
        return f"{entity_id} = {state}{suffix}" if state else entity_id or "—"
    if trigger_type == "time":
        return f"Daily at {_text(trigger.get('time')) or '—'}"
    if trigger_type == "protect_event":
        camera = _text(trigger.get("camera"))
        return f"Protect {_text(trigger.get('event_type')) or 'person'} event" + (f" on {camera}" if camera else "")
    return trigger_type.replace("_", " ").title() if trigger_type else "—"


def _action_label(actions: Any) -> str:
    actions = actions if isinstance(actions, list) else []
    if not actions or not isinstance(actions[0], dict):
        return "—"
    action = actions[0]
    action_type = _token(action.get("type"))
    if action_type == "call_service":
        label = f"{_text(action.get('domain'))}.{_text(action.get('service'))}"
        if _text(action.get("entity_id")):
            label += f" {_text(action.get('entity_id'))}"
        return label
    if action_type == "announce":
        message = _text(action.get("message"))
        if message:
            return f"Announce '{message[:60]}'"
        lines = [line for line in (_text(item) for item in _list(action.get("messages"))) if line]
        if lines:
            return f"Announce random of {len(lines)}: '{lines[0][:60]}'"
        style = _text(action.get("message_style"))
        if style:
            return f"Announce fresh (LLM): '{style[:60]}'"
        return "Announce (no text set)"
    if action_type == "notify":
        priority = _text(action.get("priority"))
        return f"Notify via {_text(action.get('platform')) or 'webui'}" + (f" ({priority})" if priority and priority != "normal" else "")
    if action_type == "wait":
        return f"Wait {_int(action.get('seconds'), 1)}s"
    if action_type == "camera_ai":
        return f"Camera AI on {_text(action.get('camera')) or 'camera'}"
    if action_type == "device":
        label = _ACTION_LABELS.get(_token(action.get("action")), _text(action.get("action")).replace("_", " ").title())
        device_ref = _text(action.get("device"))
        return f"{label} on {device_ref or 'all compatible devices'}"
    if action_type == "ask_yes_no":
        return f"Ask: '{_text(action.get('message'))[:60]}'"
    if action_type == "webhook":
        return f"Webhook {(_text(action.get('method')) or 'POST').upper()} → {_text(action.get('url'))[:60]}"
    return action_type.replace("_", " ").title() if action_type else "—"


def _rule_form(
    auto_id: str,
    definition: Dict[str, Any],
    meta: Dict[str, Any],
    registry: Dict[str, Any],
    client: Any = None,
    *,
    announcement_catalog: Optional[List[Dict[str, Any]]] = None,
    notification_catalog: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    name = _text(definition.get("name")) or auto_id
    enabled = _automation_enabled(definition)
    detail = f"Last run {_now_label(meta.get('last_run_at'))}"
    if _text(meta.get("last_result")):
        detail += f" — {_text(meta.get('last_result'))[:120]}"
    item: Dict[str, Any] = {
        "id": auto_id,
        "group": "automations",
        "title": name,
        "detail": detail,
        "hero_badges": [
            {"label": "Enabled" if enabled else "Disabled", "tone": "running" if enabled else "muted"},
        ],
        "summary_rows": [
            {"label": "Trigger", "value": _trigger_label(definition.get("trigger"))},
            {"label": "Action", "value": _action_label(definition.get("actions"))},
        ],
        "actions": [
            {
                "action": "tab_toggle",
                "label": "Disable" if enabled else "Enable",
                "payload": {"id": auto_id},
                "tone": "warn" if enabled else "run",
            }
        ],
        "run_action": "tab_run",
        "run_label": "Test now",
        "remove_action": "tab_delete",
        "remove_confirm": f"Delete automation '{name}'?",
    }
    if _text(meta.get("last_auto_cooldown")) and float(meta.get("last_auto_cooldown", 0)) > 0:
        item["actions"].append({
            "action": "tab_undamp",
            "label": "Reset damping",
            "payload": {"id": auto_id},
        })
    if _definition_form_supported(definition):
        item["save_action"] = "ga_save_automation"
        item["fields"] = _editor_fields(
            definition,
            registry,
            client,
            announcement_catalog=announcement_catalog,
            notification_catalog=notification_catalog,
        )
    else:
        item["detail"] = detail + " Chat-authored definition — edit it in chat with Tater."
    if _bool(definition.get("builtin")):
        # builtins can be edited/toggled but not deleted; edits are reversible via
        # the card's restore-defaults action (renderer-native reset_action).
        item["reset_action"] = "ga_restore_builtin"
        item["reset_confirm"] = f"Restore '{name}' to its default definition? Your edits to it are lost."
        item.pop("remove_action", None)
        item.pop("remove_confirm", None)
    item["sections"] = [
        {
            "label": "Last Run",
            "fields": [
                {"key": "last_result", "label": "Result", "type": "textarea", "read_only": True, "value": _text(meta.get("last_result"))},
                {"key": "run_count", "label": "Runs", "type": "number", "read_only": True, "value": _int(meta.get("run_count"), 0)},
                {"key": "last_answer", "label": "Last answer", "type": "text", "read_only": True, "value": _text(meta.get("last_answer"))},
            ],
        }
    ]
    return item


def _saved_destination_catalogs(automations: Dict[str, Dict[str, Any]]) -> Tuple[List[str], List[str]]:
    """Collect announcement + notification destination values referenced by saved automations."""
    announcement_targets: List[str] = []
    notification_targets: List[str] = []
    for definition in automations.values():
        if not isinstance(definition, dict):
            continue
        for action in definition.get("actions") if isinstance(definition.get("actions"), list) else []:
            if not isinstance(action, dict):
                continue
            action_type = _token(action.get("type"))
            if action_type in ("announce", "ask_yes_no"):
                announcement_targets.extend(_list(action.get("targets")))
            elif action_type == "camera_ai":
                announce_spec = action.get("announce") if isinstance(action.get("announce"), dict) else {}
                announcement_targets.extend(_list(announce_spec.get("targets")))
                notify_spec = action.get("notify") if isinstance(action.get("notify"), dict) else {}
                if notify_spec:
                    notification_targets.append(_encode_notification_target(notify_spec.get("platform"), notify_spec.get("targets")))
            elif action_type == "notify":
                notification_targets.append(_encode_notification_target(action.get("platform"), action.get("targets")))
    return announcement_targets, notification_targets


def _take_post_create_tab_return(client: Any) -> bool:
    """Consume the one-shot marker set by ``ga_create_automation``.

    The WebUI renderer only re-picks the active manager tab when the current
    tab disappears from ``manager_tabs`` (falling back to ``default_tab``), so
    the refresh that follows a successful create omits the Create tab for
    exactly one fetch: the panel lands back on the Automations list while the
    success popup shows, and the Create tab is offered again from the next
    load.
    """
    raw = client.get(UI_STATE_KEY)
    if not raw:
        return False
    client.set(UI_STATE_KEY, "")
    return True


def get_htmlui_tab_data(redis_client: Any = None, core_key: str = "", core_tab: Any = None) -> Dict[str, Any]:
    rc = redis_client if redis_client is not None else _redis()
    registry = _registry(rc)
    automations = _load_automations(rc)
    return_to_automations = _take_post_create_tab_return(rc)
    metas = _load_meta(rc)
    enabled_count = sum(1 for definition in automations.values() if _automation_enabled(definition))
    pending = _pending_summaries(rc)
    day_ago = time.time() - 86400
    runs_24h = sum(
        1
        for row in _read_activity(rc, limit=200)
        if _text(row.get("event")) in {"run_started", "triggered"} and _as_float(row.get("ts"), 0.0) >= day_ago
    )
    announcement_targets, notification_targets = _saved_destination_catalogs(automations)
    announcement_catalog = _announcement_options(announcement_targets)
    notification_catalog = _notification_options(rc, notification_targets)
    item_forms = [
        _rule_form(
            auto_id,
            definition,
            _meta_for(metas, auto_id),
            registry,
            rc,
            announcement_catalog=announcement_catalog,
            notification_catalog=notification_catalog,
        )
        for auto_id, definition in sorted(
            automations.items(), key=lambda pair: (_text(pair[1].get("name")).casefold(), pair[0])
        )
    ]
    for row in _read_activity(rc, limit=40):
        item_forms.append(
            {
                "id": f"act_{_as_float(row.get('ts'), 0.0):.0f}",
                "group": "activity",
                "title": f"{_text(row.get('event'))} — {_text(row.get('automation'))}",
                "subtitle": _text(row.get("ts_text")),
                "detail": _text(row.get("detail")),
            }
        )
    manager_tabs = [
        {
            "key": "automations",
            "label": "Automations",
            "source": "items",
            "item_group": "automations",
            "selector": False,
            "empty_message": "No automations configured.",
        },
        {"key": "create", "label": "Create Automation", "source": "add_form"},
        {
            "key": "activity",
            "label": "Activity",
            "source": "items",
            "item_group": "activity",
            "selector": False,
            "empty_message": "No activity yet.",
        },
    ]
    if return_to_automations:
        manager_tabs = [tab for tab in manager_tabs if tab["key"] != "create"]
    return {
        "summary": "Generative Agent automations — build them by chatting with Tater or using the form editor.",
        "stats": [
            {"label": "Enabled", "value": enabled_count},
            {"label": "Total", "value": len(automations)},
            {"label": "Runs (24h)", "value": runs_24h},
            {"label": "Pending answers", "value": len(pending)},
        ],
        "items": [],
        "empty_message": "No automations yet — ask Tater in any chat: 'Build me an automation that…'",
        "ui": {
            "kind": "settings_manager",
            "title": "Generative Agent",
            "stats_refresh_button": True,
            "stats_refresh_label": "Refresh devices",
            "stats_refresh_action": "ga_refresh_devices",
            "item_fields_popup": True,
            "item_fields_popup_label": "Edit Automation",
            "default_tab": "automations" if (return_to_automations or automations) else "create",
            "manager_tabs": manager_tabs,
            "add_form": {
                "action": "ga_create_automation",
                "submit_label": "Create Automation",
                "fields": _editor_fields(
                    _blank_definition(),
                    registry,
                    rc,
                    announcement_catalog=announcement_catalog,
                    notification_catalog=notification_catalog,
                ),
            },
            "item_forms": item_forms,
        },
    }


def handle_htmlui_tab_action(
    action: str = "",
    payload: Optional[Dict[str, Any]] = None,
    redis_client: Any = None,
    core_key: str = "",
) -> Dict[str, Any]:
    rc = redis_client if redis_client is not None else _redis()
    payload = dict(payload or {})
    values = _payload_values(payload)
    auto_id = _text(payload.get("id"))
    action = _text(action)
    automations = _load_automations(rc)
    if action == "ga_refresh_devices":
        _registry(rc, refresh=True)
        refreshed = 0
        try:
            watched = _automation_entities(automations)
            if not watched:
                watched = sorted(_ha_entities(rc))[:50]
            refreshed = _refresh_state_catalog(rc, watched)
        except Exception as exc:
            logger.warning("state catalog refresh during ga_refresh_devices failed: %s", exc)
        message = "Integration devices refreshed."
        if refreshed:
            message += f" State history updated for {refreshed} device(s)."
        return {"ok": True, "message": message}
    if action == "ga_create_automation":
        try:
            definition = _definition_from_form(values, payload, client=rc)
        except ValueError as exc:
            return {"ok": False, "message": str(exc)}
        auto_id = f"a_{uuid.uuid4().hex[:8]}"
        definition["id"] = auto_id
        rc.hset(AUTOMATIONS_KEY, auto_id, json.dumps(definition, separators=(",", ":"), default=str))
        _log_activity(rc, auto_id, _text(definition.get("name")), "created", "form editor")
        rc.set(UI_STATE_KEY, "return_to_automations")
        return {"ok": True, "id": auto_id, "message": f"Automation '{_text(definition.get('name'))}' created."}
    if action == "ga_save_automation":
        existing = automations.get(auto_id)
        if not existing:
            return {"ok": False, "message": f"Automation '{auto_id}' not found."}
        if not _definition_form_supported(existing):
            return {"ok": False, "message": "This automation was authored in chat and uses features the form editor cannot express — edit it in chat with Tater."}
        try:
            definition = _definition_from_form(values, payload, existing=existing, client=rc)
        except ValueError as exc:
            return {"ok": False, "message": str(exc)}
        definition["id"] = auto_id
        rc.hset(AUTOMATIONS_KEY, auto_id, json.dumps(definition, separators=(",", ":"), default=str))
        _log_activity(rc, auto_id, _text(definition.get("name")), "updated", "form editor")
        return {"ok": True, "id": auto_id, "message": f"Automation '{_text(definition.get('name'))}' saved."}
    if action in ("tab_toggle", "tab_run", "tab_delete", "tab_undamp") and auto_id not in automations:
        return {"ok": False, "message": f"Automation '{auto_id}' not found."}
    if action == "ga_restore_builtin":
        existing = automations.get(auto_id)
        if not existing:
            return {"ok": False, "message": f"Automation '{auto_id}' not found."}
        builtin_id = _text(existing.get("builtin_id"))
        canonical = next(
            (dict(canonical_definition) for cid, _name, canonical_definition in _builtin_definitions_for_seed(rc) if cid == builtin_id),
            None,
        )
        if canonical is None:
            return {"ok": False, "message": "No builtin default definition is available for this automation."}
        canonical["id"] = auto_id
        enabled = _automation_enabled(existing)
        canonical["enabled"] = enabled
        rc.hset(AUTOMATIONS_KEY, auto_id, json.dumps(canonical, separators=(",", ":"), default=str))
        _log_activity(rc, auto_id, _text(canonical.get("name")), "updated", "restored builtin defaults")
        return {"ok": True, "message": f"Automation '{_text(canonical.get('name'))}' restored to its default definition."}
    if action == "tab_toggle":
        result = run_hydra_kernel_tool("automation_toggle", {"id": auto_id}, redis_client=rc)
        return {"ok": bool(result.get("ok")), "message": "; ".join(result.get("facts") or []) or "Done."}
    if action == "tab_undamp":
        ok, message = _undamp_automation(rc, auto_id)
        return {"ok": ok, "message": "Damping reset." if ok else message}
    if action == "tab_run":
        result = run_hydra_kernel_tool("automation_run", {"id": auto_id}, redis_client=rc)
        message = "; ".join(result.get("facts") or [])
        if not message and isinstance(result.get("error"), dict):
            message = _text(result["error"].get("message"))
        return {"ok": bool(result.get("ok")), "message": message or "Done."}
    if action == "tab_delete":
        result = run_hydra_kernel_tool("automation_delete", {"id": auto_id}, redis_client=rc)
        return {"ok": bool(result.get("ok")), "message": "; ".join(result.get("facts") or []) or "Deleted."}
    return {"ok": False, "message": f"Unknown action '{action}'."}


def handle_core_webhook(
    webhook: str = "",
    payload: Optional[Dict[str, Any]] = None,
    query: Optional[Dict[str, Any]] = None,
    body: Any = None,
    method: str = "",
    headers: Optional[Dict[str, Any]] = None,
    redis_client: Any = None,
    core_key: str = "",
) -> Dict[str, Any]:
    rc = redis_client if redis_client is not None else _redis()
    query = dict(query or {})
    if _text(webhook) != "answer":
        return {"ok": False, "message": "Unknown webhook. Use /webhook/answer?p=<pending_id>&response=yes|no"}
    pending_id = _text(query.get("p"))
    response = _classify_response(_text(query.get("response")))
    try:
        raw_pending = rc.hget(PENDING_KEY, pending_id)
    except Exception:
        raw_pending = None
    pending = _json_loads(raw_pending, {})
    if not isinstance(pending, dict) or not pending:
        return {"ok": False, "message": "Pending automation question not found (it may have expired)."}
    verdict = response if response in ("yes", "no") else "unanswered"
    branch_key = f"{verdict}_actions" if verdict in ("yes", "no") else "unanswered_actions"
    automation_id = _text(pending.get("automation_id"))
    branch = pending.get(branch_key) if isinstance(pending.get(branch_key), list) else []
    shell = {"name": pending.get("automation_name"), "id": automation_id}
    notes = _execute_actions(rc, shell, branch) if branch else ["no actions configured for this answer"]
    _log_activity(rc, automation_id, _text(pending.get("automation_name")), f"answered_{verdict}" if verdict in ("yes", "no") else "timed_out", "webhook — " + ("; ".join(notes) if notes else "no actions"))
    rc.hdel(PENDING_KEY, pending_id)
    try:
        rc_meta = _load_meta(rc).get(automation_id)
        if isinstance(rc_meta, dict):
            rc_meta["last_answer"] = f"{verdict} (webhook)"
            _save_meta(rc, automation_id, rc_meta)
    except Exception:
        pass
    _record_pending_outcome(rc, automation_id, verdict)
    return {"ok": True, "message": f"Automation question answered '{verdict}'."}

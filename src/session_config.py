"""Central place to manage login cookies and User-Agent for LianJia crawlers.

Fill in ``DEFAULT_COOKIE_STRING`` and ``DEFAULT_USER_AGENT`` with the values
captured from your logged-in browser session. Both ``src/main.py`` and
``src/detail_scraper.py`` will import from this module so you only need to update
these values in one place.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

# ---------------------------------------------------------------------------
# User-supplied defaults (safe placeholders by default)
# ---------------------------------------------------------------------------
DEFAULT_COOKIE_STRING = ""
"""Raw Cookie header copied from the browser. Leave empty if not available."""

DEFAULT_COOKIE_FILE = None
"""Optional path (absolute or relative to project root) to a cookie file."""

DEFAULT_USER_AGENT = ""
"""Browser User-Agent string captured together with the cookie."""


# ---------------------------------------------------------------------------
# Helper functions reused by crawlers
# ---------------------------------------------------------------------------

def parse_cookie_string(raw: str | None) -> Dict[str, str]:
    """Convert a Cookie header string into a dict usable by ``requests``."""
    cookies: Dict[str, str] = {}
    if not raw:
        return cookies
    for pair in raw.split(';'):
        pair = pair.strip()
        if not pair or '=' not in pair:
            continue
        key, value = pair.split('=', 1)
        key = key.strip()
        value = value.strip()
        if key:
            cookies[key] = value
    return cookies


def load_cookie_file(path: Path) -> Dict[str, str]:
    """Load cookies from file (JSON mapping/list or raw cookie string)."""
    if not path.exists():
        raise FileNotFoundError(f"Cookie file not found: {path}")
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items()}
        if isinstance(data, list):
            return {
                str(item[0]): str(item[1])
                for item in data
                if isinstance(item, (list, tuple)) and len(item) >= 2
            }
    except json.JSONDecodeError:
        pass
    return parse_cookie_string(raw)


def get_default_cookie_dict(base_dir: Path | None = None) -> Dict[str, str]:
    """Return default cookies provided in this configuration."""
    raw = DEFAULT_COOKIE_STRING.strip()
    if raw:
        return parse_cookie_string(raw)
    if DEFAULT_COOKIE_FILE:
        path = Path(DEFAULT_COOKIE_FILE)
        if not path.is_absolute() and base_dir:
            path = base_dir / DEFAULT_COOKIE_FILE
        return load_cookie_file(path)
    return {}


def get_default_cookie_string() -> str:
    return DEFAULT_COOKIE_STRING.strip()


def get_default_user_agent() -> str:
    return DEFAULT_USER_AGENT.strip()

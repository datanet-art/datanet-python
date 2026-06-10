"""Load example configuration from the repo-level .env file.

This keeps the examples dependency-free while still supporting the common
copy-.env.example-to-.env workflow.
"""

from __future__ import annotations

import os
from pathlib import Path


def load_example_env() -> None:
    """Load simple KEY=VALUE pairs from .env without overriding real env vars."""

    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key:
            os.environ.setdefault(key, value)

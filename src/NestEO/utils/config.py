"""YAML config loading helpers."""
from __future__ import annotations

import os
from pathlib import Path

import yaml


def load_config(config_path: str | Path) -> dict:
    """Load a YAML config file, expanding ~ and env vars in string values."""
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")
    with config_path.open() as f:
        cfg = yaml.safe_load(f)
    return expand_paths(cfg)


def expand_paths(cfg: dict) -> dict:
    """Recursively expand ~ and $ENV_VAR in string values of a dict."""
    out = {}
    for k, v in cfg.items():
        if isinstance(v, str):
            out[k] = os.path.expandvars(os.path.expanduser(v))
        elif isinstance(v, dict):
            out[k] = expand_paths(v)
        else:
            out[k] = v
    return out

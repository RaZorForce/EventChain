# -*- coding: utf-8 -*-

import os
from pathlib import Path
import yaml

# Default config file
DEFAULT_CONFIG = "backtest"


def load_config(config_name: str = None) -> dict:
    """
    Load configuration from YAML file.

    Args:
        config_name: Name of config file (without .yaml extension).
                    Defaults to DEFAULT_CONFIG or CONFIG_NAME env var.
    """
    if config_name is None:
        config_name = os.getenv("CONFIG_NAME", DEFAULT_CONFIG)

    config_dir = Path(__file__).parent.parent / "config"
    config_path = config_dir / f"{config_name}.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# Load default config for module-level access
_config = load_config()

SYMBOLS = _config.get("symbols", [])
CSV_DIR = _config.get("data", {}).get("csv_dir", "data/Historical/Daily")
START_DATE = _config.get("data", {}).get("start_date", "20230215")
INITIAL_CAPITAL = float(_config.get("portfolio", {}).get("initial_capital", 100000.0))
STRATEGY_NAME = _config.get("strategy", "double_top")

# Strategy registry - import here to avoid circular imports
from src.strategy import (
    doubleTop,
    doubleBottom,
    BuyAndHoldStrategy,
)

STRATEGY_REGISTRY = {
    "double_top": doubleTop,
    "double_bottom": doubleBottom,
    "buy_and_hold": BuyAndHoldStrategy,
}

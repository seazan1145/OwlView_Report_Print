from __future__ import annotations

import json
from pathlib import Path

from .ini_migration import migrate_ini_to_config
from .models import AppConfig, default_seed_config, settings_path


class ConfigStore:
    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.settings_file = settings_path(base_dir)
        self.legacy_ini = base_dir / "Settings" / "Settings.ini"

    def load(self) -> AppConfig:
        self.settings_file.parent.mkdir(parents=True, exist_ok=True)
        if self.settings_file.exists():
            try:
                return AppConfig.from_dict(json.loads(self.settings_file.read_text(encoding="utf-8")))
            except Exception:
                broken = self.settings_file.with_suffix(".broken.json")
                self.settings_file.replace(broken)
        if self.legacy_ini.exists():
            config = migrate_ini_to_config(self.legacy_ini)
            backup = self.legacy_ini.with_suffix(".ini.bak")
            if not backup.exists():
                self.legacy_ini.replace(backup)
            self.save(config)
            return config
        cfg = default_seed_config()
        self.save(cfg)
        return cfg

    def save(self, cfg: AppConfig) -> None:
        self.settings_file.parent.mkdir(parents=True, exist_ok=True)
        self.settings_file.write_text(json.dumps(cfg.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")

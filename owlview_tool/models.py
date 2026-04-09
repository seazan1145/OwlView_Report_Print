from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Literal


OutputFormat = Literal["pdf", "jpg", "jpg&pdf"]
Orientation = Literal["portrait", "landscape"]


@dataclass
class PartConfig:
    enabled: bool = True
    selected: bool = False
    part_name: str = ""
    output_name: str = ""
    output_dir: str = ""
    output_format: OutputFormat = "pdf"
    scale: float = 100.0
    orientation: Orientation = "portrait"
    margin_top: float = 0.0
    margin_bottom: float = 0.0
    margin_left: float = 0.0
    margin_right: float = 0.0
    paper_width: float = 8.27
    paper_height: float = 11.69
    print_range: str = ""
    jpg_quality: int = 90
    local_copy_enabled: bool = False
    notes: str = ""

    def resolved_name(self, token_yymmdd: str) -> str:
        return self.output_name.replace("yymmdd", token_yymmdd)

    def validate(self) -> list[str]:
        errors: list[str] = []
        banned = '<>:"/\\|?*'
        if any(ch in self.output_name for ch in banned):
            errors.append(f"保存名に禁止文字があります: {self.output_name}")
        if self.scale <= 0:
            errors.append("scale は 0 より大きい値にしてください")
        if self.jpg_quality < 1 or self.jpg_quality > 100:
            errors.append("jpg_quality は 1〜100 にしてください")
        if self.paper_width <= 0 or self.paper_height <= 0:
            errors.append("用紙サイズは正の値を指定してください")
        return errors


@dataclass
class CommonConfig:
    owlview_home_url: str = "https://owlview.sunrise-office.net"
    owlview_report_url: str = "https://owlview.sunrise-office.net/report"
    xpath_input_box: str = "/html/body/div[1]/div[2]/div/div[2]/div[1]/div[2]/div/div[1]/input"
    selenium_wait_sec: int = 5
    default_output_root: str = ""
    default_local_copy_dir: str = ""
    ftp_default_enabled: bool = True
    ftp_protocol: str = "FTP"
    ftp_encryption: str = "Implicit TLS/SSL"
    ftp_host: str = ""
    ftp_port: int = 990
    ftp_username: str = ""
    ftp_password: str = ""
    ftp_remote_path_template: str = ""
    print_default_enabled: bool = True
    default_printer_name: str = ""
    default_print_copies: int = 1
    auto_save_settings: bool = True
    preview_auto_refresh: bool = False
    last_selected_part_index: int = 0
    window_geometry: str = "1200x760+80+40"
    window_maximized: bool = False
    xpath_report_ready: str = ""
    xpath_search_ready: str = ""
    chromedriver_path: str = ""
    curl_path: str = ""
    sumatra_path: str = ""


@dataclass
class AppConfig:
    parts: list[PartConfig] = field(default_factory=list)
    common: CommonConfig = field(default_factory=CommonConfig)
    version: int = 4

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "AppConfig":
        common_fields = set(CommonConfig.__dataclass_fields__.keys())
        raw_common = data.get("common", {})
        if not isinstance(raw_common, dict):
            raw_common = {}
        common = CommonConfig(**{k: v for k, v in raw_common.items() if k in common_fields})
        part_fields = set(PartConfig.__dataclass_fields__.keys())
        cleaned_parts: list[PartConfig] = []
        for raw in data.get("parts", []):
            if not isinstance(raw, dict):
                continue
            p = {k: v for k, v in raw.items() if k in part_fields}
            cleaned_parts.append(PartConfig(**p))
        parts = cleaned_parts
        for p in parts:
            if p.output_format == "both":
                p.output_format = "jpg&pdf"
            if p.orientation not in {"portrait", "landscape"}:
                p.orientation = "portrait"
            p.local_copy_enabled = bool(p.local_copy_enabled)
        loaded_version = int(data.get("version", 1))
        if loaded_version < 4:
            # v4: 実行時トグルへ統合した旧キー(存在しても未使用)を段階的に吸収済み。
            pass
        return cls(parts=parts, common=common, version=4)


def default_seed_config() -> AppConfig:
    return AppConfig(
        parts=[
            PartConfig(
                enabled=True,
                selected=True,
                part_name="劇場第２章 Bパート",
                output_name="LLNM2_B_yymmdd",
                output_dir=r"\\sr-fs\2st-data1\03_Works\LLN劇場第2章\01_制作\Bパート_c171-c343\97_状況報告書",
                output_format="jpg&pdf",
                scale=39,
                orientation="landscape",
            ),
            PartConfig(
                enabled=True,
                selected=True,
                part_name="劇場第２章 せつ菜ダンスパート",
                output_name="LLNM2_Y_setsuna_yymmdd",
                output_dir=r"\\sr-fs\2st-data1\03_Works\LLN劇場第2章\01_制作\Yパート_せつ菜ダンス\97_状況報告書",
                output_format="jpg&pdf",
                scale=60,
                orientation="landscape",
            ),
        ],
        common=CommonConfig(
            ftp_default_enabled=True,
            ftp_host="ftps.sunrise-office.net",
            ftp_port=990,
            ftp_remote_path_template="/kawamura/From_sunrise/LLN劇場２章__状況表/yymmdd/",
            print_default_enabled=True,
            default_printer_name="14-2",
            default_print_copies=2,
            selenium_wait_sec=5,
        ),
    )


def settings_path(base_dir: Path) -> Path:
    return base_dir / "Settings" / "settings.json"

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
    print_copies: int = 0
    enable_inputtable_excel_export: bool = False
    inputtable_excel_output_dir: str = ""
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
        if self.print_copies < 0:
            errors.append("印刷部数は 0 以上にしてください")
        return errors


@dataclass
class DebugConfig:
    enabled: bool = False
    headless: bool = True
    verbose_log: bool = False
    save_screenshot_on_error: bool = True
    save_html_on_error: bool = True
    selenium_wait_timeout: int = 5
    input_settle_wait: float = 1.0
    report_direct_navigation: bool = True


@dataclass
class AppCommonConfig:
    # アプリ共通設定
    owlview_home_url: str = "https://owlview.sunrise-office.net"
    owlview_report_url: str = "https://owlview.sunrise-office.net/report"
    xpath_input_box: str = "/html/body/div[1]/div[2]/div/div[2]/div[1]/div[2]/div/div[1]/input"
    xpath_report_ready: str = ""
    xpath_search_ready: str = ""
    selenium_wait_sec: int = 5
    default_output_root: str = ""
    default_local_copy_dir: str = ""
    ftp_protocol: str = "FTP"
    ftp_encryption: str = "Implicit TLS/SSL"
    ftp_host: str = ""
    ftp_port: int = 990
    ftp_username: str = ""
    ftp_password: str = ""
    ftp_remote_path_template: str = ""
    default_printer_name: str = ""
    default_print_copies: int = 1
    auto_save_settings: bool = True
    chromedriver_path: str = ""
    curl_path: str = ""
    sumatra_path: str = ""
    debug: DebugConfig = field(default_factory=DebugConfig)


# backward compatible alias (services.py 型注釈などで使用)
CommonConfig = AppCommonConfig


@dataclass
class JobCommonConfig:
    # ジョブ共通設定
    ftp_default_enabled: bool = True
    print_default_enabled: bool = True
    output_format_default: OutputFormat = "jpg&pdf"
    shared_output_dir: str = ""


@dataclass
class UIStateConfig:
    # UI状態
    preview_auto_refresh: bool = False
    last_selected_part_index: int = 0
    window_geometry: str = "1200x760+80+40"
    window_maximized: bool = False
    pane_left_width: int = 680
    last_selected_tab: str = "main"
    preview_zoom: float = 1.0


@dataclass
class AppConfig:
    parts: list[PartConfig] = field(default_factory=list)
    app: AppCommonConfig = field(default_factory=AppCommonConfig)
    job: JobCommonConfig = field(default_factory=JobCommonConfig)
    ui: UIStateConfig = field(default_factory=UIStateConfig)
    version: int = 6

    @property
    def common(self) -> AppCommonConfig:
        # 既存コード互換
        return self.app

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "AppConfig":
        part_fields = set(PartConfig.__dataclass_fields__.keys())
        app_fields = set(AppCommonConfig.__dataclass_fields__.keys())
        job_fields = set(JobCommonConfig.__dataclass_fields__.keys())
        ui_fields = set(UIStateConfig.__dataclass_fields__.keys())
        debug_fields = set(DebugConfig.__dataclass_fields__.keys())

        raw_app = data.get("app", {}) if isinstance(data.get("app"), dict) else {}
        raw_job = data.get("job", {}) if isinstance(data.get("job"), dict) else {}
        raw_ui = data.get("ui", {}) if isinstance(data.get("ui"), dict) else {}

        # 旧 settings.json(v5-) 互換: common から app/job/ui を組み立て
        raw_common = data.get("common", {}) if isinstance(data.get("common"), dict) else {}
        if not raw_app and raw_common:
            raw_app = dict(raw_common)

        # job へ吸収
        if not raw_job and raw_common:
            raw_job = {
                "ftp_default_enabled": raw_common.get("ftp_default_enabled", True),
                "print_default_enabled": raw_common.get("print_default_enabled", True),
                "output_format_default": raw_common.get("output_format_default", "jpg&pdf"),
                "shared_output_dir": raw_common.get("shared_output_dir", ""),
            }

        # ui へ吸収
        if not raw_ui and raw_common:
            raw_ui = {
                "preview_auto_refresh": raw_common.get("preview_auto_refresh", False),
                "last_selected_part_index": raw_common.get("last_selected_part_index", 0),
                "window_geometry": raw_common.get("window_geometry", "1200x760+80+40"),
                "window_maximized": raw_common.get("window_maximized", False),
                "pane_left_width": raw_common.get("pane_left_width", 680),
                "last_selected_tab": raw_common.get("last_selected_tab", "main"),
                "preview_zoom": raw_common.get("preview_zoom", 1.0),
            }

        # debug 吸収
        raw_debug = raw_app.get("debug", {}) if isinstance(raw_app.get("debug"), dict) else {}
        legacy_debug = {
            "headless": raw_app.get("headless"),
            "verbose_log": raw_app.get("verbose_log"),
            "save_screenshot_on_error": raw_app.get("save_screenshot_on_error"),
            "save_html_on_error": raw_app.get("save_html_on_error"),
            "selenium_wait_timeout": raw_app.get("selenium_wait_timeout"),
            "input_settle_wait": raw_app.get("input_settle_wait"),
            "report_direct_navigation": raw_app.get("report_direct_navigation"),
        }
        for key, value in legacy_debug.items():
            if key not in raw_debug and value is not None:
                raw_debug[key] = value
        if "enabled" not in raw_debug:
            raw_debug["enabled"] = bool(raw_debug.get("verbose_log", False))
        debug = DebugConfig(**{k: v for k, v in raw_debug.items() if k in debug_fields})

        app_values = {k: v for k, v in raw_app.items() if k in app_fields and k != "debug"}
        app = AppCommonConfig(**app_values)
        app.debug = debug

        job = JobCommonConfig(**{k: v for k, v in raw_job.items() if k in job_fields})
        if job.output_format_default not in {"pdf", "jpg", "jpg&pdf"}:
            job.output_format_default = "jpg&pdf"

        ui = UIStateConfig(**{k: v for k, v in raw_ui.items() if k in ui_fields})

        cleaned_parts: list[PartConfig] = []
        for raw in data.get("parts", []):
            if not isinstance(raw, dict):
                continue
            p = PartConfig(**{k: v for k, v in raw.items() if k in part_fields})
            if p.output_format == "both":
                p.output_format = "jpg&pdf"
            if p.orientation not in {"portrait", "landscape"}:
                p.orientation = "portrait"
            p.local_copy_enabled = bool(p.local_copy_enabled)
            cleaned_parts.append(p)

        return cls(parts=cleaned_parts, app=app, job=job, ui=ui, version=6)


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
        app=AppCommonConfig(
            ftp_host="ftps.sunrise-office.net",
            ftp_port=990,
            ftp_remote_path_template="/kawamura/From_sunrise/LLN劇場２章__状況表/yymmdd/",
            default_printer_name="14-2",
            default_print_copies=2,
            selenium_wait_sec=5,
        ),
        job=JobCommonConfig(
            ftp_default_enabled=True,
            print_default_enabled=True,
            output_format_default="jpg&pdf",
            shared_output_dir="",
        ),
    )


def settings_path(base_dir: Path) -> Path:
    return base_dir / "Settings" / "settings.json"

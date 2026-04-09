from __future__ import annotations

from pathlib import Path

from .models import AppConfig, CommonConfig, PartConfig


def _read_ini(path: Path) -> dict[str | None, list[str]]:
    cfg: dict[str | None, list[str]] = {}
    sec: str | None = None
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("[") and line.endswith("]"):
            sec = line[1:-1]
            cfg.setdefault(sec, [])
            continue
        cfg.setdefault(sec, []).append(line)
    return cfg


def _first(sec: list[str], default: str = "") -> str:
    if not sec:
        return default
    s = sec[0]
    return s.split("=", 1)[-1].strip()


def _bool(v: str, default: bool = False) -> bool:
    if not v:
        return default
    return v.strip().lower() in {"1", "true", "yes", "on"}


def migrate_ini_to_config(ini_path: Path) -> AppConfig:
    ini = _read_ini(ini_path)
    eps = ini.get("EP", [])
    names = ini.get("PDF_Name", [])
    dirs = ini.get("Directory", [])
    scales = [int(x.split("=", 1)[-1]) for x in ini.get("Size_Set", []) if "=" in x] or [100]
    dirs_flag = [_bool(x) for x in ini.get("pdf_direct", [])] or [False]

    local_copy = _bool(_first(ini.get("Local_Copy", []), "False"))
    ftp_upload = _bool(_first(ini.get("FTP_Upload", []), "False"))
    print_auto = _bool(_first(ini.get("Print_Auto", []), "False"))

    ftp_path = _first(ini.get("FTP_Path", []), "")
    ftp_dict: dict[str, str] = {}
    for row in ini.get("FTP_Set", []):
        if "=" in row:
            k, v = row.split("=", 1)
            ftp_dict[k.strip().lower()] = v.strip()

    printer_name = _first(ini.get("Printer_Name", []), "")
    copies = int(_first(ini.get("Print_busu", []), "1") or "1")
    wait = int(_first(ini.get("Script_Time", []), "5") or "5")

    parts: list[PartConfig] = []
    for i, ep in enumerate(eps):
        parts.append(
            PartConfig(
                enabled=True,
                part_name=ep,
                output_name=names[i] if i < len(names) else ep.replace(" ", "_"),
                output_dir=dirs[i] if i < len(dirs) else "",
                output_format="pdf",
                scale=scales[i] if i < len(scales) else scales[-1],
                orientation="landscape" if (dirs_flag[i] if i < len(dirs_flag) else dirs_flag[-1]) else "portrait",
                local_copy_enabled=local_copy,
            )
        )

    common = CommonConfig(
        selenium_wait_sec=wait,
        ftp_default_enabled=ftp_upload,
        ftp_protocol=ftp_dict.get("protocol", "FTP"),
        ftp_encryption=ftp_dict.get("encryption", "Implicit TLS/SSL"),
        ftp_host=ftp_dict.get("host", ""),
        ftp_port=int(ftp_dict.get("port", "990")),
        ftp_username=ftp_dict.get("username", ""),
        ftp_password=ftp_dict.get("password", ""),
        ftp_remote_path_template=ftp_path,
        print_default_enabled=print_auto,
        default_printer_name=printer_name,
        default_print_copies=copies,
    )
    return AppConfig(parts=parts, common=common)

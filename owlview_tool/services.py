from __future__ import annotations

import base64
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from PIL import Image

from .models import CommonConfig, PartConfig

try:
    import win32print  # type: ignore
except ImportError:  # pragma: no cover
    win32print = None


@dataclass
class ExternalTools:
    chromedriver: Path
    curl: Path
    sumatra: Path


@dataclass
class CurlResult:
    command_summary: str
    returncode: int
    stdout: str
    stderr: str


def resolve_tool_path(configured: str, fallback: Path, exe_name: str) -> Path:
    if configured.strip():
        return Path(configured).expanduser()
    if fallback.exists():
        return fallback
    from shutil import which

    found = which(exe_name)
    return Path(found) if found else fallback


def printer_list() -> list[str]:
    if not win32print:
        return []
    return sorted({p[2] for p in win32print.EnumPrinters(2)})


def print_with_sumatra(sumatra: Path, pdf_path: Path, printer: str, copies: int) -> None:
    creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    subprocess.run(
        [str(sumatra), "-silent", "-exit-on-print", "-print-to", printer, "-print-settings", f"{copies}x", str(pdf_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
        text=True,
        creationflags=creationflags,
    )


def save_pdf(driver, dest: Path, part: PartConfig) -> None:
    prefs = {
        "paperWidth": part.paper_width,
        "paperHeight": part.paper_height,
        "marginTop": part.margin_top,
        "marginBottom": part.margin_bottom,
        "marginLeft": part.margin_left,
        "marginRight": part.margin_right,
        "printBackground": True,
        "scale": part.scale / 100.0,
        "landscape": part.orientation == "landscape",
    }
    data = driver.execute_cdp_cmd("Page.printToPDF", prefs)["data"]
    dest.write_bytes(base64.b64decode(data))


def save_jpg_from_screenshot(driver, dest: Path, quality: int = 90) -> None:
    png = driver.get_screenshot_as_png()
    tmp = dest.with_suffix(".tmp.png")
    tmp.write_bytes(png)
    with Image.open(tmp) as im:
        rgb = im.convert("RGB")
        rgb.save(dest, "JPEG", quality=quality)
    tmp.unlink(missing_ok=True)


def validate_ftp_path_template(path_template: str) -> list[str]:
    issues: list[str] = []
    v = path_template.strip()
    if not v:
        issues.append("FTP Path が未設定です")
        return issues
    if "//" in v:
        issues.append("FTP Path に連続スラッシュ(//)があります")
    if " " in v:
        issues.append("FTP Path に空白が含まれています")
    if "yymmdd" not in v:
        issues.append("FTP Path に yymmdd トークンがありません")
    return issues


def resolved_remote_path(path_template: str, token: str | None = None) -> str:
    t = token or datetime.now().strftime("%y%m%d")
    return path_template.replace("yymmdd", t).strip()


def _build_curl_base_command(common: CommonConfig, curl_path: Path, *, timeout_sec: int) -> list[str]:
    enc = common.ftp_encryption.lower()
    scheme = "ftps" if "implicit" in enc else "ftp"
    base = [
        str(curl_path),
        "--connect-timeout",
        str(timeout_sec),
        "--max-time",
        str(max(timeout_sec * 2, 30)),
        "--fail",
        "--show-error",
        "--disable-epsv",
        "--user",
        f"{common.ftp_username}:{common.ftp_password}",
    ]
    if "explicit" in enc:
        base.append("--ssl-reqd")
    if "implicit" in enc:
        base.append("--ftp-ssl")
    base.append(f"{scheme}://{common.ftp_host}:{common.ftp_port}")
    return base


def _sanitize_command(cmd: list[str], password: str) -> str:
    rendered: list[str] = []
    for token in cmd:
        replaced = token.replace(password, "********") if password else token
        rendered.append(replaced)
    return " ".join(rendered)


def run_ftp_curl_command(common: CommonConfig, curl_path: Path, extra_args: list[str], *, timeout_sec: int = 20) -> CurlResult:
    cmd = _build_curl_base_command(common, curl_path, timeout_sec=timeout_sec)
    cmd.extend(extra_args)
    creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    p = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        creationflags=creationflags,
        check=False,
    )
    return CurlResult(
        command_summary=_sanitize_command(cmd, common.ftp_password),
        returncode=p.returncode,
        stdout=(p.stdout or "").strip(),
        stderr=(p.stderr or "").strip(),
    )


def ftp_test_connection(common: CommonConfig, curl_path: Path) -> tuple[str, CurlResult]:
    remote = resolved_remote_path(common.ftp_remote_path_template)
    remote_url = remote.strip("/")
    test_target = f"{remote_url}/" if remote_url else ""
    result = run_ftp_curl_command(common, curl_path, ["--list-only", test_target], timeout_sec=15)
    if result.returncode != 0:
        raise RuntimeError(
            "\n".join(
                [
                    "FTP接続テスト失敗",
                    f"host={common.ftp_host}",
                    f"port={common.ftp_port}",
                    f"encryption={common.ftp_encryption}",
                    f"username={common.ftp_username}",
                    f"remote_path={remote}",
                    f"curl={result.command_summary}",
                    f"stdout={result.stdout or '(empty)'}",
                    f"stderr={result.stderr or '(empty)'}",
                ]
            )
        )
    return remote, result


def ftp_upload(local_file: Path, common: CommonConfig, curl_path: Path) -> tuple[str, CurlResult]:
    remote = resolved_remote_path(common.ftp_remote_path_template)
    remote_clean = remote.strip("/")
    target = f"{remote_clean}/{local_file.name}" if remote_clean else local_file.name
    result = run_ftp_curl_command(
        common,
        curl_path,
        ["--ftp-create-dirs", "--upload-file", str(local_file), target],
        timeout_sec=30,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "\n".join(
                [
                    "FTPアップロード失敗",
                    f"host={common.ftp_host}",
                    f"port={common.ftp_port}",
                    f"encryption={common.ftp_encryption}",
                    f"username={common.ftp_username}",
                    f"remote_path={remote}",
                    f"curl={result.command_summary}",
                    f"stdout={result.stdout or '(empty)'}",
                    f"stderr={result.stderr or '(empty)'}",
                ]
            )
        )
    return target, result


def local_copy(src: Path, common: CommonConfig) -> None:
    dst_dir = Path(common.default_local_copy_dir)
    if not str(dst_dir):
        return
    dst_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst_dir / src.name)

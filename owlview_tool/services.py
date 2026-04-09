from __future__ import annotations

import base64
import ftplib
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


def printer_list() -> list[str]:
    if not win32print:
        return []
    return [p[2] for p in win32print.EnumPrinters(2)]


def print_with_sumatra(sumatra: Path, pdf_path: Path, printer: str, copies: int) -> None:
    creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    subprocess.run(
        [str(sumatra), "-silent", "-exit-on-print", "-print-to", printer, "-print-settings", f"{copies}x", str(pdf_path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=True,
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


def ftp_test_connection(common: CommonConfig) -> None:
    if common.ftp_encryption.lower().startswith("implicit"):
        ftp = ftplib.FTP_TLS()
        ftp.connect(common.ftp_host, common.ftp_port, timeout=10)
        ftp.auth()
        ftp.prot_p()
    else:
        ftp = ftplib.FTP()
        ftp.connect(common.ftp_host, common.ftp_port, timeout=10)
    ftp.login(common.ftp_username, common.ftp_password)
    ftp.quit()


def ftp_upload(local_file: Path, common: CommonConfig) -> None:
    token = datetime.now().strftime("%y%m%d")
    remote_tpl = common.ftp_remote_path_template.replace("yymmdd", token).strip("/")
    filename = local_file.name

    if common.ftp_encryption.lower().startswith("implicit"):
        ftp = ftplib.FTP_TLS()
        ftp.connect(common.ftp_host, common.ftp_port, timeout=30)
        ftp.auth()
        ftp.prot_p()
    else:
        ftp = ftplib.FTP()
        ftp.connect(common.ftp_host, common.ftp_port, timeout=30)
    ftp.login(common.ftp_username, common.ftp_password)

    curr = ""
    for section in [s for s in remote_tpl.split("/") if s]:
        curr = f"{curr}/{section}" if curr else section
        try:
            ftp.mkd(curr)
        except Exception:
            pass

    remote_path = f"{remote_tpl}/{filename}" if remote_tpl else filename
    with local_file.open("rb") as f:
        ftp.storbinary(f"STOR {remote_path}", f)
    ftp.quit()


def local_copy(src: Path, common: CommonConfig) -> None:
    dst_dir = Path(common.default_local_copy_dir)
    if not str(dst_dir):
        return
    dst_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst_dir / src.name)

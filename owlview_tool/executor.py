from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from queue import Queue

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

from .models import AppConfig, PartConfig
from .services import ExternalTools, ftp_upload, local_copy, print_with_sumatra, save_jpg_from_screenshot, save_pdf


@dataclass
class JobResult:
    part_name: str
    success: bool
    message: str
    outputs: list[Path]


class Runner:
    def __init__(self, cfg: AppConfig, tools: ExternalTools, queue: Queue):
        self.cfg = cfg
        self.tools = tools
        self.queue = queue
        self.stop_event = threading.Event()

    def stop(self) -> None:
        self.stop_event.set()

    def run_async(self, parts: list[PartConfig]) -> threading.Thread:
        t = threading.Thread(target=self._run, args=(parts,), daemon=True)
        t.start()
        return t

    def _emit(self, kind: str, payload: dict) -> None:
        self.queue.put((kind, payload))

    def _run(self, parts: list[PartConfig]) -> None:
        total = len(parts)
        self._emit("start", {"total": total})
        opts = Options()
        opts.add_argument("--headless=new")
        driver = webdriver.Chrome(service=Service(str(self.tools.chromedriver)), options=opts)
        results: list[JobResult] = []
        try:
            for idx, part in enumerate(parts, start=1):
                if self.stop_event.is_set():
                    self._emit("log", {"text": "ユーザーキャンセルにより停止しました。"})
                    break
                results.append(self._run_part(driver, idx, total, part))
        finally:
            driver.quit()
        self._emit("done", {"results": results})

    def _run_part(self, driver, idx: int, total: int, part: PartConfig) -> JobResult:
        common = self.cfg.common
        outputs: list[Path] = []
        tag = f"[{idx}/{total}] {part.part_name}"
        try:
            self._emit("progress", {"value": idx - 1, "total": total, "text": f"開始 {tag}"})
            driver.get(common.owlview_home_url)
            time.sleep(common.selenium_wait_sec)
            box = driver.find_element(By.XPATH, common.xpath_input_box)
            box.clear()
            box.send_keys(part.part_name)
            time.sleep(common.selenium_wait_sec)
            driver.get(common.owlview_report_url)
            time.sleep(common.selenium_wait_sec)

            stamp = datetime.now().strftime("%y%m%d")
            base = part.resolved_name(stamp)
            out_dir = Path(part.output_dir)
            out_dir.mkdir(parents=True, exist_ok=True)

            if part.output_format in {"pdf", "both"}:
                pdf_path = out_dir / f"{base}.pdf"
                save_pdf(driver, pdf_path, part)
                outputs.append(pdf_path)
            if part.output_format in {"jpg", "both"}:
                jpg_path = out_dir / f"{base}.jpg"
                save_jpg_from_screenshot(driver, jpg_path, part.jpg_quality)
                outputs.append(jpg_path)

            for p in outputs:
                if part.local_copy_enabled:
                    local_copy(p, common)
                if part.ftp_upload_enabled:
                    ftp_upload(p, common)
                if part.print_enabled and p.suffix.lower() == ".pdf" and part.printer_name:
                    print_with_sumatra(self.tools.sumatra, p, part.printer_name, max(1, part.copies))

            self._emit("progress", {"value": idx, "total": total, "text": f"完了 {tag}"})
            self._emit("log", {"text": f"完了: {part.part_name}"})
            return JobResult(part.part_name, True, "ok", outputs)
        except Exception as exc:
            self._emit("log", {"text": f"失敗: {part.part_name}: {exc}"})
            self._emit("progress", {"value": idx, "total": total, "text": f"失敗 {tag}"})
            return JobResult(part.part_name, False, str(exc), outputs)

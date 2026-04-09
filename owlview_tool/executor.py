from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from queue import Queue

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

from .models import AppConfig, PartConfig
from .services import (
    ExternalTools,
    ftp_upload,
    local_copy,
    print_with_sumatra,
    printer_list,
    save_jpg_from_pdf,
    save_pdf,
)


@dataclass
class FileActionStatus:
    file_path: Path
    local_copy: str = "-"
    ftp: str = "-"
    print_status: str = "-"


@dataclass
class JobResult:
    part_name: str
    success: bool
    message: str
    outputs: list[Path]
    details: list[str] = field(default_factory=list)
    file_statuses: list[FileActionStatus] = field(default_factory=list)


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
        details: list[str] = []
        file_statuses: list[FileActionStatus] = []
        tag = f"[{idx}/{total}] {part.part_name}"
        try:
            self._emit("progress", {"value": idx - 1, "total": total, "text": f"開始 {tag}"})
            self._emit("log", {"text": f"開始: {tag}"})
            self._emit("log", {"text": f"使用URL(home): {common.owlview_home_url}"})
            driver.get(common.owlview_home_url)
            time.sleep(common.selenium_wait_sec)
            box = driver.find_element(By.XPATH, common.xpath_input_box)
            box.clear()
            box.send_keys(part.part_name)
            self._emit("log", {"text": f"XPath入力成功: {part.part_name}"})
            time.sleep(common.selenium_wait_sec)

            self._emit("log", {"text": f"使用URL(report): {common.owlview_report_url}"})
            driver.get(common.owlview_report_url)
            time.sleep(common.selenium_wait_sec)
            self._emit("log", {"text": "reportページ遷移成功"})

            stamp = datetime.now().strftime("%y%m%d")
            base = part.resolved_name(stamp)
            out_dir = Path(part.output_dir)
            out_dir.mkdir(parents=True, exist_ok=True)

            pdf_path = out_dir / f"{base}.pdf"
            pdf_generated = False
            if part.output_format in {"pdf", "both", "jpg"}:
                save_pdf(driver, pdf_path, part)
                pdf_generated = True
                self._emit("log", {"text": f"PDF生成成功: {pdf_path}"})
            if part.output_format in {"pdf", "both"} and pdf_generated:
                outputs.append(pdf_path)
                self._emit("log", {"text": f"PDF保存成功: {pdf_path}"})
            if part.output_format in {"jpg", "both"}:
                jpg_path = out_dir / f"{base}.jpg"
                if not pdf_generated:
                    save_pdf(driver, pdf_path, part)
                    pdf_generated = True
                save_jpg_from_pdf(driver, pdf_path, jpg_path, part.jpg_quality)
                outputs.append(jpg_path)
                self._emit("log", {"text": f"JPG保存成功: {jpg_path}"})
                if part.output_format == "jpg" and pdf_generated and pdf_path.exists():
                    pdf_path.unlink(missing_ok=True)
                    self._emit("log", {"text": f"JPG変換用の一時PDFを削除: {pdf_path}"})

            self._emit(
                "log",
                {
                    "text": (
                        f"印刷設定: print_enabled={part.print_enabled}, printer={part.printer_name or '(未設定)'}, "
                        f"copies={part.copies}, sumatra={self.tools.sumatra}"
                    )
                },
            )

            printers = printer_list()

            for p in outputs:
                status = FileActionStatus(file_path=p)

                if part.local_copy_enabled:
                    local_copy(p, common)
                    status.local_copy = "成功"
                    self._emit("log", {"text": f"ローカルコピー成功: {p.name}"})
                else:
                    status.local_copy = "スキップ(OFF)"
                    self._emit("log", {"text": f"ローカルコピー: OFFのためスキップ ({p.name})"})

                if part.ftp_upload_enabled:
                    self._emit("log", {"text": f"FTP開始: {p.name}"})
                    target, curl_result = ftp_upload(p, common, self.tools.curl)
                    status.ftp = f"成功 ({target})"
                    self._emit("log", {"text": f"FTP成功: {p.name} -> {target}"})
                    self._emit("log", {"text": f"FTP command: {curl_result.command_summary}"})
                    if curl_result.stdout:
                        self._emit("log", {"text": f"FTP stdout: {curl_result.stdout}"})
                    if curl_result.stderr:
                        self._emit("log", {"text": f"FTP stderr: {curl_result.stderr}"})
                else:
                    status.ftp = "スキップ(OFF)"
                    self._emit("log", {"text": f"FTP: OFFのためスキップ ({p.name})"})

                if p.suffix.lower() != ".pdf":
                    status.print_status = "スキップ(PDFのみ)"
                    self._emit("log", {"text": f"印刷スキップ: PDF以外 ({p.name})"})
                elif not part.print_enabled:
                    status.print_status = "スキップ(print_enabled=False)"
                    self._emit("log", {"text": f"印刷OFFのためスキップ: {p.name}"})
                elif not part.printer_name:
                    status.print_status = "スキップ(プリンタ未設定)"
                    self._emit("log", {"text": f"印刷スキップ: printer_name 未設定 ({p.name})"})
                elif not self.tools.sumatra.exists():
                    status.print_status = "スキップ(Sumatra未検出)"
                    self._emit("log", {"text": f"印刷スキップ: SumatraPDFが見つかりません ({self.tools.sumatra})"})
                elif printers and part.printer_name not in printers:
                    status.print_status = "失敗(プリンタ未存在)"
                    raise RuntimeError(f"指定プリンタが存在しません: {part.printer_name}")
                else:
                    self._emit("log", {"text": f"印刷開始: {p.name}"})
                    print_with_sumatra(self.tools.sumatra, p, part.printer_name, max(1, part.copies))
                    status.print_status = "成功"
                    self._emit("log", {"text": f"印刷成功: {p.name}"})
                file_statuses.append(status)

            details.append("保存完了")
            self._emit("progress", {"value": idx, "total": total, "text": f"完了 {tag}"})
            self._emit("log", {"text": f"完了: {part.part_name}"})
            return JobResult(part.part_name, True, "ok", outputs, details=details, file_statuses=file_statuses)
        except Exception as exc:
            self._emit("log", {"text": f"失敗: {part.part_name}: {exc}"})
            self._emit("progress", {"value": idx, "total": total, "text": f"失敗 {tag}"})
            return JobResult(part.part_name, False, str(exc), outputs, details=details, file_statuses=file_statuses)

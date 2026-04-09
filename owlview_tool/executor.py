from __future__ import annotations

import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from queue import Queue

from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .models import AppConfig, PartConfig
from .services import (
    ExternalTools,
    convert_pdf_first_page_to_jpg,
    ftp_upload,
    local_copy,
    print_with_sumatra,
    printer_list,
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

    def _wait_ready_state(self, driver, timeout: int, label: str) -> None:
        WebDriverWait(driver, timeout).until(lambda d: d.execute_script("return document.readyState") == "complete")
        self._emit("log", {"text": f"{label}: readyState=complete"})

    def _input_part_name(self, driver, part_name: str) -> None:
        common = self.cfg.common
        timeout = max(1, common.selenium_wait_sec)
        last_exc: Exception | None = None
        for attempt in range(1, 4):
            try:
                self._wait_ready_state(driver, timeout, "homeページ表示後")
                WebDriverWait(driver, timeout).until(EC.visibility_of_element_located((By.XPATH, common.xpath_input_box)))
                WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((By.XPATH, common.xpath_input_box)))
                box = driver.find_element(By.XPATH, common.xpath_input_box)
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", box)
                box.click()
                box.clear()
                box = driver.find_element(By.XPATH, common.xpath_input_box)
                box.send_keys(part_name)
                input_value = (box.get_attribute("value") or "").strip()
                if input_value != part_name:
                    driver.execute_script(
                        "arguments[0].value = arguments[1];"
                        "arguments[0].dispatchEvent(new Event('input', {bubbles: true}));"
                        "arguments[0].dispatchEvent(new Event('change', {bubbles: true}));",
                        box,
                        part_name,
                    )
                    input_value = (box.get_attribute("value") or "").strip()
                box.send_keys(Keys.ENTER)
                if input_value != part_name:
                    raise RuntimeError(f"入力検証NG: value='{input_value}'")
                self._emit("log", {"text": "入力欄取得成功"})
                self._emit("log", {"text": f"パート名入力成功: {part_name}"})
                return
            except StaleElementReferenceException as exc:
                last_exc = exc
                self._emit("log", {"text": f"stale element 発生のため再試行 ({attempt}/3)"})
                time.sleep(0.2)
            except (TimeoutException, RuntimeError) as exc:
                last_exc = exc
                self._emit("log", {"text": f"入力失敗 ({attempt}/3): {exc}"})
                time.sleep(0.2)
                if attempt < 3:
                    continue
                break
        raise RuntimeError(f"入力欄操作に失敗しました: {last_exc}")

    def _navigate_to_report(self, driver) -> None:
        common = self.cfg.common
        timeout = max(1, common.selenium_wait_sec)
        targets = {common.owlview_report_url.rstrip("/")}
        if common.owlview_report_url.endswith("/report"):
            targets.add(common.owlview_report_url[:-6].rstrip("/"))

        last_exc: Exception | None = None
        for attempt in range(1, 4):
            try:
                driver.get(common.owlview_report_url)
                self._wait_ready_state(driver, timeout, "reportページ遷移後")
                current = driver.current_url.rstrip("/")
                if current not in targets:
                    raise RuntimeError(f"URL検証NG: current={driver.current_url}")
                self._emit("log", {"text": f"report遷移成功: {driver.current_url}"})
                return
            except Exception as exc:
                last_exc = exc
                self._emit("log", {"text": f"report遷移失敗 ({attempt}/3): {exc}"})
                if attempt < 3:
                    time.sleep(0.5)
                    continue
        raise RuntimeError(f"reportページ遷移に失敗しました: {last_exc}")

    def run_capture_flow(self, driver, part: PartConfig, preview_mode: bool = False) -> tuple[list[Path], Path | None]:
        common = self.cfg.common
        self._emit("log", {"text": "プレビュー開始" if preview_mode else f"開始: {part.part_name}"})
        driver.get(common.owlview_home_url)
        self._emit("log", {"text": f"home遷移成功: {driver.current_url}"})
        self._input_part_name(driver, part.part_name)
        self._navigate_to_report(driver)

        stamp = datetime.now().strftime("%y%m%d")
        base = part.resolved_name(stamp)
        out_dir = Path(part.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        pdf_path = out_dir / f"{base}.pdf"
        outputs: list[Path] = []

        save_pdf(driver, pdf_path, part)
        self._emit("log", {"text": f"PDF保存成功: {pdf_path}"})

        jpg_path: Path | None = None
        if part.output_format in {"both", "jpg"}:
            jpg_path = out_dir / f"{base}.jpg"
            try:
                convert_pdf_first_page_to_jpg(pdf_path, jpg_path, part.jpg_quality)
                outputs.append(jpg_path)
                self._emit("log", {"text": f"JPG変換成功(1ページ目): {jpg_path}"})
            except Exception as exc:
                self._emit("log", {"text": f"PDF保存は成功 / JPG変換は失敗: {exc}"})
                jpg_path = None

        if part.output_format in {"both", "pdf"}:
            outputs.append(pdf_path)
        elif part.output_format == "jpg":
            if jpg_path and pdf_path.exists():
                pdf_path.unlink(missing_ok=True)
            else:
                outputs.append(pdf_path)
        return outputs, pdf_path

    def _run_part(self, driver, idx: int, total: int, part: PartConfig) -> JobResult:
        common = self.cfg.common
        outputs: list[Path] = []
        details: list[str] = []
        file_statuses: list[FileActionStatus] = []
        tag = f"[{idx}/{total}] {part.part_name}"
        try:
            self._emit("progress", {"value": idx - 1, "total": total, "text": f"開始 {tag}"})
            outputs, _pdf = self.run_capture_flow(driver, part)

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
                    status.print_status = "スキップ(対象ファイルがPDFではない)"
                    self._emit("log", {"text": f"印刷スキップ理由: 対象ファイルが PDF ではない ({p.name})"})
                elif not part.print_enabled:
                    status.print_status = "スキップ(print_enabled=False)"
                    self._emit("log", {"text": f"印刷スキップ理由: print_enabled が false ({p.name})"})
                elif not part.printer_name:
                    status.print_status = "スキップ(プリンタ未設定)"
                    self._emit("log", {"text": f"印刷スキップ理由: printer_name 未設定 ({p.name})"})
                elif not self.tools.sumatra.exists():
                    status.print_status = "スキップ(Sumatra未検出)"
                    self._emit("log", {"text": f"印刷スキップ理由: SumatraPDF 不在 ({self.tools.sumatra})"})
                elif printers and part.printer_name not in printers:
                    status.print_status = "スキップ(指定プリンタ未存在)"
                    self._emit("log", {"text": f"印刷スキップ理由: 指定プリンタ未存在 ({part.printer_name})"})
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
            self._emit("log", {"text": traceback.format_exc()})
            self._emit("progress", {"value": idx, "total": total, "text": f"失敗 {tag}"})
            return JobResult(part.part_name, False, str(exc), outputs, details=details, file_statuses=file_statuses)

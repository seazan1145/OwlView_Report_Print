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
from .services import ExternalTools, convert_pdf_first_page_to_jpg, ftp_upload, local_copy, print_with_sumatra, printer_list, save_pdf


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

    def _new_driver(self):
        opts = Options()
        opts.add_argument("--headless=new")
        return webdriver.Chrome(service=Service(str(self.tools.chromedriver)), options=opts)

    def _run(self, parts: list[PartConfig]) -> None:
        total = len(parts)
        self._emit("start", {"total": total})
        driver = self._new_driver()
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

    def run_preview_capture(self, part: PartConfig, preview_dir: Path) -> tuple[Path, list[Path]]:
        preview_dir.mkdir(parents=True, exist_ok=True)
        tmp = PartConfig(**part.__dict__)
        tmp.output_format = "pdf"
        tmp.output_dir = str(preview_dir)
        tmp.output_name = f"preview_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        driver = self._new_driver()
        try:
            outputs, pdf_path = self.run_capture_flow(driver, tmp, preview_mode=True)
        finally:
            driver.quit()
        if not pdf_path:
            raise RuntimeError("プレビューPDF生成に失敗しました")
        return pdf_path, outputs

    def _wait_ready_state(self, driver, timeout: int, label: str) -> None:
        WebDriverWait(driver, timeout).until(lambda d: d.execute_script("return document.readyState") == "complete")
        self._emit("log", {"text": f"{label}: readyState 完了"})

    def _find_input(self, driver, timeout: int):
        locator = (By.XPATH, self.cfg.common.xpath_input_box)
        WebDriverWait(driver, timeout).until(EC.presence_of_element_located(locator))
        WebDriverWait(driver, timeout).until(EC.visibility_of_element_located(locator))
        WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(locator))
        return driver.find_element(*locator)

    def _input_part_name(self, driver, part_name: str) -> None:
        timeout = max(1, self.cfg.common.selenium_wait_sec)
        last_exc: Exception | None = None
        self._wait_ready_state(driver, timeout, "homeページ遷移成功")
        for attempt in range(1, 4):
            try:
                box = self._find_input(driver, timeout)
                self._emit("log", {"text": "XPath要素取得成功"})
                self._emit("log", {"text": f"要素状態: tag={box.tag_name}, visible={box.is_displayed()}, enabled={box.is_enabled()}"})
                box.clear()
                self._emit("log", {"text": "clear 実行"})
                box.send_keys(part_name)
                self._emit("log", {"text": "send_keys 実行"})
                current = (box.get_attribute("value") or "").strip()
                self._emit("log", {"text": f"入力後value: {current}"})
                if current == part_name:
                    self._emit("log", {"text": f"入力値確認: {part_name}"})
                    box.send_keys(Keys.ENTER)
                    return

                # fallback 1: click + Ctrl+A Delete + send_keys
                box = self._find_input(driver, timeout)
                box.click()
                box.send_keys(Keys.CONTROL, "a")
                box.send_keys(Keys.DELETE)
                box.send_keys(part_name)
                current = (box.get_attribute("value") or "").strip()
                self._emit("log", {"text": f"フォールバック1後value: {current}"})
                if current == part_name:
                    self._emit("log", {"text": f"入力値確認: {part_name}"})
                    box.send_keys(Keys.ENTER)
                    return

                # fallback 2: JS assign + events
                box = self._find_input(driver, timeout)
                driver.execute_script(
                    "arguments[0].value=arguments[1];"
                    "arguments[0].dispatchEvent(new Event('input',{bubbles:true}));"
                    "arguments[0].dispatchEvent(new Event('change',{bubbles:true}));",
                    box,
                    part_name,
                )
                current = (box.get_attribute("value") or "").strip()
                self._emit("log", {"text": f"フォールバック2(JS)後value: {current}"})
                if current == part_name:
                    self._emit("log", {"text": f"入力値確認: {part_name}"})
                    box.send_keys(Keys.ENTER)
                    return

                raise RuntimeError("取得は成功したが値が入らなかった")
            except StaleElementReferenceException as exc:
                last_exc = exc
                self._emit("log", {"text": f"stale element 発生。再取得して再試行 ({attempt}/3)"})
                time.sleep(0.2)
            except (TimeoutException, RuntimeError) as exc:
                last_exc = exc
                self._emit("log", {"text": f"入力処理失敗 ({attempt}/3): {exc}"})
                time.sleep(0.2)
        raise RuntimeError(f"入力欄操作に失敗しました: {last_exc}")

    def _navigate_to_report(self, driver) -> None:
        common = self.cfg.common
        timeout = max(1, common.selenium_wait_sec)
        driver.get(common.owlview_report_url)
        self._wait_ready_state(driver, timeout, "reportページ遷移成功")
        self._emit("log", {"text": f"report遷移成功: {driver.current_url}"})

    def run_capture_flow(self, driver, part: PartConfig, preview_mode: bool = False) -> tuple[list[Path], Path | None]:
        common = self.cfg.common
        self._emit("log", {"text": "プレビュー開始" if preview_mode else f"開始: {part.part_name}"})
        driver.get(common.owlview_home_url)
        self._emit("log", {"text": f"homeページ遷移成功: {driver.current_url}"})
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
        if part.output_format in {"jpg&pdf", "jpg"}:
            jpg_path = out_dir / f"{base}.jpg"
            try:
                convert_pdf_first_page_to_jpg(pdf_path, jpg_path, part.jpg_quality)
                outputs.append(jpg_path)
                self._emit("log", {"text": f"JPG変換成功(1ページ目): {jpg_path}"})
            except Exception as exc:
                self._emit("log", {"text": f"PDF保存は成功 / JPG変換は失敗: {exc}"})
                jpg_path = None

        if part.output_format in {"jpg&pdf", "pdf"}:
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
            self._emit("log", {"text": f"使用プリンタ: {part.printer_name or '(未設定)'}"})
            printers = printer_list()
            for p in outputs:
                status = FileActionStatus(file_path=p)
                if part.local_copy_enabled:
                    local_copy(p, common)
                    status.local_copy = "成功"
                else:
                    status.local_copy = "スキップ(OFF)"

                if part.ftp_upload_enabled:
                    target, _ = ftp_upload(p, common, self.tools.curl)
                    status.ftp = f"成功 ({target})"
                else:
                    status.ftp = "スキップ(OFF)"

                if p.suffix.lower() != ".pdf":
                    status.print_status = "スキップ(PDFのみ)"
                elif not part.print_enabled:
                    status.print_status = "スキップ(OFF)"
                elif not part.printer_name:
                    status.print_status = "スキップ(プリンタ未設定)"
                elif printers and part.printer_name not in printers:
                    status.print_status = "スキップ(指定プリンタ未存在)"
                elif not self.tools.sumatra.exists():
                    status.print_status = "スキップ(Sumatra未検出)"
                else:
                    print_with_sumatra(self.tools.sumatra, p, part.printer_name, max(1, part.copies))
                    status.print_status = "成功"
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

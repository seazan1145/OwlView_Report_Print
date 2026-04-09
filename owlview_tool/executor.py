from __future__ import annotations

import threading
import time
import traceback
from pathlib import Path
from urllib.parse import urlsplit
from dataclasses import dataclass, field
from datetime import datetime
from queue import Queue

from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
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
class PartExecutionSummary:
    part_name: str
    started_at: datetime
    finished_at: datetime | None = None
    capture_stage: str = "-"
    pdf: str = "-"
    jpg: str = "-"
    ftp: str = "-"
    printing: str = "-"
    output_dir: str = ""
    error_summary: str = ""

    @property
    def elapsed_sec(self) -> float:
        end = self.finished_at or datetime.now()
        return max(0.0, (end - self.started_at).total_seconds())


@dataclass
class JobResult:
    part_name: str
    success: bool
    message: str
    outputs: list[Path]
    details: list[str] = field(default_factory=list)
    file_statuses: list[FileActionStatus] = field(default_factory=list)
    summary: PartExecutionSummary | None = None


class Runner:
    REPORT_READY_SELECTORS = [
        "main",
        "[data-testid*='report']",
        "[id*='report']",
        ".report",
    ]

    def __init__(
        self,
        cfg: AppConfig,
        tools: ExternalTools,
        queue: Queue,
        *,
        run_ftp_enabled: bool = False,
        run_print_enabled: bool = False,
        run_printer_name: str = "",
        run_copies: int = 1,
    ):
        self.cfg = cfg
        self.tools = tools
        self.queue = queue
        self.stop_event = threading.Event()
        self.run_ftp_enabled = run_ftp_enabled
        self.run_print_enabled = run_print_enabled
        self.run_printer_name = run_printer_name.strip()
        self.run_copies = max(1, int(run_copies))

    # ========= UI連携 =========
    def stop(self) -> None:
        self.stop_event.set()

    def run_async(self, parts: list[PartConfig]) -> threading.Thread:
        t = threading.Thread(target=self._run, args=(parts,), daemon=True)
        t.start()
        return t

    def _emit(self, kind: str, payload: dict) -> None:
        self.queue.put((kind, payload))

    def _log(self, text: str, *, verbose: bool = False) -> None:
        debug = self.cfg.common.debug
        if verbose and not (debug.enabled and debug.verbose_log):
            return
        self._emit("log", {"text": text})

    # ========= 設定値 =========
    def _wait_timeout(self) -> int:
        debug_timeout = int(self.cfg.common.debug.selenium_wait_timeout)
        if debug_timeout > 0:
            return debug_timeout
        return max(1, self.cfg.common.selenium_wait_sec)

    def _input_settle_wait(self) -> float:
        return max(0.1, float(self.cfg.common.debug.input_settle_wait))

    # ========= エントリポイント =========
    def run_preview(self, part: PartConfig, preview_dir: Path) -> tuple[Path, list[Path]]:
        """run_preview(part, settings): プレビュー用の実行経路。"""
        preview_dir.mkdir(parents=True, exist_ok=True)
        tmp = PartConfig(**part.__dict__)
        tmp.output_format = "pdf"
        tmp.output_dir = str(preview_dir)
        tmp.output_name = f"preview_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        driver = self._new_driver()
        try:
            outputs, pdf_path = self._run_capture_pipeline(driver, tmp, preview_mode=True)
        finally:
            driver.quit()
        if not pdf_path:
            raise RuntimeError("プレビューPDF生成に失敗しました")
        return pdf_path, outputs

    def run_batch(self, parts: list[PartConfig]) -> list[JobResult]:
        """run_batch(parts, settings): 本番バッチ用の実行経路。"""
        results: list[JobResult] = []
        driver = self._new_driver()
        try:
            total = len(parts)
            self._emit("start", {"total": total})
            for idx, part in enumerate(parts, start=1):
                if self.stop_event.is_set():
                    self._log("ユーザーキャンセルにより停止しました。")
                    break
                results.append(self._run_part(driver, idx, total, part))
        finally:
            driver.quit()
        return results

    def _run(self, parts: list[PartConfig]) -> None:
        results = self.run_batch(parts)
        self._emit("done", {"results": results})

    # ========= OwlView操作 =========
    def _new_driver(self):
        opts = Options()
        if self.cfg.common.debug.headless:
            opts.add_argument("--headless=new")
        return webdriver.Chrome(service=Service(str(self.tools.chromedriver)), options=opts)

    def open_home(self, driver) -> None:
        common = self.cfg.common
        driver.get(common.owlview_home_url)
        self._log(f"homeページ遷移成功: {driver.current_url}")
        self._wait_ready_state(driver, self._wait_timeout(), "homeページ遷移成功")

    def select_part(self, driver, part_name: str) -> None:
        self._input_part_name(driver, part_name)

    def open_report(self, driver) -> None:
        self._navigate_to_report(driver)

    def _wait_ready_state(self, driver, timeout: int, label: str) -> None:
        WebDriverWait(driver, timeout).until(lambda d: d.execute_script("return document.readyState") == "complete")
        self._log(f"{label}: readyState 完了", verbose=True)

    @staticmethod
    def _normalize_path(path: str) -> str:
        normalized = (path or "/").strip()
        if not normalized.startswith("/"):
            normalized = f"/{normalized}"
        if normalized != "/" and normalized.endswith("/"):
            normalized = normalized[:-1]
        return normalized

    @classmethod
    def _is_expected_url(cls, current_url: str, expected_url: str) -> bool:
        current = urlsplit(current_url.strip())
        expected = urlsplit(expected_url.strip())
        current_scheme = (current.scheme or "").lower()
        expected_scheme = (expected.scheme or "").lower()
        if not current_scheme or not expected_scheme or current_scheme != expected_scheme:
            return False
        current_host = (current.hostname or "").lower()
        expected_host = (expected.hostname or "").lower()
        if not current_host or not expected_host or current_host != expected_host:
            return False
        if expected.port is not None and current.port != expected.port:
            return False

        expected_path = cls._normalize_path(expected.path)
        current_path = cls._normalize_path(current.path)
        if current_path.startswith(expected_path):
            return True

        fragment = (current.fragment or "").strip()
        if not fragment:
            return False
        fragment_path = fragment.split("?", 1)[0].lstrip("#").strip()
        if not fragment_path:
            return False
        return cls._normalize_path(fragment_path).startswith(expected_path)

    def _wait_url_prefix(self, driver, timeout: int, expected_url: str, label: str) -> None:
        def _matches(d) -> bool:
            return self._is_expected_url(d.current_url, expected_url)

        try:
            WebDriverWait(driver, timeout).until(_matches)
            self._log(f"{label}: URL確認OK ({driver.current_url})", verbose=True)
        except TimeoutException:
            shot, html, snippet = self._capture_debug_artifacts(driver, "wait_url_timeout")
            self._log(f"{label}: URL待機タイムアウト current_url={driver.current_url}")
            if shot:
                self._log(f"timeout時スクリーンショット: {shot}")
            if html:
                self._log(f"timeout時HTML: {html}")
            if snippet:
                self._log(f"page_source抜粋: {snippet}", verbose=True)
            raise

    def _find_input(self, driver, timeout: int):
        locator = (By.XPATH, self.cfg.common.xpath_input_box)
        WebDriverWait(driver, timeout).until(EC.presence_of_element_located(locator))
        WebDriverWait(driver, timeout).until(EC.visibility_of_element_located(locator))
        WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(locator))
        return driver.find_element(*locator)

    def _collect_candidate_texts(self, driver) -> list[str]:
        selectors = ["[role='option']", "[class*='option']", "[class*='suggest']", "li"]
        values: list[str] = []
        for sel in selectors:
            try:
                for el in driver.find_elements(By.CSS_SELECTOR, sel):
                    txt = (el.text or "").strip()
                    if txt:
                        values.append(txt)
            except Exception:
                continue
            if values:
                break
        uniq: list[str] = []
        for text in values:
            if text not in uniq:
                uniq.append(text)
        return uniq

    def _brief_wait_after_input(self, driver, timeout: int, before_url: str) -> None:
        search_xpath = (self.cfg.common.xpath_search_ready or "").strip()

        def _ready(d) -> bool:
            if d.current_url != before_url:
                return True
            if search_xpath:
                try:
                    return len(d.find_elements(By.XPATH, search_xpath)) > 0
                except Exception:
                    return False
            return False

        try:
            WebDriverWait(driver, min(timeout, self._wait_timeout())).until(_ready)
            self._log(f"入力後短時間待機: 反映検知 ({driver.current_url})", verbose=True)
        except TimeoutException:
            self._log(f"入力後短時間待機: 反映検知なし (許容) current_url={driver.current_url}", verbose=True)
        time.sleep(self._input_settle_wait())

    def _input_part_name(self, driver, part_name: str) -> None:
        timeout = self._wait_timeout()
        last_exc: Exception | None = None
        self._log(f"開始時URL: {driver.current_url}", verbose=True)
        self._log(f"入力XPath: {self.cfg.common.xpath_input_box}", verbose=True)
        self._log(f"入力対象part_name: {part_name}")
        self._wait_ready_state(driver, timeout, "homeページ遷移成功")
        for attempt in range(1, 4):
            try:
                box = self._find_input(driver, timeout)
                self._log("XPath要素取得成功", verbose=True)
                box.clear()
                box.send_keys(part_name)
                current = (box.get_attribute("value") or "").strip()
                if current == part_name:
                    candidates = self._collect_candidate_texts(driver)
                    self._log(f"候補一覧: {len(candidates)}件 / 先頭={candidates[:5]}", verbose=True)
                    before_wait = driver.current_url
                    self._brief_wait_after_input(driver, timeout, before_wait)
                    return

                box = self._find_input(driver, timeout)
                box.click()
                box.clear()
                box.send_keys(part_name)
                current = (box.get_attribute("value") or "").strip()
                if current == part_name:
                    before_wait = driver.current_url
                    self._brief_wait_after_input(driver, timeout, before_wait)
                    return

                box = self._find_input(driver, timeout)
                driver.execute_script(
                    "arguments[0].value=arguments[1];"
                    "arguments[0].dispatchEvent(new Event('input',{bubbles:true}));"
                    "arguments[0].dispatchEvent(new Event('change',{bubbles:true}));",
                    box,
                    part_name,
                )
                current = (box.get_attribute("value") or "").strip()
                if current == part_name:
                    before_wait = driver.current_url
                    self._brief_wait_after_input(driver, timeout, before_wait)
                    return

                raise RuntimeError("取得は成功したが値が入らなかった")
            except StaleElementReferenceException as exc:
                last_exc = exc
                self._log(f"stale element 発生。再取得して再試行 ({attempt}/3)", verbose=True)
                time.sleep(0.2)
            except (TimeoutException, RuntimeError) as exc:
                last_exc = exc
                self._log(f"入力処理失敗 ({attempt}/3): {exc}")
                time.sleep(0.2)
        raise RuntimeError(f"入力欄操作に失敗しました: {last_exc}")

    def _wait_report_marker(self, driver, timeout: int) -> None:
        custom_xpath = self.cfg.common.xpath_report_ready.strip()

        def _has_marker(d) -> bool:
            if custom_xpath:
                try:
                    return len(d.find_elements(By.XPATH, custom_xpath)) > 0
                except Exception:
                    return False
            for sel in self.REPORT_READY_SELECTORS:
                try:
                    if d.find_elements(By.CSS_SELECTOR, sel):
                        return True
                except Exception:
                    continue
            return False

        try:
            WebDriverWait(driver, timeout).until(_has_marker)
            self._log("report到達判定要素: 確認OK", verbose=True)
            return
        except TimeoutException:
            if custom_xpath:
                raise

        def _fallback_ready(d) -> bool:
            try:
                body = d.find_element(By.TAG_NAME, "body")
                text = (body.text or "").strip()
                return body.is_displayed() and len(text) > 0
            except Exception:
                return False

        WebDriverWait(driver, min(timeout, 3)).until(_fallback_ready)
        self._log("report到達判定要素: body描画を確認して続行", verbose=True)

    def _navigate_to_report(self, driver) -> None:
        common = self.cfg.common
        timeout = self._wait_timeout()
        if common.debug.report_direct_navigation:
            self._log(f"report遷移開始URL: {driver.current_url}", verbose=True)
            driver.get(common.owlview_report_url)
            self._wait_ready_state(driver, timeout, "reportページ遷移成功")
        self._wait_url_prefix(driver, timeout, common.owlview_report_url, "reportページ遷移成功")
        try:
            self._wait_report_marker(driver, timeout)
        except TimeoutException:
            shot, html, _ = self._capture_debug_artifacts(driver, "report_marker_timeout")
            self._log(f"report要素待機タイムアウト。最終URL: {driver.current_url}")
            if shot:
                self._log(f"timeout時スクリーンショット: {shot}")
            if html:
                self._log(f"timeout時HTML: {html}")
            raise
        self._log(f"report遷移成功: {driver.current_url}")

    # ========= 出力生成 =========
    def export_pdf(self, driver, part: PartConfig) -> tuple[Path, Path]:
        stamp = datetime.now().strftime("%y%m%d")
        base = part.resolved_name(stamp)
        out_dir = Path(part.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        pdf_path = out_dir / f"{base}.pdf"
        save_pdf(driver, pdf_path, part)
        self._log(f"PDF保存成功: {pdf_path}")
        return out_dir, pdf_path

    def export_jpg(self, pdf_path: Path, part: PartConfig) -> Path:
        jpg_path = pdf_path.with_suffix(".jpg")
        convert_pdf_first_page_to_jpg(pdf_path, jpg_path, part.jpg_quality)
        self._log(f"JPG変換成功(1ページ目): {jpg_path}")
        return jpg_path

    # ========= 後処理 =========
    def upload_ftp(self, file_path: Path) -> str:
        target, _ = ftp_upload(file_path, self.cfg.common, self.tools.curl)
        return f"成功 ({target})"

    def print_file(self, file_path: Path) -> str:
        printers = printer_list()
        if file_path.suffix.lower() != ".pdf":
            return "スキップ(PDFのみ)"
        if not self.run_print_enabled:
            return "スキップ(OFF)"
        if not self.run_printer_name:
            return "スキップ(プリンタ未設定)"
        if printers and self.run_printer_name not in printers:
            return "スキップ(指定プリンタ未存在)"
        if not self.tools.sumatra.exists():
            return "スキップ(Sumatra未検出)"
        print_with_sumatra(self.tools.sumatra, file_path, self.run_printer_name, self.run_copies)
        return "成功"

    # ========= 実行フロー =========
    def _run_capture_pipeline(self, driver, part: PartConfig, preview_mode: bool = False) -> tuple[list[Path], Path | None]:
        self._log("プレビュー開始" if preview_mode else f"開始: {part.part_name}")
        self.open_home(driver)
        self.select_part(driver, part.part_name)
        self.open_report(driver)

        outputs: list[Path] = []
        _out_dir, pdf_path = self.export_pdf(driver, part)

        jpg_path: Path | None = None
        if part.output_format in {"jpg&pdf", "jpg"}:
            jpg_path = self.export_jpg(pdf_path, part)
            outputs.append(jpg_path)

        if part.output_format in {"jpg&pdf", "pdf"}:
            outputs.append(pdf_path)
        elif part.output_format == "jpg":
            if jpg_path and pdf_path.exists():
                pdf_path.unlink(missing_ok=True)
            else:
                outputs.append(pdf_path)
        return outputs, pdf_path

    def run_capture_flow(self, driver, part: PartConfig, preview_mode: bool = False) -> tuple[list[Path], Path | None]:
        # 既存呼び出し互換
        return self._run_capture_pipeline(driver, part, preview_mode=preview_mode)

    def _run_part(self, driver, idx: int, total: int, part: PartConfig) -> JobResult:
        common = self.cfg.common
        outputs: list[Path] = []
        details: list[str] = []
        file_statuses: list[FileActionStatus] = []
        tag = f"[{idx}/{total}] {part.part_name}"
        summary = PartExecutionSummary(part_name=part.part_name, started_at=datetime.now(), output_dir=part.output_dir)
        try:
            self._emit("progress", {"value": idx - 1, "total": total, "text": f"開始 {tag}"})
            summary.capture_stage = "OwlView操作"
            outputs, _pdf = self._run_capture_pipeline(driver, part)
            summary.capture_stage = "出力生成"
            summary.pdf = "成功" if any(p.suffix.lower() == ".pdf" for p in outputs) else "未出力"
            summary.jpg = "成功" if any(p.suffix.lower() == ".jpg" for p in outputs) else "未出力"
            self._log(f"使用プリンタ: {self.run_printer_name or '(未設定)'}", verbose=True)
            for p in outputs:
                status = FileActionStatus(file_path=p)
                if part.local_copy_enabled:
                    local_copy(p, common)
                    status.local_copy = "成功"
                else:
                    status.local_copy = "スキップ(OFF)"

                if self.run_ftp_enabled:
                    status.ftp = self.upload_ftp(p)
                else:
                    status.ftp = "スキップ(OFF)"

                status.print_status = self.print_file(p)
                file_statuses.append(status)

            summary.ftp = "成功" if any(s.ftp.startswith("成功") for s in file_statuses) else ("スキップ" if not self.run_ftp_enabled else "失敗")
            summary.printing = "成功" if any(s.print_status == "成功" for s in file_statuses) else ("スキップ" if not self.run_print_enabled else "未実施/スキップ")
            details.append("保存完了")
            self._emit("progress", {"value": idx, "total": total, "text": f"完了 {tag}"})
            summary.finished_at = datetime.now()
            self._emit("part_summary", {"summary": summary})
            self._log(f"完了: {part.part_name}")
            return JobResult(part.part_name, True, "ok", outputs, details=details, file_statuses=file_statuses, summary=summary)
        except Exception as exc:
            summary.finished_at = datetime.now()
            summary.error_summary = str(exc)
            if summary.pdf == "-":
                summary.pdf = "失敗"
            if summary.jpg == "-":
                summary.jpg = "失敗/未実行"
            if summary.ftp == "-":
                summary.ftp = "未実行"
            if summary.printing == "-":
                summary.printing = "未実行"
            self._log(f"失敗: {part.part_name}: {exc}")
            self._log(traceback.format_exc(), verbose=True)
            self._emit("progress", {"value": idx, "total": total, "text": f"失敗 {tag}"})
            self._emit("part_summary", {"summary": summary})
            return JobResult(part.part_name, False, str(exc), outputs, details=details, file_statuses=file_statuses, summary=summary)

    # ========= デバッグ保存 =========
    def _capture_debug_artifacts(self, driver, prefix: str) -> tuple[Path | None, Path | None, str]:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        debug = self.cfg.common.debug
        debug_dir = Path(self.cfg.common.default_output_root or ".") / "Settings" / "_debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        shot: Path | None = None
        html: Path | None = None
        snippet = ""
        if debug.save_screenshot_on_error:
            shot = debug_dir / f"{prefix}_{stamp}.png"
            try:
                driver.save_screenshot(str(shot))
            except Exception:
                shot.write_text("screenshot failed", encoding="utf-8")
        if debug.save_html_on_error:
            html = debug_dir / f"{prefix}_{stamp}.html"
            try:
                source = driver.page_source or ""
                html.write_text(source, encoding="utf-8")
                snippet = " ".join(source[:300].split())
            except Exception:
                html.write_text("page_source failed", encoding="utf-8")
        return shot, html, snippet

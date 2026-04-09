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
class JobResult:
    part_name: str
    success: bool
    message: str
    outputs: list[Path]
    details: list[str] = field(default_factory=list)
    file_statuses: list[FileActionStatus] = field(default_factory=list)


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

    @staticmethod
    def _normalize_url_path(url: str) -> str:
        parts = urlsplit(url.strip())
        path = parts.path or "/"
        if path != "/" and path.endswith("/"):
            path = path[:-1]
        return f"{parts.scheme}://{parts.netloc}{path}"

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

        # report URL 設定側にポート指定がある場合のみ、ポートを厳密比較する。
        # (http->https リダイレクトや既定ポートの省略差異を吸収するため)
        if expected.port is not None and current.port != expected.port:
            return False

        expected_path = cls._normalize_path(expected.path)
        current_path = cls._normalize_path(current.path)
        if current_path.startswith(expected_path):
            return True

        # OwlView が hash routing を使う環境では "/#/report" のように path が fragment 側に入る。
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
            self._emit("log", {"text": f"{label}: URL確認OK ({driver.current_url})"})
        except TimeoutException:
            shot, html, snippet = self._capture_debug_artifacts(driver, "wait_url_timeout")
            self._emit("log", {"text": f"{label}: URL待機タイムアウト current_url={driver.current_url}"})
            self._emit("log", {"text": f"timeout時スクリーンショット: {shot}"})
            self._emit("log", {"text": f"timeout時HTML: {html}"})
            if snippet:
                self._emit("log", {"text": f"page_source抜粋: {snippet}"})
            raise

    def _find_input(self, driver, timeout: int):
        locator = (By.XPATH, self.cfg.common.xpath_input_box)
        WebDriverWait(driver, timeout).until(EC.presence_of_element_located(locator))
        WebDriverWait(driver, timeout).until(EC.visibility_of_element_located(locator))
        WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(locator))
        return driver.find_element(*locator)

    def _collect_candidate_texts(self, driver) -> list[str]:
        selectors = [
            "[role='option']",
            "[class*='option']",
            "[class*='suggest']",
            "li",
        ]
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
            WebDriverWait(driver, min(timeout, 2)).until(_ready)
            self._emit("log", {"text": f"入力後短時間待機: 反映検知 ({driver.current_url})"})
        except TimeoutException:
            self._emit("log", {"text": f"入力後短時間待機: 反映検知なし (許容) current_url={driver.current_url}"})

    def _input_part_name(self, driver, part_name: str) -> None:
        timeout = max(1, self.cfg.common.selenium_wait_sec)
        last_exc: Exception | None = None
        self._emit("log", {"text": f"開始時URL: {driver.current_url}"})
        self._emit("log", {"text": f"入力XPath: {self.cfg.common.xpath_input_box}"})
        self._emit("log", {"text": f"入力対象part_name: {part_name}"})
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
                    candidates = self._collect_candidate_texts(driver)
                    self._emit("log", {"text": f"候補一覧: {len(candidates)}件 / 先頭={candidates[:5]}"})
                    before_wait = driver.current_url
                    self._brief_wait_after_input(driver, timeout, before_wait)
                    return

                # fallback 1: click + Ctrl+A Delete + send_keys
                box = self._find_input(driver, timeout)
                box.click()
                box.clear()
                box.send_keys(part_name)
                current = (box.get_attribute("value") or "").strip()
                self._emit("log", {"text": f"フォールバック1後value: {current}"})
                if current == part_name:
                    self._emit("log", {"text": f"入力値確認: {part_name}"})
                    candidates = self._collect_candidate_texts(driver)
                    self._emit("log", {"text": f"候補一覧: {len(candidates)}件 / 先頭={candidates[:5]}"})
                    before_wait = driver.current_url
                    self._brief_wait_after_input(driver, timeout, before_wait)
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
                    candidates = self._collect_candidate_texts(driver)
                    self._emit("log", {"text": f"候補一覧: {len(candidates)}件 / 先頭={candidates[:5]}"})
                    before_wait = driver.current_url
                    self._brief_wait_after_input(driver, timeout, before_wait)
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

    def _capture_debug_artifacts(self, driver, prefix: str) -> tuple[Path, Path, str]:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        debug_dir = Path(self.cfg.common.default_output_root or ".") / "Settings" / "_debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        shot = debug_dir / f"{prefix}_{stamp}.png"
        html = debug_dir / f"{prefix}_{stamp}.html"
        snippet = ""
        try:
            driver.save_screenshot(str(shot))
        except Exception:
            shot.write_text("screenshot failed", encoding="utf-8")
        try:
            source = driver.page_source or ""
            html.write_text(source, encoding="utf-8")
            snippet = " ".join(source[:300].split())
        except Exception:
            html.write_text("page_source failed", encoding="utf-8")
        return shot, html, snippet

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
            self._emit("log", {"text": "report到達判定要素: 確認OK"})
            return
        except TimeoutException:
            # custom XPath が設定されている場合は、明示指定された判定を優先して失敗扱いにする。
            if custom_xpath:
                raise

        # 既定セレクタで判定できないページ向けのフォールバック。
        # URL/readyState は事前に確認済みのため、body が描画済みなら続行する。
        def _fallback_ready(d) -> bool:
            try:
                body = d.find_element(By.TAG_NAME, "body")
                text = (body.text or "").strip()
                return body.is_displayed() and len(text) > 0
            except Exception:
                return False

        try:
            WebDriverWait(driver, min(timeout, 3)).until(_fallback_ready)
            self._emit(
                "log",
                {"text": "report到達判定要素: 既定セレクタ未検出のためbody描画を確認して続行"},
            )
        except TimeoutException:
            raise TimeoutException("report到達判定要素を検出できず、body描画確認も失敗しました")

    def _navigate_to_report(self, driver) -> None:
        common = self.cfg.common
        timeout = max(1, common.selenium_wait_sec)
        self._emit("log", {"text": f"report遷移開始URL: {driver.current_url}"})
        driver.get(common.owlview_report_url)
        self._wait_ready_state(driver, timeout, "reportページ遷移成功")
        self._wait_url_prefix(driver, timeout, common.owlview_report_url, "reportページ遷移成功")
        try:
            self._wait_report_marker(driver, timeout)
        except TimeoutException:
            shot, html, _ = self._capture_debug_artifacts(driver, "report_marker_timeout")
            self._emit("log", {"text": f"report要素待機タイムアウト。最終URL: {driver.current_url}"})
            self._emit("log", {"text": f"timeout時スクリーンショット: {shot}"})
            self._emit("log", {"text": f"timeout時HTML: {html}"})
            raise
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
            self._emit("log", {"text": f"使用プリンタ: {self.run_printer_name or '(未設定)'}"})
            printers = printer_list()
            for p in outputs:
                status = FileActionStatus(file_path=p)
                if part.local_copy_enabled:
                    local_copy(p, common)
                    status.local_copy = "成功"
                else:
                    status.local_copy = "スキップ(OFF)"

                if self.run_ftp_enabled:
                    target, _ = ftp_upload(p, common, self.tools.curl)
                    status.ftp = f"成功 ({target})"
                else:
                    status.ftp = "スキップ(OFF)"

                if p.suffix.lower() != ".pdf":
                    status.print_status = "スキップ(PDFのみ)"
                elif not self.run_print_enabled:
                    status.print_status = "スキップ(OFF)"
                elif not self.run_printer_name:
                    status.print_status = "スキップ(プリンタ未設定)"
                elif printers and self.run_printer_name not in printers:
                    status.print_status = "スキップ(指定プリンタ未存在)"
                elif not self.tools.sumatra.exists():
                    status.print_status = "スキップ(Sumatra未検出)"
                else:
                    print_with_sumatra(self.tools.sumatra, p, self.run_printer_name, self.run_copies)
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

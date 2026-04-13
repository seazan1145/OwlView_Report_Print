from __future__ import annotations

import threading
import time
import traceback
import tempfile
from pathlib import Path
from urllib.parse import urlsplit
from dataclasses import dataclass, field
from datetime import datetime
from queue import Queue

from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .models import AppConfig, PartConfig
from .services import ExternalTools, convert_pdf_first_page_to_jpg, ftp_upload, local_copy, print_with_sumatra, printer_list, sanitize_filename, save_inputtable_excel, save_pdf


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
        excel_only_mode: bool = False,
    ):
        self.cfg = cfg
        self.tools = tools
        self.queue = queue
        self.stop_event = threading.Event()
        self.run_ftp_enabled = run_ftp_enabled
        self.run_print_enabled = run_print_enabled
        self.run_printer_name = run_printer_name.strip()
        self.run_copies = max(1, int(run_copies))
        self.excel_only_mode = bool(excel_only_mode)

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
        try:
            return webdriver.Chrome(options=opts)
        except WebDriverException as exc:
            message = (
                "Chrome WebDriverの起動に失敗しました。"
                "Selenium Managerによる自動解決に失敗した可能性があります。\n"
                "確認ポイント: Chromeのインストール状態 / Seleniumパッケージ / 社内ネットワーク制限 / プロキシ設定。\n"
                f"詳細: {exc}"
            )
            self._log(message)
            raise RuntimeError(message) from exc

    def open_home(self, driver) -> None:
        common = self.cfg.common
        driver.get(common.owlview_home_url)
        self._log(f"homeページ遷移成功: {driver.current_url}")
        self._wait_ready_state(driver, self._wait_timeout(), "homeページ遷移成功")

    def select_part(self, driver, part_name: str) -> None:
        self._input_part_name(driver, part_name)

    def open_report(self, driver) -> None:
        self._navigate_to_report(driver)

    def _inputtable_url(self) -> str:
        return f"{self.cfg.common.owlview_home_url.rstrip('/')}/inputtable"

    def _resolve_excel_output_dir(self, part: PartConfig) -> Path:
        raw = (part.inputtable_excel_output_dir or part.output_dir or ".").strip()
        token = datetime.now().strftime("%y%m%d")
        resolved = raw.replace("yymmdd", token)
        return Path(resolved)

    def _build_excel_filename(self, payload: dict) -> str:
        today = datetime.now().strftime("%Y%m%d")
        project = sanitize_filename(str(payload.get("project", "")))
        episode = sanitize_filename(str(payload.get("episode", "")))
        base = "owlview_export"
        if project:
            base += f"_{project}"
        if episode:
            base += f"_{episode}"
        base += f"_{today}"
        return sanitize_filename(base) + ".xlsx"

    def _wait_inputtable_grid_ready(self, driver, timeout: int) -> None:
        def _ready(d) -> bool:
            if "/inputtable" not in d.current_url:
                return False
            try:
                grid = d.find_elements(By.CSS_SELECTOR, "#grid")
                if not grid:
                    return False
                if d.find_elements(By.CSS_SELECTOR, "#grid .ht_master tbody tr"):
                    return True
                if d.find_elements(By.CSS_SELECTOR, "#grid thead th"):
                    return True
                if d.find_elements(By.CSS_SELECTOR, "#grid table") or d.find_elements(By.CSS_SELECTOR, "#grid .handsontable") or d.find_elements(By.CSS_SELECTOR, "#grid .ht_master"):
                    return True
                return False
            except Exception:
                return False

        WebDriverWait(driver, timeout).until(_ready)

    def _inputtable_dom_stats(self, driver) -> dict:
        script = """
const q = (s) => document.querySelector(s);
const qa = (s) => document.querySelectorAll(s).length;
return {
  url: location.href,
  has_grid: !!q('#grid'),
  has_ht_master: !!q('#grid .ht_master'),
  has_handsontable: !!q('#grid .handsontable'),
  thead_tr_count: qa('#grid thead tr'),
  tbody_tr_count: qa('#grid tbody tr'),
};
"""
        try:
            result = driver.execute_script(script)
            return result if isinstance(result, dict) else {}
        except Exception:
            return {}

    def _extract_inputtable_payload(self, driver) -> dict:
        js = r"""
const strip = (s) => {
  if (s == null) return '';
  const str = String(s);
  if (!/[<>]/.test(str)) return str.replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim();
  const div = document.createElement('div');
  div.innerHTML = str;
  return (div.textContent || '').replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim();
};
const getText = (sel) => {
  const el = document.querySelector(sel);
  return el ? (el.textContent || '').trim() : '';
};
const buildMatrixFromRows = (rows, colCountHint = 0) => {
  const parsed = rows.map(cells => cells.map(c => ({
    text: strip(c.text || ''),
    colspan: Math.max(1, parseInt(c.colspan || 1, 10) || 1),
    rowspan: Math.max(1, parseInt(c.rowspan || 1, 10) || 1),
  })));
  let colCount = Math.max(0, colCountHint);
  for (const row of parsed) {
    const width = row.reduce((s, c) => s + c.colspan, 0);
    if (width > colCount) colCount = width;
  }
  if (!colCount) return { matrix: [], merges: [], colCount: 0 };
  const matrix = Array.from({ length: parsed.length }, () => Array(colCount).fill(''));
  const occ = Array.from({ length: parsed.length }, () => Array(colCount).fill(false));
  const merges = [];
  for (let r = 0; r < parsed.length; r++) {
    let c = 0;
    for (const cell of parsed[r]) {
      while (c < colCount && occ[r][c]) c++;
      if (c >= colCount) break;
      const colspan = Math.max(1, Math.min(colCount - c, cell.colspan));
      const rowspan = Math.max(1, Math.min(parsed.length - r, cell.rowspan));
      matrix[r][c] = cell.text;
      for (let rr = 0; rr < rowspan; rr++) {
        for (let cc = 0; cc < colspan; cc++) {
          occ[r + rr][c + cc] = true;
          if (rr !== 0 || cc !== 0) matrix[r + rr][c + cc] = '';
        }
      }
      if (rowspan > 1 || colspan > 1) {
        merges.push({ s: { r, c }, e: { r: r + rowspan - 1, c: c + colspan - 1 } });
      }
      c += colspan;
    }
  }
  return { matrix, merges, colCount };
};
const hotTrace = [];
const findHot = () => {
  const grid = document.querySelector('#grid');
  if (!grid) {
    hotTrace.push('findHot: #grid not found');
    return null;
  }
  const HT = window.Handsontable;
  hotTrace.push(`findHot: window.Handsontable exists = ${!!HT}`);
  hotTrace.push(`findHot: typeof Handsontable.getInstance = ${HT ? typeof HT.getInstance : 'undefined'}`);
  const candidates = [
    grid,
    grid.querySelector('.handsontable'),
    grid.querySelector('.ht_master'),
    grid.querySelector('.ht_master .handsontable'),
    grid.querySelector('table.htCore'),
    ...Array.from(grid.querySelectorAll('.handsontable')),
    ...Array.from(grid.querySelectorAll('.ht_master')),
  ].filter(Boolean);
  const label = (el) => {
    try { return el.id ? `#${el.id}` : (el.className ? `.${String(el.className).split(' ').filter(Boolean).join('.')}` : el.tagName); }
    catch (_) { return 'el'; }
  };
  if (HT && typeof HT.getInstance === 'function') {
    for (const el of candidates) {
      try {
        const i = HT.getInstance(el);
        if (i && typeof i.getData === 'function') {
          hotTrace.push(`findHot: Handsontable.getInstance(${label(el)}) -> success`);
          return i;
        }
        hotTrace.push(`findHot: Handsontable.getInstance(${label(el)}) -> none`);
      } catch (e) { hotTrace.push(`findHot: Handsontable.getInstance(${label(el)}) -> fail`); }
    }
  } else {
    hotTrace.push('findHot: window.Handsontable.getInstance unavailable');
  }
  if (HT && HT.Core && typeof HT.Core.getInstance === 'function') {
    for (const el of candidates) {
      try {
        const i = HT.Core.getInstance(el);
        if (i && typeof i.getData === 'function') {
          hotTrace.push(`findHot: Handsontable.Core.getInstance(${label(el)}) -> success`);
          return i;
        }
        hotTrace.push(`findHot: Handsontable.Core.getInstance(${label(el)}) -> none`);
      } catch (_) { hotTrace.push(`findHot: Handsontable.Core.getInstance(${label(el)}) -> fail`); }
    }
  }
  const $ = window.jQuery || window.$;
  if ($) {
    for (const el of candidates) {
      try {
        if (typeof $(el).handsontable === 'function') {
          const i = $(el).handsontable('getInstance');
          if (i && typeof i.getData === 'function') {
            hotTrace.push(`findHot: jQuery.handsontable(getInstance ${label(el)}) -> success`);
            return i;
          }
        }
      } catch (_) { hotTrace.push(`findHot: jQuery.handsontable(getInstance ${label(el)}) -> fail`); }
      try {
        const i = $(el).data('handsontable');
        if (i && typeof i.getData === 'function') {
          hotTrace.push(`findHot: jQuery.data('handsontable' ${label(el)}) -> success`);
          return i;
        }
      } catch (_) { hotTrace.push(`findHot: jQuery.data('handsontable' ${label(el)}) -> fail`); }
    }
  }
  for (const key of Object.keys(window)) {
    try {
      const v = window[key];
      if (!v || typeof v !== 'object') continue;
      if (typeof v.getData === 'function' && v.rootElement && grid.contains(v.rootElement)) {
        hotTrace.push(`findHot: window scan -> success (${key})`);
        return v;
      }
      for (const subKey of Object.keys(v).slice(0, 80)) {
        try {
          const sub = v[subKey];
          if (!sub || typeof sub !== 'object') continue;
          if (typeof sub.getData === 'function' && sub.rootElement && grid.contains(sub.rootElement)) {
            hotTrace.push(`findHot: nested window scan -> success (${key}.${subKey})`);
            return sub;
          }
        } catch (_) {}
      }
    } catch (_) {}
  }
  hotTrace.push('findHot: all strategies failed');
  return null;
};
const buildDomOnlyPayload = () => {
  const grid = document.querySelector('#grid');
  if (!grid) return null;
  const thRowsRaw = Array.from(grid.querySelectorAll('.ht_clone_top thead tr, .ht_master thead tr'))
    .filter(tr => !tr.querySelector('input,select,textarea,button'));
  const thRows = thRowsRaw.map(tr =>
    Array.from(tr.children)
      .filter(th => th.tagName === 'TH' && !((th.className || '').includes('rowHeader')) && !((th.className || '').includes('cornerHeader')))
      .map(th => ({
        text: (th.querySelector('span.colHeader') || th).textContent || '',
        colspan: th.getAttribute('colspan') || 1,
        rowspan: th.getAttribute('rowspan') || 1,
      }))
  ).filter(r => r.length > 0);
  const bodyRowsRaw = Array.from(grid.querySelectorAll('.ht_master tbody tr'));
  const tdRows = bodyRowsRaw.map(tr =>
    Array.from(tr.children)
      .filter(td => td.tagName === 'TD' && !((td.className || '').includes('rowHeader')))
      .map(td => ({
        text: td.textContent || '',
        colspan: td.getAttribute('colspan') || 1,
        rowspan: td.getAttribute('rowspan') || 1,
      }))
  ).filter(r => r.length > 0);
  if (!thRows.length && !tdRows.length) return null;
  const head = buildMatrixFromRows(thRows);
  const body = buildMatrixFromRows(tdRows, head.colCount);
  const merged = [...head.matrix, ...body.matrix];
  const merges = [...head.merges];
  for (const m of body.merges) {
    merges.push({ s: { r: m.s.r + head.matrix.length, c: m.s.c }, e: { r: m.e.r + head.matrix.length, c: m.e.c } });
  }
  const flat = merged.map(r => r.slice());
  for (const m of merges) {
    const v = (flat[m.s.r] && flat[m.s.r][m.s.c]) || '';
    for (let rr = m.s.r; rr <= m.e.r; rr++) {
      if (!flat[rr]) continue;
      for (let cc = m.s.c; cc <= m.e.c; cc++) {
        if (flat[rr][cc] == null || flat[rr][cc] === '') flat[rr][cc] = v;
      }
    }
  }
  return {
    project: getText('.HeaderCommonProjectName span:nth-of-type(2)'),
    episode: getText('.HeaderCommonEpisodeName span:nth-of-type(2)'),
    merged_sheet: merged,
    flat_sheet: flat,
    merges,
    warning: 'Handsontable未取得のためDOMフォールバックを使用 (partial extraction suspected)',
    hot_trace: hotTrace,
    extraction_mode: 'dom_fallback',
    visible_row_count: merged.length,
    visible_col_count: Math.max(0, ...merged.map(r => r.length)),
  };
};
const hot = findHot();
if (!hot) {
  const fallback = buildDomOnlyPayload();
  if (fallback) return fallback;
  return { error: 'Handsontable インスタンス取得失敗', hot_trace: hotTrace };
}
const data = (typeof hot.getData === 'function') ? (hot.getData() || []) : [];
const colCount = (typeof hot.countCols === 'function') ? hot.countCols() : ((data[0] || []).length);
const rowCount = (typeof hot.countRows === 'function') ? hot.countRows() : data.length;
const nestedHeaders = (() => { try { return (hot.getSettings && hot.getSettings().nestedHeaders) || null; } catch (_) { return null; } })();
const headerRows = [];
const headerMerges = [];
if (Array.isArray(nestedHeaders) && nestedHeaders.length) {
  const norm = nestedHeaders.map(row => row.map(cell => {
    if (cell == null) return { text: '', colspan: 1, rowspan: 1 };
    if (typeof cell === 'string' || typeof cell === 'number') return { text: String(cell), colspan: 1, rowspan: 1 };
    return { text: cell.label ?? cell.title ?? cell.name ?? '', colspan: cell.colspan ?? 1, rowspan: cell.rowspan ?? 1 };
  }));
  const built = buildMatrixFromRows(norm, colCount);
  for (const r of built.matrix) headerRows.push(r);
  for (const m of built.merges) headerMerges.push(m);
}
const thead = document.querySelector('#grid .ht_clone_top thead') || document.querySelector('#grid .ht_master thead');
if (thead && !headerRows.length) {
  const trs = Array.from(thead.querySelectorAll('tr')).filter(tr => !tr.querySelector('input,select,textarea,button'));
  const occ = Array.from({length: trs.length}, () => Array(colCount).fill(false));
  for (let r=0; r<trs.length; r++) {
    const row = Array(colCount).fill('');
    let c = 0;
    const ths = Array.from(trs[r].children).filter(th => th.tagName === 'TH' && !(th.className||'').includes('rowHeader') && !(th.className||'').includes('cornerHeader'));
    for (const th of ths) {
      while (c < colCount && occ[r][c]) c++;
      if (c >= colCount) break;
      const colspan = Math.max(1, Math.min(colCount - c, parseInt(th.getAttribute('colspan') || '1', 10) || 1));
      const rowspan = Math.max(1, Math.min(trs.length - r, parseInt(th.getAttribute('rowspan') || '1', 10) || 1));
      const label = strip((th.querySelector('span.colHeader') || th).textContent || '');
      row[c] = label;
      for (let rr = 0; rr < rowspan; rr++) {
        for (let cc = 0; cc < colspan; cc++) {
          occ[r+rr][c+cc] = true;
        }
      }
      if (rowspan > 1 || colspan > 1) {
        headerMerges.push({ s: { r, c }, e: { r: r + rowspan - 1, c: c + colspan - 1 } });
      }
      c += colspan;
    }
    headerRows.push(row);
  }
}
if (!headerRows.length) {
  const row = [];
  for (let c = 0; c < colCount; c++) {
    try { row.push(strip(hot.getColHeader(c) || '')); } catch (_) { row.push(''); }
  }
  headerRows.push(row);
}
const aoa = [...headerRows.map(r => r.slice())];
for (let r = 0; r < data.length; r++) {
  const row = Array.isArray(data[r]) ? data[r].slice(0, colCount) : [];
  while (row.length < colCount) row.push('');
  aoa.push(row.map(v => strip(v)));
}
let bodyMerges = [];
try {
  const st = (typeof hot.getSettings === 'function') ? hot.getSettings() : null;
  if (Array.isArray(st && st.mergeCells)) {
    bodyMerges = st.mergeCells.map(m => ({ row: parseInt(m.row,10), col: parseInt(m.col,10), rowspan: parseInt(m.rowspan||1,10)||1, colspan: parseInt(m.colspan||1,10)||1 }));
  } else if (typeof hot.getPlugin === 'function') {
    const p = hot.getPlugin('mergeCells');
    const list = p?.mergedCellsCollection?.mergedCells || p?.mergedCellsCollection?.mergedCellsArray || p?.mergedCellsCollection?.items || [];
    bodyMerges = Array.isArray(list) ? list.map(m => ({ row: parseInt(m.row,10), col: parseInt(m.col,10), rowspan: parseInt(m.rowspan||1,10)||1, colspan: parseInt(m.colspan||1,10)||1 })) : [];
  }
} catch (_) {}
const allMerges = [...headerMerges];
const headerOffset = headerRows.length;
for (const m of bodyMerges) {
  if (!Number.isFinite(m.row) || !Number.isFinite(m.col)) continue;
  allMerges.push({ s: { r: headerOffset + m.row, c: m.col }, e: { r: headerOffset + m.row + Math.max(1,m.rowspan)-1, c: m.col + Math.max(1,m.colspan)-1 } });
}
const flat = aoa.map(r => r.slice());
for (const m of allMerges) {
  const v = (flat[m.s.r] && flat[m.s.r][m.s.c]) || '';
  for (let rr = m.s.r; rr <= m.e.r; rr++) {
    if (!flat[rr]) continue;
    for (let cc = m.s.c; cc <= m.e.c; cc++) {
      if (flat[rr][cc] == null || flat[rr][cc] === '') flat[rr][cc] = v;
    }
  }
}
return {
  project: getText('.HeaderCommonProjectName span:nth-of-type(2)'),
  episode: getText('.HeaderCommonEpisodeName span:nth-of-type(2)'),
  merged_sheet: aoa,
  flat_sheet: flat,
  merges: allMerges,
  hot_trace: hotTrace,
  extraction_mode: 'handsontable',
  hot_row_count: rowCount,
  hot_col_count: colCount,
  data_row_count: data.length,
  data_col_count: Math.max(0, ...data.map(r => Array.isArray(r) ? r.length : 0)),
  merge_count: allMerges.length,
};
"""
        payload = driver.execute_script(js)
        if not isinstance(payload, dict):
            raise RuntimeError("inputtableデータ抽出結果が不正です")
        if payload.get("error"):
            raise RuntimeError(str(payload.get("error")))
        trace = payload.get("hot_trace")
        if isinstance(trace, list):
            for line in trace[:40]:
                self._log(str(line))
        if payload.get("warning"):
            self._log(f"inputtable前処理: {payload.get('warning')}")
        self._log(
            f"inputtable抽出モード={payload.get('extraction_mode')} "
            f"rows={payload.get('hot_row_count', payload.get('visible_row_count', 0))} "
            f"cols={payload.get('hot_col_count', payload.get('visible_col_count', 0))} "
            f"merge={payload.get('merge_count', 0)}",
        )
        if payload.get("extraction_mode") == "dom_fallback":
            self._log("inputtable前処理: DOMフォールバックは可視範囲のみの可能性あり (partial extraction suspected)")
        return payload

    def _run_inputtable_export_if_enabled(self, driver, part: PartConfig, *, force: bool = False, continue_on_error: bool = True) -> Path | None:
        if not force and not part.enable_inputtable_excel_export:
            return None
        output_dir = self._resolve_excel_output_dir(part)
        self._log(f"inputtable前処理: 開始 ({part.part_name})")
        try:
            driver.get(self._inputtable_url())
            self._wait_ready_state(driver, self._wait_timeout(), "inputtable遷移")
            self._wait_inputtable_grid_ready(driver, self._wait_timeout())
            time.sleep(0.3)
            stats = self._inputtable_dom_stats(driver)
            self._log(f"inputtable状態: {stats}")
            payload: dict | None = None
            last_error: Exception | None = None
            retry_waits = [0.5, 1.0, 1.5]
            for attempt in range(1, 4):
                try:
                    payload = self._extract_inputtable_payload(driver)
                    if not payload.get("merged_sheet"):
                        raise RuntimeError("DOM抽出結果が空")
                    break
                except Exception as exc:
                    last_error = exc
                    self._log(f"inputtable抽出リトライ {attempt}/3 失敗: {exc}")
                    time.sleep(retry_waits[attempt - 1])
            if not payload:
                raise RuntimeError(f"inputtable抽出失敗: {last_error}")
            out_path = output_dir / self._build_excel_filename(payload)
            self._log(f"inputtable出力先: {out_path}")
            merged_rows = payload.get("merged_sheet", [])
            max_cols = max((len(r) for r in merged_rows), default=0) if isinstance(merged_rows, list) else 0
            self._log(f"inputtable最終出力サイズ: rows={len(merged_rows) if isinstance(merged_rows, list) else 0}, cols={max_cols}")
            save_inputtable_excel(
                output_path=out_path,
                merged_sheet=payload.get("merged_sheet", []),
                merged_ranges=payload.get("merges", []),
                flat_sheet=payload.get("flat_sheet", []),
            )
            self._log(f"inputtable Excel保存成功: {out_path}")
            return out_path
        except Exception as exc:
            self._log(f"inputtable前処理失敗(続行): {exc} / current_url={driver.current_url}")
            if not continue_on_error:
                raise
            return None

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

    def export_jpg(self, pdf_path: Path, part: PartConfig, jpg_path: Path | None = None) -> Path:
        if jpg_path is None:
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
        if not preview_mode:
            self._run_inputtable_export_if_enabled(driver, part)
        self.open_report(driver)

        outputs: list[Path] = []
        temp_pdf_path: Path | None = None
        if part.output_format == "jpg":
            _out_dir = Path(part.output_dir)
            _out_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now().strftime("%y%m%d")
            temp_base = part.resolved_name(stamp)
            with tempfile.NamedTemporaryFile(
                prefix=f".{sanitize_filename(temp_base)}_",
                suffix=".pdf",
                dir=_out_dir,
                delete=False,
            ) as tmp_pdf:
                temp_pdf_path = Path(tmp_pdf.name)
            save_pdf(driver, temp_pdf_path, part)
            pdf_path = temp_pdf_path
            jpg_path = _out_dir / f"{temp_base}.jpg"
        else:
            _out_dir, pdf_path = self.export_pdf(driver, part)
            jpg_path = None

        if part.output_format in {"jpg&pdf", "jpg"}:
            jpg_path = self.export_jpg(pdf_path, part, jpg_path=jpg_path)
            outputs.append(jpg_path)

        if part.output_format in {"jpg&pdf", "pdf"}:
            outputs.append(pdf_path)
        elif part.output_format == "jpg" and not jpg_path:
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
            if self.excel_only_mode:
                self._run_inputtable_export_if_enabled(driver, part, force=True, continue_on_error=False)
                summary.pdf = "スキップ(excel only)"
                summary.jpg = "スキップ(excel only)"
                summary.ftp = "スキップ(excel only)"
                summary.printing = "スキップ(excel only)"
                summary.finished_at = datetime.now()
                self._emit("progress", {"value": idx, "total": total, "text": f"完了 {tag}"})
                self._emit("part_summary", {"summary": summary})
                return JobResult(part.part_name, True, "excel_only_done", [], details=["inputtable Excelのみ実行"], file_statuses=[], summary=summary)
            summary.capture_stage = "OwlView操作"
            outputs, _pdf = self._run_capture_pipeline(driver, part)
            summary.capture_stage = "出力生成"
            summary.pdf = "成功" if any(p.suffix.lower() == ".pdf" for p in outputs) else "未出力"
            summary.jpg = "成功" if any(p.suffix.lower() == ".jpg" for p in outputs) else "未出力"
            self._log(f"使用プリンタ: {self.run_printer_name or '(未設定)'}", verbose=True)
            part_copies = max(1, int(part.print_copies)) if int(part.print_copies) > 0 else self.run_copies
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

                original_copies = self.run_copies
                self.run_copies = part_copies
                status.print_status = self.print_file(p)
                self.run_copies = original_copies
                file_statuses.append(status)

            if self.run_print_enabled and _pdf and _pdf.exists() and not any(s.print_status == "成功" for s in file_statuses):
                original_copies = self.run_copies
                self.run_copies = part_copies
                print_status = self.print_file(_pdf)
                self.run_copies = original_copies
                file_statuses.append(
                    FileActionStatus(
                        file_path=_pdf,
                        local_copy="スキップ(印刷専用)",
                        ftp="スキップ(印刷専用)",
                        print_status=print_status,
                    )
                )

            summary.ftp = "成功" if any(s.ftp.startswith("成功") for s in file_statuses) else ("スキップ" if not self.run_ftp_enabled else "失敗")
            summary.printing = "成功" if any(s.print_status == "成功" for s in file_statuses) else ("スキップ" if not self.run_print_enabled else "未実施/スキップ")
            if part.output_format == "jpg" and _pdf and _pdf.exists():
                _pdf.unlink(missing_ok=True)
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

from __future__ import annotations

import os
import traceback
import tkinter as tk
from datetime import datetime
from pathlib import Path
from queue import Empty, Queue
from tkinter import filedialog, messagebox, simpledialog, ttk

from PIL import Image, ImageTk

from .config_store import ConfigStore
from .executor import Runner
from .models import AppConfig, PartConfig
from .services import (
    ExternalTools,
    ftp_test_connection,
    printer_list,
    render_pdf_first_page_image,
    resolve_tool_path,
    resolved_remote_path,
    validate_ftp_path_template,
)


class OwlViewApp:
    def __init__(self, root: tk.Tk, base_dir: Path) -> None:
        self.root = root
        self.base_dir = base_dir
        self.store = ConfigStore(base_dir)
        self.cfg: AppConfig = self.store.load()
        self.queue: Queue = Queue()
        self.runner: Runner | None = None
        self.search_var = tk.StringVar()
        self.status_var = tk.StringVar(value="Ready")
        self.progress_var = tk.DoubleVar(value=0)
        self.selected_ids: list[int] = []
        self.main_ftp_var = tk.BooleanVar(value=True)
        self.main_print_var = tk.BooleanVar(value=True)
        self.main_printer_var = tk.StringVar(value=self.cfg.common.default_printer_name)
        self.preview_window: tk.Toplevel | None = None
        self.preview_label: ttk.Label | None = None
        self.preview_status = tk.StringVar(value="プレビュー未表示")
        self.preview_scale = 1.0
        self.preview_source: Path | None = None
        self.preview_image: Image.Image | None = None
        self.preview_photo: ImageTk.PhotoImage | None = None
        self.tools = self._resolve_tools()

        self._build_ui()
        self._refresh_printer_combo()
        self._refresh_part_list()
        self._poll_queue()
        self._startup_external_tool_check()

    def _resolve_tools(self) -> ExternalTools:
        data = self.base_dir / "Data"
        c = self.cfg.common
        return ExternalTools(
            chromedriver=resolve_tool_path(c.chromedriver_path, data / "chromedriver.exe", "chromedriver.exe"),
            curl=resolve_tool_path(c.curl_path, data / "curl" / "curl.exe", "curl.exe"),
            sumatra=resolve_tool_path(c.sumatra_path, data / "SumatraPDF" / "SumatraPDF.exe", "SumatraPDF.exe"),
        )

    def _build_ui(self) -> None:
        self.root.title("OwlView 自動出力ツール")
        self.root.geometry(self.cfg.common.window_geometry)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        top = ttk.Frame(self.root, padding=6)
        top.pack(fill=tk.BOTH, expand=True)
        top.columnconfigure(0, weight=3)
        top.columnconfigure(1, weight=2)

        left = ttk.Frame(top)
        right = ttk.Frame(top)
        left.grid(row=0, column=0, sticky="nsew")
        right.grid(row=0, column=1, sticky="nsew", padx=(8, 0))

        self._build_parts_area(left)
        self._build_main_controls(right)
        self._build_bottom_area()

    def _build_parts_area(self, parent: ttk.Frame) -> None:
        top = ttk.Frame(parent)
        top.pack(fill=tk.X)
        ttk.Label(top, text="パート検索").pack(side=tk.LEFT)
        ent = ttk.Entry(top, textvariable=self.search_var)
        ent.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=4)
        ent.bind("<KeyRelease>", lambda _e: self._refresh_part_list())

        cols = ("enabled", "selected", "part_name", "output_name", "output_dir", "format", "orientation", "scale")
        self.tree = ttk.Treeview(parent, columns=cols, show="headings", selectmode="extended", height=18)
        headers = {
            "enabled": "有効",
            "selected": "対象",
            "part_name": "パート名",
            "output_name": "保存名",
            "output_dir": "保存先",
            "format": "形式",
            "orientation": "向き",
            "scale": "倍率",
        }
        for c in cols:
            self.tree.heading(c, text=headers[c])
            self.tree.column(c, width=100 if c != "output_dir" else 240, anchor=tk.W)
        self.tree.pack(fill=tk.BOTH, expand=True, pady=6)
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)

        row = ttk.Frame(parent)
        row.pack(fill=tk.X)
        for label, cmd in [
            ("追加", self.add_part),
            ("編集", self.edit_part),
            ("複製", self.duplicate_selected),
            ("削除", self.delete_selected),
            ("↑", lambda: self.move_selected(-1)),
            ("↓", lambda: self.move_selected(1)),
            ("一括ON", self.select_all_parts),
            ("一括OFF", self.clear_all_parts),
        ]:
            ttk.Button(row, text=label, command=cmd).pack(side=tk.LEFT, padx=2)

    def _build_main_controls(self, parent: ttk.Frame) -> None:
        frm = ttk.LabelFrame(parent, text="メイン操作", padding=8)
        frm.pack(fill=tk.BOTH, expand=True)

        ttk.Checkbutton(frm, text="FTPへアップロードする", variable=self.main_ftp_var).pack(anchor="w", pady=2)
        ttk.Checkbutton(frm, text="印刷する", variable=self.main_print_var).pack(anchor="w", pady=2)

        pr = ttk.Frame(frm)
        pr.pack(fill=tk.X, pady=4)
        ttk.Label(pr, text="プリンタ").pack(side=tk.LEFT)
        self.printer_combo = ttk.Combobox(pr, textvariable=self.main_printer_var, state="readonly")
        self.printer_combo.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=4)
        ttk.Button(pr, text="再取得", command=self.reload_printers).pack(side=tk.LEFT)

        btns = ttk.Frame(frm)
        btns.pack(fill=tk.X, pady=6)
        ttk.Button(btns, text="実行", command=self.run_selected).pack(side=tk.LEFT, padx=2)
        ttk.Button(btns, text="プレビューのみ", command=self.preview_only).pack(side=tk.LEFT, padx=2)
        ttk.Button(btns, text="詳細設定", command=self.open_detail_settings).pack(side=tk.LEFT, padx=2)
        ttk.Button(btns, text="画像プレビュー表示", command=self.open_preview_window).pack(side=tk.LEFT, padx=2)

        ttk.Label(frm, textvariable=self.preview_status, foreground="#333").pack(anchor="w", pady=(8, 0))

    def _build_bottom_area(self) -> None:
        bar = ttk.Frame(self.root, padding=6)
        bar.pack(fill=tk.X)
        ttk.Button(bar, text="個別実行", command=self.run_single).pack(side=tk.LEFT, padx=2)
        ttk.Button(bar, text="範囲実行", command=self.run_range).pack(side=tk.LEFT, padx=2)
        ttk.Button(bar, text="全件実行", command=self.run_all).pack(side=tk.LEFT, padx=2)
        ttk.Button(bar, text="停止", command=self.stop_run).pack(side=tk.LEFT, padx=2)
        ttk.Button(bar, text="出力先を開く", command=self.open_output_dir).pack(side=tk.LEFT, padx=2)
        ttk.Progressbar(bar, variable=self.progress_var, maximum=100, length=220).pack(side=tk.RIGHT)

        logf = ttk.LabelFrame(self.root, text="実行ログ", padding=6)
        logf.pack(fill=tk.BOTH, expand=False)
        self.log_text = tk.Text(logf, height=11)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        ttk.Label(self.root, textvariable=self.status_var, padding=6).pack(anchor="w")

    def _refresh_printer_combo(self) -> None:
        ps = printer_list()
        self.printer_combo["values"] = ps
        if self.main_printer_var.get() not in ps and ps:
            self.main_printer_var.set(ps[0])

    def _refresh_part_list(self) -> None:
        self.tree.delete(*self.tree.get_children())
        q = self.search_var.get().strip().lower()
        for idx, p in enumerate(self.cfg.parts):
            if q and q not in p.part_name.lower() and q not in p.output_name.lower():
                continue
            self.tree.insert(
                "",
                tk.END,
                iid=str(idx),
                values=("ON" if p.enabled else "OFF", "ON" if p.selected else "OFF", p.part_name, p.output_name, p.output_dir, p.output_format, p.orientation, p.scale),
            )

    def _on_tree_select(self, _e=None) -> None:
        self.selected_ids = [int(i) for i in self.tree.selection()]

    def _part_dialog(self, part: PartConfig | None = None) -> PartConfig | None:
        d = tk.Toplevel(self.root)
        d.title("パート編集")
        d.columnconfigure(1, weight=1)
        p = part or PartConfig(
            local_copy_enabled=True,
            ftp_upload_enabled=self.cfg.common.ftp_default_enabled,
            print_enabled=self.cfg.common.print_default_enabled,
            copies=self.cfg.common.default_print_copies,
            printer_name=self.cfg.common.default_printer_name,
            margin_top=0.0,
            margin_bottom=0.0,
            margin_left=0.0,
            margin_right=0.0,
            paper_width=8.27,
            paper_height=11.69,
            jpg_quality=90,
        )
        vals: dict[str, tk.Variable] = {
            "part_name": tk.StringVar(value=p.part_name),
            "output_name": tk.StringVar(value=p.output_name),
            "output_dir": tk.StringVar(value=p.output_dir),
            "format": tk.StringVar(value=p.output_format),
            "scale": tk.DoubleVar(value=p.scale),
            "orientation": tk.StringVar(value=p.orientation),
            "ftp_upload_enabled": tk.BooleanVar(value=p.ftp_upload_enabled),
            "local_copy_enabled": tk.BooleanVar(value=p.local_copy_enabled),
            "print_enabled": tk.BooleanVar(value=p.print_enabled),
            "printer_name": tk.StringVar(value=p.printer_name),
            "copies": tk.IntVar(value=p.copies),
            "margin_top": tk.DoubleVar(value=p.margin_top),
            "margin_bottom": tk.DoubleVar(value=p.margin_bottom),
            "margin_left": tk.DoubleVar(value=p.margin_left),
            "margin_right": tk.DoubleVar(value=p.margin_right),
            "paper_width": tk.DoubleVar(value=p.paper_width),
            "paper_height": tk.DoubleVar(value=p.paper_height),
            "jpg_quality": tk.IntVar(value=p.jpg_quality),
        }

        row = 0
        text_items = [
            ("パート名", "part_name"), ("保存名", "output_name"), ("保存先", "output_dir"), ("形式(pdf/jpg/both)", "format"),
            ("倍率", "scale"), ("向き", "orientation"), ("プリンタ名", "printer_name"), ("部数", "copies"),
            ("余白 上", "margin_top"), ("余白 下", "margin_bottom"), ("余白 左", "margin_left"), ("余白 右", "margin_right"),
            ("用紙幅", "paper_width"), ("用紙高", "paper_height"), ("JPG品質", "jpg_quality"),
        ]
        printers = printer_list()
        for label, key in text_items:
            ttk.Label(d, text=label).grid(row=row, column=0, sticky="w")
            if key == "printer_name" and printers:
                ttk.Combobox(d, textvariable=vals[key], values=printers).grid(row=row, column=1, sticky="ew", pady=2)
            else:
                ttk.Entry(d, textvariable=vals[key], width=50).grid(row=row, column=1, sticky="ew", pady=2)
            row += 1

        for label, key in [("FTPアップロードON", "ftp_upload_enabled"), ("ローカルコピーON", "local_copy_enabled"), ("印刷ON", "print_enabled")]:
            ttk.Checkbutton(d, text=label, variable=vals[key]).grid(row=row, column=1, sticky="w")
            row += 1

        ok = {"value": False}
        ttk.Button(d, text="保存", command=lambda: (ok.__setitem__("value", True), d.destroy())).grid(row=row + 1, column=1, sticky="e")
        d.transient(self.root)
        d.grab_set()
        d.wait_window()
        if not ok["value"]:
            return None

        new = PartConfig(
            enabled=True,
            selected=part.selected if part else False,
            part_name=vals["part_name"].get(),
            output_name=vals["output_name"].get(),
            output_dir=vals["output_dir"].get(),
            output_format=vals["format"].get(),
            scale=float(vals["scale"].get()),
            orientation=vals["orientation"].get(),
            ftp_upload_enabled=bool(vals["ftp_upload_enabled"].get()),
            local_copy_enabled=bool(vals["local_copy_enabled"].get()),
            print_enabled=bool(vals["print_enabled"].get()),
            printer_name=vals["printer_name"].get(),
            copies=int(vals["copies"].get()),
            margin_top=float(vals["margin_top"].get()),
            margin_bottom=float(vals["margin_bottom"].get()),
            margin_left=float(vals["margin_left"].get()),
            margin_right=float(vals["margin_right"].get()),
            paper_width=float(vals["paper_width"].get()),
            paper_height=float(vals["paper_height"].get()),
            jpg_quality=int(vals["jpg_quality"].get()),
        )
        errs = new.validate()
        if errs:
            messagebox.showerror("入力エラー", "\n".join(errs))
            return None
        return new

    def open_detail_settings(self) -> None:
        c = self.cfg.common
        d = tk.Toplevel(self.root)
        d.title("詳細設定")
        d.geometry("880x720")
        d.columnconfigure(1, weight=1)
        vars = {
            "home": tk.StringVar(value=c.owlview_home_url), "report": tk.StringVar(value=c.owlview_report_url),
            "xpath": tk.StringVar(value=c.xpath_input_box), "wait": tk.IntVar(value=c.selenium_wait_sec),
            "local_dir": tk.StringVar(value=c.default_local_copy_dir),
            "chromedriver": tk.StringVar(value=c.chromedriver_path), "curl": tk.StringVar(value=c.curl_path), "sumatra": tk.StringVar(value=c.sumatra_path),
            "ftp_default": tk.BooleanVar(value=c.ftp_default_enabled), "ftp_encryption": tk.StringVar(value=c.ftp_encryption),
            "ftp_host": tk.StringVar(value=c.ftp_host), "ftp_port": tk.IntVar(value=c.ftp_port),
            "ftp_user": tk.StringVar(value=c.ftp_username), "ftp_pass": tk.StringVar(value=c.ftp_password), "ftp_path": tk.StringVar(value=c.ftp_remote_path_template),
            "print_default": tk.BooleanVar(value=c.print_default_enabled), "default_printer": tk.StringVar(value=c.default_printer_name),
            "default_copies": tk.IntVar(value=c.default_print_copies), "auto_save": tk.BooleanVar(value=c.auto_save_settings),
        }

        rows = [
            ("OwlView Home URL", "home"), ("OwlView Report URL", "report"), ("XPath", "xpath"), ("Selenium待機秒数", "wait"),
            ("ローカルコピー先", "local_dir"), ("ChromeDriverパス", "chromedriver"), ("curlパス", "curl"), ("SumatraPDFパス", "sumatra"),
            ("FTPデフォルト有効", "ftp_default"), ("FTP Encryption", "ftp_encryption"), ("FTP Host", "ftp_host"), ("FTP Port", "ftp_port"),
            ("FTP Username", "ftp_user"), ("FTP Password", "ftp_pass"), ("Remote Path Template", "ftp_path"),
            ("印刷デフォルト有効", "print_default"), ("デフォルトプリンタ", "default_printer"), ("デフォルト部数", "default_copies"),
            ("自動保存", "auto_save"),
        ]
        for i, (label, key) in enumerate(rows):
            ttk.Label(d, text=label).grid(row=i, column=0, sticky="w", padx=6, pady=3)
            if isinstance(vars[key], tk.BooleanVar):
                ttk.Checkbutton(d, variable=vars[key]).grid(row=i, column=1, sticky="w")
            elif key == "ftp_encryption":
                ttk.Combobox(d, textvariable=vars[key], values=["Implicit TLS/SSL", "Explicit TLS/SSL", "None"], state="readonly").grid(row=i, column=1, sticky="ew", padx=6)
            elif key == "default_printer":
                ttk.Combobox(d, textvariable=vars[key], values=printer_list()).grid(row=i, column=1, sticky="ew", padx=6)
            else:
                ttk.Entry(d, textvariable=vars[key], show="*" if key == "ftp_pass" else "").grid(row=i, column=1, sticky="ew", padx=6)

        def _save_detail() -> None:
            c.owlview_home_url = vars["home"].get()
            c.owlview_report_url = vars["report"].get()
            c.xpath_input_box = vars["xpath"].get()
            c.selenium_wait_sec = int(vars["wait"].get())
            c.default_local_copy_dir = vars["local_dir"].get()
            c.chromedriver_path = vars["chromedriver"].get()
            c.curl_path = vars["curl"].get()
            c.sumatra_path = vars["sumatra"].get()
            c.ftp_default_enabled = bool(vars["ftp_default"].get())
            c.ftp_encryption = vars["ftp_encryption"].get()
            c.ftp_host = vars["ftp_host"].get()
            c.ftp_port = int(vars["ftp_port"].get())
            c.ftp_username = vars["ftp_user"].get()
            c.ftp_password = vars["ftp_pass"].get()
            c.ftp_remote_path_template = vars["ftp_path"].get()
            c.print_default_enabled = bool(vars["print_default"].get())
            c.default_printer_name = vars["default_printer"].get()
            c.default_print_copies = int(vars["default_copies"].get())
            c.auto_save_settings = bool(vars["auto_save"].get())
            self.store.save(self.cfg)
            self.tools = self._resolve_tools()
            self.main_printer_var.set(c.default_printer_name)
            self._refresh_printer_combo()
            self._log("詳細設定を保存しました")
            d.destroy()

        btns = ttk.Frame(d)
        btns.grid(row=len(rows), column=0, columnspan=2, sticky="ew", padx=6, pady=10)
        ttk.Button(btns, text="FTP接続テスト", command=self.test_ftp).pack(side=tk.LEFT, padx=2)
        ttk.Button(btns, text="保存", command=_save_detail).pack(side=tk.RIGHT, padx=2)

    def add_part(self) -> None:
        p = self._part_dialog()
        if p:
            self.cfg.parts.append(p)
            self._refresh_part_list()
            self.auto_save()

    def edit_part(self) -> None:
        if not self.selected_ids:
            return
        idx = self.selected_ids[0]
        p = self._part_dialog(self.cfg.parts[idx])
        if p:
            self.cfg.parts[idx] = p
            self._refresh_part_list()
            self.auto_save()

    def duplicate_selected(self) -> None:
        for idx in self.selected_ids:
            src = self.cfg.parts[idx]
            cp = PartConfig(**src.__dict__)
            cp.output_name = f"{cp.output_name}_copy"
            self.cfg.parts.append(cp)
        self._refresh_part_list()
        self.auto_save()

    def delete_selected(self) -> None:
        for idx in sorted(self.selected_ids, reverse=True):
            del self.cfg.parts[idx]
        self._refresh_part_list()
        self.auto_save()

    def move_selected(self, offset: int) -> None:
        if len(self.selected_ids) != 1:
            return
        idx = self.selected_ids[0]
        ni = idx + offset
        if not (0 <= ni < len(self.cfg.parts)):
            return
        self.cfg.parts[idx], self.cfg.parts[ni] = self.cfg.parts[ni], self.cfg.parts[idx]
        self._refresh_part_list()

    def select_all_parts(self) -> None:
        for p in self.cfg.parts:
            p.selected = True
        self._refresh_part_list()

    def clear_all_parts(self) -> None:
        for p in self.cfg.parts:
            p.selected = False
        self._refresh_part_list()

    def auto_save(self) -> None:
        if self.cfg.common.auto_save_settings:
            self.store.save(self.cfg)

    def test_ftp(self) -> None:
        path_errors = validate_ftp_path_template(self.cfg.common.ftp_remote_path_template)
        expanded = resolved_remote_path(self.cfg.common.ftp_remote_path_template)
        if path_errors:
            messagebox.showwarning("FTP Path", f"バリデーション警告:\n" + "\n".join(path_errors) + f"\n\n展開後: {expanded}")
        try:
            remote, result = ftp_test_connection(self.cfg.common, self.tools.curl)
            msg = "\n".join([
                "接続成功", f"host={self.cfg.common.ftp_host}", f"port={self.cfg.common.ftp_port}", f"暗号方式={self.cfg.common.ftp_encryption}",
                f"ユーザー名={self.cfg.common.ftp_username}", f"リモートパス(展開後)={remote}", f"curl={result.command_summary}",
                f"stdout={result.stdout or '(empty)'}", f"stderr={result.stderr or '(empty)'}",
            ])
            self._log(msg)
            messagebox.showinfo("FTP", msg)
        except Exception as exc:
            self._log(str(exc))
            messagebox.showerror("FTP", f"接続失敗:\n{exc}")

    def reload_printers(self) -> None:
        self._refresh_printer_combo()
        ps = printer_list()
        messagebox.showinfo("Printer", "\n".join(ps) if ps else "プリンタが見つかりません")

    def _startup_external_tool_check(self) -> None:
        checks = [(self.tools.chromedriver, "OwlView取得"), (self.tools.curl, "FTP接続テスト / FTPアップロード"), (self.tools.sumatra, "印刷")]
        missing = [f"{p} ({feature}に必要)" for p, feature in checks if not p.exists()]
        if missing:
            messagebox.showwarning("外部ツール警告", "起動時チェックで未検出:\n" + "\n".join(missing))
            for m in missing:
                self._log(f"外部ツール未検出: {m}")

    def _apply_main_toggles(self, parts: list[PartConfig]) -> list[PartConfig]:
        cloned: list[PartConfig] = []
        printer = self.main_printer_var.get().strip()
        for p in parts:
            cp = PartConfig(**p.__dict__)
            cp.ftp_upload_enabled = self.main_ftp_var.get() and cp.ftp_upload_enabled
            cp.print_enabled = self.main_print_var.get() and cp.print_enabled
            if printer:
                cp.printer_name = printer
            cloned.append(cp)
        return cloned

    def _validate_before_run(self, parts: list[PartConfig]) -> list[str]:
        errs: list[str] = []
        if not self.tools.chromedriver.exists():
            errs.append(f"ChromeDriver が見つかりません: {self.tools.chromedriver}")
        if any(p.ftp_upload_enabled for p in parts) and not self.tools.curl.exists():
            errs.append(f"curl が見つかりません: {self.tools.curl}")
        if any(p.print_enabled for p in parts):
            ps = printer_list()
            if not self.tools.sumatra.exists():
                errs.append(f"SumatraPDF が見つかりません: {self.tools.sumatra}")
            for p in parts:
                if p.print_enabled and p.printer_name and ps and p.printer_name not in ps:
                    errs.append(f"指定プリンタが存在しません: {p.part_name}: {p.printer_name}")
        return errs

    def _start_run(self, parts: list[PartConfig]) -> None:
        if self.runner:
            messagebox.showwarning("実行中", "現在実行中です")
            return
        valid = [p for p in parts if p.enabled]
        if not valid:
            messagebox.showwarning("対象なし", "実行対象がありません")
            return
        valid = self._apply_main_toggles(valid)
        errors = self._validate_before_run(valid)
        if errors:
            messagebox.showerror("実行前バリデーション", "\n".join(errors))
            return
        run_cfg = AppConfig(parts=self.cfg.parts, common=self.cfg.common, version=self.cfg.version)
        self.runner = Runner(run_cfg, self.tools, self.queue)
        self.runner.run_async(valid)

    def run_single(self) -> None:
        if self.selected_ids:
            self._start_run([self.cfg.parts[self.selected_ids[0]]])

    def run_selected(self) -> None:
        self._start_run([p for p in self.cfg.parts if p.selected] or ([self.cfg.parts[self.selected_ids[0]]] if self.selected_ids else []))

    def run_range(self) -> None:
        s = simpledialog.askinteger("範囲", "開始番号(1開始)", parent=self.root)
        e = simpledialog.askinteger("範囲", "終了番号(1開始)", parent=self.root)
        if not s or not e:
            return
        s0, e0 = min(s, e) - 1, max(s, e)
        self._start_run(self.cfg.parts[s0:e0])

    def run_all(self) -> None:
        self._start_run(self.cfg.parts)

    def preview_only(self) -> None:
        if not self.selected_ids:
            return
        p = self._apply_main_toggles([self.cfg.parts[self.selected_ids[0]]])[0]
        if not self.tools.chromedriver.exists():
            messagebox.showerror("プレビュー", f"ChromeDriverが見つかりません: {self.tools.chromedriver}")
            return
        preview_dir = self.base_dir / "Settings" / "_preview"
        preview_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        p.output_dir = str(preview_dir)
        p.output_name = f"preview_{timestamp}"

        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service

        opts = Options()
        opts.add_argument("--headless=new")
        runner = Runner(self.cfg, self.tools, self.queue)
        try:
            driver = webdriver.Chrome(service=Service(str(self.tools.chromedriver)), options=opts)
            try:
                outputs, pdf_path = runner.run_capture_flow(driver, p, preview_mode=True)
            finally:
                driver.quit()
            target = next((x for x in outputs if x.suffix.lower() == ".jpg"), None) or pdf_path
            if target:
                self.update_preview(target)
            self._log("プレビュー完了")
        except Exception as exc:
            self._log(f"プレビュー失敗: {exc}")
            self._append_stacktrace(exc)
            messagebox.showerror("プレビュー", f"印刷プレビュー表示に失敗しました。\n{exc}")

    def open_preview_window(self) -> None:
        if self.preview_window and self.preview_window.winfo_exists():
            self.preview_window.deiconify()
            self.preview_window.lift()
            return
        w = tk.Toplevel(self.root)
        w.title("画像プレビュー")
        w.geometry("960x720")
        self.preview_window = w
        bar = ttk.Frame(w, padding=6)
        bar.pack(fill=tk.X)
        ttk.Button(bar, text="拡大+", command=lambda: self._zoom_preview(1.2)).pack(side=tk.LEFT, padx=2)
        ttk.Button(bar, text="縮小-", command=lambda: self._zoom_preview(1 / 1.2)).pack(side=tk.LEFT, padx=2)
        ttk.Button(bar, text="再読み込み", command=self.reload_preview).pack(side=tk.LEFT, padx=2)
        ttk.Button(bar, text="ファイルを開く", command=self.open_preview_file).pack(side=tk.LEFT, padx=2)
        ttk.Label(bar, textvariable=self.preview_status).pack(side=tk.RIGHT)
        self.preview_label = ttk.Label(w)
        self.preview_label.pack(fill=tk.BOTH, expand=True)
        if self.preview_source:
            self.update_preview(self.preview_source)

    def update_preview(self, path: Path) -> None:
        try:
            if path.suffix.lower() == ".jpg":
                img = Image.open(path).convert("RGB")
            elif path.suffix.lower() == ".pdf":
                img = render_pdf_first_page_image(path)
            else:
                raise RuntimeError(f"プレビュー対象外: {path}")
            self.preview_source = path
            self.preview_image = img
            self.preview_scale = 1.0
            self._render_preview()
            self.preview_status.set(f"プレビュー: {path.name}")
            if self.preview_window is None or not self.preview_window.winfo_exists():
                self.open_preview_window()
        except Exception as exc:
            self.preview_status.set(f"プレビュー失敗: {exc}")
            self._append_stacktrace(exc)

    def _render_preview(self) -> None:
        if not self.preview_image or not self.preview_label:
            return
        w = max(1, int(self.preview_image.width * self.preview_scale))
        h = max(1, int(self.preview_image.height * self.preview_scale))
        resized = self.preview_image.resize((w, h), Image.Resampling.LANCZOS)
        self.preview_photo = ImageTk.PhotoImage(resized)
        self.preview_label.configure(image=self.preview_photo)

    def _zoom_preview(self, ratio: float) -> None:
        if not self.preview_image:
            return
        self.preview_scale = max(0.1, min(5.0, self.preview_scale * ratio))
        self._render_preview()

    def reload_preview(self) -> None:
        if self.preview_source and self.preview_source.exists():
            self.update_preview(self.preview_source)

    def open_preview_file(self) -> None:
        if self.preview_source and self.preview_source.exists():
            os.startfile(self.preview_source)  # type: ignore[attr-defined]

    def stop_run(self) -> None:
        if self.runner:
            self.runner.stop()

    def open_output_dir(self) -> None:
        if self.selected_ids:
            p = self.cfg.parts[self.selected_ids[0]]
            if os.path.isdir(p.output_dir):
                os.startfile(p.output_dir)  # type: ignore[attr-defined]

    def _poll_queue(self) -> None:
        try:
            while True:
                kind, payload = self.queue.get_nowait()
                if kind == "start":
                    self.progress_var.set(0)
                    self.status_var.set("実行開始")
                elif kind == "progress":
                    total = max(payload["total"], 1)
                    self.progress_var.set(payload["value"] / total * 100)
                    self.status_var.set(payload["text"])
                elif kind == "log":
                    self._log(payload["text"])
                elif kind == "done":
                    self._show_result_dialog(payload["results"])
                    self.runner = None
        except Empty:
            pass
        self.root.after(150, self._poll_queue)

    def _show_result_dialog(self, results) -> None:
        dlg = tk.Toplevel(self.root)
        dlg.title("実行結果")
        txt = tk.Text(dlg, width=110, height=22)
        txt.pack(fill=tk.BOTH, expand=True)
        for r in results:
            txt.insert(tk.END, f"{'OK' if r.success else 'NG'} | {r.part_name} | {r.message}\n")
            for status in r.file_statuses:
                txt.insert(tk.END, f"  - {status.file_path.name}: 保存済み / ローカル={status.local_copy} / FTP={status.ftp} / 印刷={status.print_status}\n")
            txt.insert(tk.END, "\n")
        ttk.Button(dlg, text="閉じる", command=dlg.destroy).pack(side=tk.RIGHT)

    def _append_stacktrace(self, exc: Exception) -> None:
        log_dir = self.base_dir / "Settings"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "error.log"
        with log_file.open("a", encoding="utf-8") as f:
            f.write(f"\n[{datetime.now().isoformat()}] {exc}\n")
            f.write(traceback.format_exc())
        self._log(f"詳細ログ保存: {log_file}")

    def _log(self, text: str) -> None:
        self.log_text.insert(tk.END, text + "\n")
        self.log_text.see(tk.END)

    def on_close(self) -> None:
        self.cfg.common.default_printer_name = self.main_printer_var.get()
        self.cfg.common.window_geometry = self.root.geometry()
        self.store.save(self.cfg)
        self.root.destroy()

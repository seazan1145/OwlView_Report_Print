from __future__ import annotations

import os
import tkinter as tk
from datetime import datetime
from pathlib import Path
from queue import Empty, Queue
from tkinter import filedialog, messagebox, simpledialog, ttk

from .config_store import ConfigStore
from .executor import Runner
from .models import AppConfig, PartConfig
from .services import (
    ExternalTools,
    ftp_test_connection,
    printer_list,
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
        self.tools = self._resolve_tools()

        self._build_ui()
        self._refresh_part_list()
        self._poll_queue()
        self._startup_external_tool_check()

    def _resolve_tools(self) -> ExternalTools:
        data = self.base_dir / "Data"
        common = self.cfg.common
        return ExternalTools(
            chromedriver=resolve_tool_path(common.chromedriver_path, data / "chromedriver.exe", "chromedriver.exe"),
            curl=resolve_tool_path(common.curl_path, data / "curl" / "curl.exe", "curl.exe"),
            sumatra=resolve_tool_path(common.sumatra_path, data / "SumatraPDF" / "SumatraPDF.exe", "SumatraPDF.exe"),
        )

    def _build_ui(self) -> None:
        self.root.title("OwlView 自動出力ツール (新版)")
        self.root.geometry(self.cfg.common.window_geometry)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        paned = ttk.Panedwindow(self.root, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True)

        left = ttk.Frame(paned, padding=6)
        right = ttk.Frame(paned, padding=6)
        paned.add(left, weight=3)
        paned.add(right, weight=2)

        self._build_parts_area(left)
        self._build_common_area(right)
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
            self.tree.column(c, width=100 if c != "output_dir" else 220, anchor=tk.W)
        self.tree.pack(fill=tk.BOTH, expand=True, pady=6)
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)
        self.tree.bind("<Button-3>", self._on_context)

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

    def _build_common_area(self, parent: ttk.Frame) -> None:
        frm = ttk.LabelFrame(parent, text="共通設定", padding=6)
        frm.pack(fill=tk.BOTH, expand=True)
        c = self.cfg.common
        self.vars = {
            "home": tk.StringVar(value=c.owlview_home_url),
            "report": tk.StringVar(value=c.owlview_report_url),
            "xpath": tk.StringVar(value=c.xpath_input_box),
            "wait": tk.IntVar(value=c.selenium_wait_sec),
            "local_dir": tk.StringVar(value=c.default_local_copy_dir),
            "ftp_encryption": tk.StringVar(value=c.ftp_encryption),
            "ftp_host": tk.StringVar(value=c.ftp_host),
            "ftp_port": tk.IntVar(value=c.ftp_port),
            "ftp_user": tk.StringVar(value=c.ftp_username),
            "ftp_pass": tk.StringVar(value=c.ftp_password),
            "ftp_path": tk.StringVar(value=c.ftp_remote_path_template),
            "printer": tk.StringVar(value=c.default_printer_name),
            "chromedriver": tk.StringVar(value=c.chromedriver_path),
            "curl": tk.StringVar(value=c.curl_path),
            "sumatra": tk.StringVar(value=c.sumatra_path),
        }
        rows = [
            ("Home URL", "home"),
            ("Report URL", "report"),
            ("XPath", "xpath"),
            ("Wait秒", "wait"),
            ("Local Copy先", "local_dir"),
            ("FTP 暗号", "ftp_encryption"),
            ("FTP Host", "ftp_host"),
            ("FTP Port", "ftp_port"),
            ("FTP User", "ftp_user"),
            ("FTP Pass", "ftp_pass"),
            ("FTP Path", "ftp_path"),
            ("Printer", "printer"),
            ("ChromeDriver Path", "chromedriver"),
            ("curl Path", "curl"),
            ("SumatraPDF Path", "sumatra"),
        ]
        for i, (lbl, key) in enumerate(rows):
            ttk.Label(frm, text=lbl).grid(row=i, column=0, sticky="w")
            if key == "ftp_encryption":
                cb = ttk.Combobox(frm, textvariable=self.vars[key], values=["Implicit TLS/SSL", "Explicit TLS/SSL", "None"], state="readonly")
                cb.grid(row=i, column=1, sticky="ew", pady=2)
            else:
                ttk.Entry(frm, textvariable=self.vars[key], width=40, show="*" if key == "ftp_pass" else "").grid(row=i, column=1, sticky="ew", pady=2)

        buttons = ttk.Frame(frm)
        buttons.grid(row=16, column=0, columnspan=2, sticky="ew", pady=6)
        ttk.Button(buttons, text="保存", command=self.save_settings).pack(side=tk.LEFT, padx=2)
        ttk.Button(buttons, text="保存先参照", command=self.pick_local_dir).pack(side=tk.LEFT, padx=2)
        ttk.Button(buttons, text="FTP接続テスト", command=self.test_ftp).pack(side=tk.LEFT, padx=2)
        ttk.Button(buttons, text="プリンタ再取得", command=self.reload_printers).pack(side=tk.LEFT, padx=2)

    def _build_bottom_area(self) -> None:
        bar = ttk.Frame(self.root, padding=6)
        bar.pack(fill=tk.X)
        ttk.Button(bar, text="個別実行", command=self.run_single).pack(side=tk.LEFT, padx=2)
        ttk.Button(bar, text="選択実行", command=self.run_selected).pack(side=tk.LEFT, padx=2)
        ttk.Button(bar, text="範囲実行", command=self.run_range).pack(side=tk.LEFT, padx=2)
        ttk.Button(bar, text="全件実行", command=self.run_all).pack(side=tk.LEFT, padx=2)
        ttk.Button(bar, text="プレビューのみ", command=self.preview_only).pack(side=tk.LEFT, padx=2)
        ttk.Button(bar, text="停止", command=self.stop_run).pack(side=tk.LEFT, padx=2)
        ttk.Button(bar, text="出力先を開く", command=self.open_output_dir).pack(side=tk.LEFT, padx=2)
        ttk.Progressbar(bar, variable=self.progress_var, maximum=100, length=220).pack(side=tk.RIGHT)

        logf = ttk.LabelFrame(self.root, text="実行ログ", padding=6)
        logf.pack(fill=tk.BOTH, expand=False)
        self.log_text = tk.Text(logf, height=10)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        ttk.Label(self.root, textvariable=self.status_var, padding=6).pack(anchor="w")

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
        if self.selected_ids:
            self.cfg.common.last_selected_part_index = self.selected_ids[0]

    def _on_context(self, e) -> None:
        menu = tk.Menu(self.root, tearoff=0)
        menu.add_command(label="編集", command=self.edit_part)
        menu.add_command(label="複製", command=self.duplicate_selected)
        menu.add_command(label="一時無効化", command=lambda: self.toggle_enabled(False))
        menu.add_command(label="有効化", command=lambda: self.toggle_enabled(True))
        menu.tk_popup(e.x_root, e.y_root)

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
            ("パート名", "part_name"),
            ("保存名", "output_name"),
            ("保存先", "output_dir"),
            ("形式(pdf/jpg/both)", "format"),
            ("倍率", "scale"),
            ("向き", "orientation"),
            ("プリンタ名", "printer_name"),
            ("部数", "copies"),
            ("余白 上", "margin_top"),
            ("余白 下", "margin_bottom"),
            ("余白 左", "margin_left"),
            ("余白 右", "margin_right"),
            ("用紙幅", "paper_width"),
            ("用紙高", "paper_height"),
            ("JPG品質", "jpg_quality"),
        ]
        printers = printer_list()
        for label, key in text_items:
            ttk.Label(d, text=label).grid(row=row, column=0, sticky="w")
            if key == "printer_name" and printers:
                ttk.Combobox(d, textvariable=vals[key], values=printers).grid(row=row, column=1, sticky="ew", pady=2)
            else:
                ttk.Entry(d, textvariable=vals[key], width=50).grid(row=row, column=1, sticky="ew", pady=2)
            row += 1

        for label, key in [
            ("FTPアップロードON", "ftp_upload_enabled"),
            ("ローカルコピーON", "local_copy_enabled"),
            ("印刷ON", "print_enabled"),
        ]:
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

    def toggle_enabled(self, value: bool) -> None:
        for i in self.selected_ids:
            self.cfg.parts[i].enabled = value
        self._refresh_part_list()
        self.auto_save()

    def select_all_parts(self) -> None:
        for p in self.cfg.parts:
            p.selected = True
        self._refresh_part_list()

    def clear_all_parts(self) -> None:
        for p in self.cfg.parts:
            p.selected = False
        self._refresh_part_list()

    def save_settings(self) -> None:
        c = self.cfg.common
        c.owlview_home_url = self.vars["home"].get()
        c.owlview_report_url = self.vars["report"].get()
        c.xpath_input_box = self.vars["xpath"].get()
        c.selenium_wait_sec = int(self.vars["wait"].get())
        c.default_local_copy_dir = self.vars["local_dir"].get()
        c.ftp_encryption = self.vars["ftp_encryption"].get()
        c.ftp_host = self.vars["ftp_host"].get()
        c.ftp_port = int(self.vars["ftp_port"].get())
        c.ftp_username = self.vars["ftp_user"].get()
        c.ftp_password = self.vars["ftp_pass"].get()
        c.ftp_remote_path_template = self.vars["ftp_path"].get()
        c.default_printer_name = self.vars["printer"].get()
        c.chromedriver_path = self.vars["chromedriver"].get()
        c.curl_path = self.vars["curl"].get()
        c.sumatra_path = self.vars["sumatra"].get()
        self.tools = self._resolve_tools()
        self.store.save(self.cfg)
        self.status_var.set("設定を保存しました")

    def auto_save(self) -> None:
        if self.cfg.common.auto_save_settings:
            self.save_settings()

    def pick_local_dir(self) -> None:
        d = filedialog.askdirectory()
        if d:
            self.vars["local_dir"].set(d)
            self.auto_save()

    def test_ftp(self) -> None:
        self.save_settings()
        path_errors = validate_ftp_path_template(self.cfg.common.ftp_remote_path_template)
        expanded = resolved_remote_path(self.cfg.common.ftp_remote_path_template)
        if path_errors:
            messagebox.showwarning("FTP Path", f"バリデーション警告:\n" + "\n".join(path_errors) + f"\n\n展開後: {expanded}")
        try:
            remote, result = ftp_test_connection(self.cfg.common, self.tools.curl)
            msg = "\n".join(
                [
                    "接続成功",
                    f"host={self.cfg.common.ftp_host}",
                    f"port={self.cfg.common.ftp_port}",
                    f"暗号方式={self.cfg.common.ftp_encryption}",
                    f"ユーザー名={self.cfg.common.ftp_username}",
                    f"リモートパス(展開後)={remote}",
                    f"curl={result.command_summary}",
                    f"stdout={result.stdout or '(empty)'}",
                    f"stderr={result.stderr or '(empty)'}",
                ]
            )
            self._log(msg)
            messagebox.showinfo("FTP", msg)
        except Exception as exc:
            self._log(str(exc))
            messagebox.showerror("FTP", f"接続失敗:\n{exc}")

    def reload_printers(self) -> None:
        ps = printer_list()
        messagebox.showinfo("Printer", "\n".join(ps) if ps else "プリンタが見つかりません")

    def _startup_external_tool_check(self) -> None:
        checks = [
            (self.tools.chromedriver, "OwlView取得"),
            (self.tools.curl, "FTP接続テスト / FTPアップロード"),
            (self.tools.sumatra, "印刷"),
        ]
        missing = [f"{p} ({feature}に必要)" for p, feature in checks if not p.exists()]
        if missing:
            messagebox.showwarning("外部ツール警告", "起動時チェックで未検出:\n" + "\n".join(missing))
            for m in missing:
                self._log(f"外部ツール未検出: {m}")

    def _validate_before_run(self, parts: list[PartConfig]) -> list[str]:
        errs: list[str] = []
        today = datetime.now().strftime("%y%m%d")

        if not self.tools.chromedriver.exists():
            errs.append(f"ChromeDriver が見つかりません: {self.tools.chromedriver}")
        if not self.tools.curl.exists():
            errs.append(f"curl が見つかりません: {self.tools.curl}")

        ftp_needed = any(p.ftp_upload_enabled for p in parts)
        if ftp_needed:
            c = self.cfg.common
            if not c.ftp_host or not c.ftp_username or not c.ftp_password:
                errs.append("FTP設定(ホスト/ユーザー/パスワード)が不足しています")
            errs.extend(validate_ftp_path_template(c.ftp_remote_path_template))

        print_needed = any(p.print_enabled for p in parts)
        printers = printer_list() if print_needed else []
        if print_needed and not self.tools.sumatra.exists():
            errs.append(f"SumatraPDF が見つかりません: {self.tools.sumatra}")

        names: set[str] = set()
        for p in parts:
            if not p.output_name.strip():
                errs.append(f"出力ファイル名が空です: {p.part_name}")
            name = p.resolved_name(today)
            if name in names:
                errs.append(f"yymmdd 展開後に重複しています: {name}")
            names.add(name)

            out_dir = Path(p.output_dir)
            try:
                out_dir.mkdir(parents=True, exist_ok=True)
            except Exception as exc:
                errs.append(f"出力先を作成できません: {out_dir}: {exc}")

            if p.print_enabled and p.printer_name and printers and p.printer_name not in printers:
                errs.append(f"指定プリンタが存在しません: {p.part_name}: {p.printer_name}")

        return errs

    def _start_run(self, parts: list[PartConfig]) -> None:
        if self.runner:
            messagebox.showwarning("実行中", "現在実行中です")
            return
        valid: list[PartConfig] = []
        seen = set()
        for p in parts:
            if not p.enabled:
                continue
            name = p.resolved_name(datetime.now().strftime("%y%m%d"))
            if name in seen:
                self._log(f"重複ファイル名警告: {name}")
            seen.add(name)
            valid.append(p)
        if not valid:
            messagebox.showwarning("対象なし", "実行対象がありません")
            return

        errors = self._validate_before_run(valid)
        if errors:
            messagebox.showerror("実行前バリデーション", "\n".join(errors))
            for e in errors:
                self._log(f"実行前エラー: {e}")
            return

        self.runner = Runner(self.cfg, self.tools, self.queue)
        self.runner.run_async(valid)

    def run_single(self) -> None:
        if not self.selected_ids:
            return
        self._start_run([self.cfg.parts[self.selected_ids[0]]])

    def run_selected(self) -> None:
        self._start_run([p for p in self.cfg.parts if p.selected])

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
        p = self.cfg.parts[self.selected_ids[0]]
        messagebox.showinfo("プレビュー", f"対象: {p.part_name}\n向き: {p.orientation}\n倍率: {p.scale}")

    def stop_run(self) -> None:
        if self.runner:
            self.runner.stop()

    def open_output_dir(self) -> None:
        if not self.selected_ids:
            return
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
                    self._lock_editing(True)
                elif kind == "progress":
                    total = max(payload["total"], 1)
                    self.progress_var.set(payload["value"] / total * 100)
                    self.status_var.set(payload["text"])
                elif kind == "log":
                    self._log(payload["text"])
                elif kind == "done":
                    results = payload["results"]
                    self._show_result_dialog(results)
                    self.runner = None
                    self._lock_editing(False)
        except Empty:
            pass
        self.root.after(150, self._poll_queue)

    def _show_result_dialog(self, results) -> None:
        dlg = tk.Toplevel(self.root)
        dlg.title("実行結果")
        txt = tk.Text(dlg, width=110, height=22)
        txt.pack(fill=tk.BOTH, expand=True)
        failed = []
        for r in results:
            txt.insert(tk.END, f"{'OK' if r.success else 'NG'} | {r.part_name} | {r.message}\n")
            for status in r.file_statuses:
                txt.insert(
                    tk.END,
                    f"  - {status.file_path.name}: 保存済み / ローカル={status.local_copy} / FTP={status.ftp} / 印刷={status.print_status}\n",
                )
            if not r.file_statuses:
                txt.insert(tk.END, "  - 出力ファイルなし\n")
            if not r.success:
                txt.insert(tk.END, f"  - 例外詳細: {r.message}\n")
                failed.append(r.part_name)
            txt.insert(tk.END, "\n")

        def rerun_failed():
            targets = [p for p in self.cfg.parts if p.part_name in failed]
            dlg.destroy()
            self._start_run(targets)

        ttk.Button(dlg, text="失敗分のみ再実行", command=rerun_failed).pack(side=tk.LEFT)
        ttk.Button(dlg, text="閉じる", command=dlg.destroy).pack(side=tk.RIGHT)

    def _lock_editing(self, lock: bool) -> None:
        state = "disabled" if lock else "normal"
        for child in self.root.winfo_children():
            if isinstance(child, ttk.Button):
                if child.cget("text") not in {"停止"}:
                    child.configure(state=state)

    def _log(self, text: str) -> None:
        self.log_text.insert(tk.END, text + "\n")
        self.log_text.see(tk.END)

    def on_close(self) -> None:
        self.cfg.common.window_geometry = self.root.geometry()
        self.save_settings()
        self.root.destroy()

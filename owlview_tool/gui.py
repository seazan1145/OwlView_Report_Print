from __future__ import annotations

import os
import json
import hashlib
import traceback
import urllib.request
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from queue import Empty, Queue

import tkinter as tk
from PIL import Image, ImageDraw, ImageTk
from tkinter import filedialog, messagebox, simpledialog, ttk

from .config_store import ConfigStore
from .executor import Runner
from .models import AppConfig, PartConfig
from .services import ExternalTools, ftp_test_connection, printer_list, render_pdf_first_page_image, resolve_tool_path, resolved_remote_path, validate_ftp_path_template

FORMAT_LABELS = {"pdf": "PDF", "jpg": "JPG", "jpg&pdf": "JPGとPDF"}
FORMAT_FROM_LABEL = {v: k for k, v in FORMAT_LABELS.items()}
ORIENTATION_LABELS = {"portrait": "縦", "landscape": "横"}
ORIENTATION_FROM_LABEL = {v: k for k, v in ORIENTATION_LABELS.items()}


def shorten_path(value: str, max_len: int = 48) -> str:
    if len(value) <= max_len:
        return value
    return f"...{value[-(max_len-3):]}"


def safe_geometry(raw: str, screen_w: int, screen_h: int, default_size: tuple[int, int] = (1200, 760)) -> str:
    width, height = default_size
    x = max(0, int((screen_w - width) / 2))
    y = max(0, int((screen_h - height) / 4))
    try:
        size_pos = raw.strip()
        size, *pos = size_pos.split("+")
        w_s, h_s = size.split("x")
        width = int(w_s)
        height = int(h_s)
        if pos:
            x = int(pos[0])
        if len(pos) > 1:
            y = int(pos[1])
    except Exception:
        pass

    min_w, min_h = 900, 580
    max_w = max(min_w, int(screen_w * 0.98))
    max_h = max(min_h, int(screen_h * 0.95))
    width = max(min_w, min(width, max_w))
    height = max(min_h, min(height, max_h))
    x = max(0, min(x, max(0, screen_w - width)))
    y = max(0, min(y, max(0, screen_h - height)))
    return f"{width}x{height}+{x}+{y}"


class PdfPreviewWindow:
    def __init__(self, app: "OwlViewApp") -> None:
        self.app = app
        self.win = tk.Toplevel(app.root)
        self.win.title("印刷プレビュー")
        self.win.geometry("980x660")
        self.current_part_index = -1
        self.preview_part: PartConfig | None = None
        self.pdf_path: Path | None = None
        self.page_index = 0
        self.zoom = 1.0
        self.base_image: Image.Image | None = None
        self.photo: ImageTk.PhotoImage | None = None

        left = ttk.LabelFrame(self.win, text="設定")
        left.pack(side=tk.LEFT, fill=tk.Y, padx=3, pady=3)
        left.configure(width=190)
        left.pack_propagate(False)
        right = ttk.Frame(self.win)
        right.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=3, pady=3)

        self.vars = {
            "scale": tk.DoubleVar(value=100.0),
            "margin_top": tk.DoubleVar(value=0.0),
            "margin_bottom": tk.DoubleVar(value=0.0),
            "margin_left": tk.DoubleVar(value=0.0),
            "margin_right": tk.DoubleVar(value=0.0),
            "orientation": tk.StringVar(value="縦"),
            "print_range": tk.StringVar(value=""),
        }
        rows = [
            ("拡縮率(%)", "scale"),
            ("余白 上", "margin_top"),
            ("余白 下", "margin_bottom"),
            ("余白 左", "margin_left"),
            ("余白 右", "margin_right"),
            ("印刷範囲", "print_range"),
        ]
        r = 0
        for label, key in rows:
            ttk.Label(left, text=label).grid(row=r, column=0, sticky="w", padx=2, pady=1)
            ttk.Entry(left, textvariable=self.vars[key], width=10).grid(row=r, column=1, sticky="ew", padx=2, pady=1)
            r += 1
        ttk.Label(left, text="向き").grid(row=r, column=0, sticky="w", padx=2, pady=1)
        ttk.Combobox(left, textvariable=self.vars["orientation"], values=["縦", "横"], state="readonly", width=8).grid(row=r, column=1, sticky="ew", padx=2, pady=1)
        r += 1

        ttk.Button(left, text="再読込", command=self.reload_pdf).grid(row=r, column=0, columnspan=2, sticky="ew", padx=2, pady=(6, 2))
        r += 1
        ttk.Button(left, text="設定を保存", command=self.save_to_part).grid(row=r, column=0, columnspan=2, sticky="ew", padx=2, pady=2)

        bar = ttk.Frame(right)
        bar.pack(fill=tk.X)
        ttk.Button(bar, text="拡大+", command=lambda: self.change_zoom(1.2)).pack(side=tk.LEFT, padx=2)
        ttk.Button(bar, text="縮小-", command=lambda: self.change_zoom(1 / 1.2)).pack(side=tk.LEFT, padx=2)
        ttk.Button(bar, text="前ページ", command=lambda: self.move_page(-1)).pack(side=tk.LEFT, padx=2)
        ttk.Button(bar, text="次ページ", command=lambda: self.move_page(1)).pack(side=tk.LEFT, padx=2)
        self.status = tk.StringVar(value="未表示")
        ttk.Label(bar, textvariable=self.status).pack(side=tk.RIGHT)

        self.canvas = tk.Canvas(right, bg="#202020")
        self.canvas.pack(fill=tk.BOTH, expand=True)

    def load_for_part(self, idx: int, part: PartConfig) -> None:
        self.current_part_index = idx
        self.preview_part = PartConfig(**asdict(part))
        self.vars["scale"].set(self.preview_part.scale)
        self.vars["margin_top"].set(self.preview_part.margin_top)
        self.vars["margin_bottom"].set(self.preview_part.margin_bottom)
        self.vars["margin_left"].set(self.preview_part.margin_left)
        self.vars["margin_right"].set(self.preview_part.margin_right)
        self.vars["print_range"].set(self.preview_part.print_range)
        self.vars["orientation"].set(ORIENTATION_LABELS.get(self.preview_part.orientation, "縦"))
        self.reload_pdf()

    def _sync_part_vars(self) -> None:
        if not self.preview_part:
            return
        self.preview_part.scale = float(self.vars["scale"].get())
        self.preview_part.margin_top = float(self.vars["margin_top"].get())
        self.preview_part.margin_bottom = float(self.vars["margin_bottom"].get())
        self.preview_part.margin_left = float(self.vars["margin_left"].get())
        self.preview_part.margin_right = float(self.vars["margin_right"].get())
        self.preview_part.print_range = self.vars["print_range"].get().strip()
        self.preview_part.orientation = ORIENTATION_FROM_LABEL[self.vars["orientation"].get()]

    def reload_pdf(self) -> None:
        if not self.preview_part:
            return
        self._sync_part_vars()
        try:
            self.app._set_preview_progress(10, "プレビューPDF生成を開始")
            pdf_path = self.app.generate_preview_pdf(self.preview_part)
            self.app._set_preview_progress(65, "PDFを画像化しています")
            self.pdf_path = pdf_path
            self.page_index = 0
            self.base_image = render_pdf_first_page_image(pdf_path, dpi=170)
            pages = self.app._pdf_page_count(pdf_path)
            self._draw()
            self.status.set(f"再読込完了: {pdf_path.name}")
            if pages > 1:
                warn = f"拡縮率/余白設定により {pages} ページになっています。1ページに収まりません。"
                messagebox.showwarning("プレビュー警告", warn)
                self.app._log(warn)
            self.app._set_preview_progress(100, "プレビュー完了")
        except Exception as exc:
            self.status.set(f"失敗: {exc}")
            self.app._append_stacktrace(exc)
            self.app._set_preview_progress(0, "プレビュー失敗")

    def _draw(self) -> None:
        if not self.base_image:
            return
        img = self.base_image.copy()
        if self.preview_part:
            draw = ImageDraw.Draw(img)
            pad_l = int(self.preview_part.margin_left * 20)
            pad_r = int(self.preview_part.margin_right * 20)
            pad_t = int(self.preview_part.margin_top * 20)
            pad_b = int(self.preview_part.margin_bottom * 20)
            draw.rectangle((pad_l, pad_t, img.width - pad_r, img.height - pad_b), outline="#d22", width=3)
        zw = max(1, int(img.width * self.zoom))
        zh = max(1, int(img.height * self.zoom))
        img = img.resize((zw, zh), Image.Resampling.LANCZOS)
        self.photo = ImageTk.PhotoImage(img)
        self.canvas.delete("all")
        self.canvas.create_image(10, 10, image=self.photo, anchor="nw")

    def change_zoom(self, ratio: float) -> None:
        self.zoom = max(0.2, min(5.0, self.zoom * ratio))
        self._draw()

    def move_page(self, _offset: int) -> None:
        messagebox.showinfo("印刷プレビュー", "現バージョンでは1ページ目を表示します。")

    def save_to_part(self) -> None:
        if self.current_part_index < 0 or not self.preview_part:
            return
        self._sync_part_vars()
        self.app.cfg.parts[self.current_part_index].scale = self.preview_part.scale
        self.app.cfg.parts[self.current_part_index].margin_top = self.preview_part.margin_top
        self.app.cfg.parts[self.current_part_index].margin_bottom = self.preview_part.margin_bottom
        self.app.cfg.parts[self.current_part_index].margin_left = self.preview_part.margin_left
        self.app.cfg.parts[self.current_part_index].margin_right = self.preview_part.margin_right
        self.app.cfg.parts[self.current_part_index].print_range = self.preview_part.print_range
        self.app.cfg.parts[self.current_part_index].orientation = self.preview_part.orientation
        self.app.auto_save()
        self.app._refresh_part_list()
        self.status.set("設定を保存しました")


class OwlViewApp:
    def __init__(self, root: tk.Tk, base_dir: Path) -> None:
        self.root = root
        self.base_dir = base_dir
        self.store = ConfigStore(base_dir)
        self.cfg: AppConfig = self.store.load()
        self.queue: Queue = Queue()
        self.runner: Runner | None = None
        self.preview_window: PdfPreviewWindow | None = None
        self.preview_temp_files: list[Path] = []
        self.preview_cache: dict[str, Path] = {}
        self.preview_image_cache: dict[str, Image.Image] = {}
        self.inline_preview_base: Image.Image | None = None
        self.inline_preview_part: PartConfig | None = None
        self.inline_preview_photo: ImageTk.PhotoImage | None = None

        self.search_var = tk.StringVar()
        self.status_var = tk.StringVar(value="Ready")
        self.progress_var = tk.DoubleVar(value=0)
        self.detail_path_var = tk.StringVar(value="")
        self.selected_ids: list[int] = []
        self.main_ftp_var = tk.BooleanVar(value=True)
        self.main_print_var = tk.BooleanVar(value=True)
        self.excel_only_var = tk.BooleanVar(value=False)
        self.main_printer_var = tk.StringVar(value=self.cfg.app.default_printer_name)
        self.main_ftp_var.set(bool(self.cfg.job.ftp_default_enabled))
        self.main_print_var.set(bool(self.cfg.job.print_default_enabled))
        self.preview_zoom_var = tk.DoubleVar(value=float(self.cfg.ui.preview_zoom or 1.0))

        self.tools = self._resolve_tools()
        self._configure_styles()
        self._build_ui()
        self._refresh_printer_combo()
        self._refresh_part_list()
        self._log_missing_tools()
        self._show_missing_tool_dialog()
        self._poll_queue()

    def _resolve_tools(self) -> ExternalTools:
        data = self.base_dir / "Data"
        c = self.cfg.common
        return ExternalTools(
            curl=resolve_tool_path(c.curl_path, data / "curl" / "curl.exe", "curl.exe"),
            sumatra=resolve_tool_path(c.sumatra_path, data / "SumatraPDF" / "SumatraPDF.exe", "SumatraPDF.exe"),
        )

    def _log_missing_tools(self) -> None:
        for name, path in [("curl", self.tools.curl), ("SumatraPDF", self.tools.sumatra)]:
            if not path.exists():
                self._log(f"{name} が見つかりません。設定の明示パスまたはPATHを確認してください: {path}")

    def _show_missing_tool_dialog(self) -> None:
        missing: list[str] = []
        for name, path in [("curl", self.tools.curl), ("SumatraPDF", self.tools.sumatra)]:
            if not path.exists():
                missing.append(f"- {name}: {path}")
        if not missing:
            return
        messagebox.showwarning(
            "外部ツール未検出",
            "以下の実行ファイルが見つかりません。\n"
            "Data同梱なし構成では、詳細設定の明示パスまたはPATHに配置してください。\n\n"
            + "\n".join(missing),
        )

    def _configure_styles(self) -> None:
        self.style = ttk.Style(self.root)
        try:
            self.style.theme_use("vista")
        except Exception:
            pass
        self.style.configure("Primary.TButton", padding=(10, 6), foreground="#ffffff", background="#0b63ce")
        self.style.map(
            "Primary.TButton",
            background=[("active", "#1d74df"), ("pressed", "#0954af"), ("disabled", "#b4c6df")],
            foreground=[("disabled", "#f4f7fb")],
        )
        self.style.configure("Success.TButton", padding=(10, 6), foreground="#ffffff", background="#2e7d32")
        self.style.map(
            "Success.TButton",
            background=[("active", "#3a9140"), ("pressed", "#1f6c27"), ("disabled", "#bfd9c1")],
            foreground=[("disabled", "#f4f8f4")],
        )
        self.style.configure("Danger.TButton", padding=(10, 6), foreground="#ffffff", background="#c62828")
        self.style.map(
            "Danger.TButton",
            background=[("active", "#d53d3d"), ("pressed", "#a51f1f"), ("disabled", "#e2baba")],
            foreground=[("disabled", "#fff7f7")],
        )
        self.style.configure("Warning.TButton", padding=(10, 6), foreground="#ffffff", background="#cc6e14")
        self.style.map(
            "Warning.TButton",
            background=[("active", "#dc7f25"), ("pressed", "#b75e0d"), ("disabled", "#ead0b7")],
            foreground=[("disabled", "#fff9f3")],
        )
        self.style.configure("Secondary.TButton", padding=(10, 6), foreground="#1f2937", background="#e5e7eb")
        self.style.map(
            "Secondary.TButton",
            background=[("active", "#d7dbe2"), ("pressed", "#c4cbd6"), ("disabled", "#f0f2f6")],
            foreground=[("disabled", "#9ca3af")],
        )
        self.style.configure("PartTree.Treeview", rowheight=26)
        self.style.map(
            "PartTree.Treeview",
            background=[("selected", "#cfe5ff")],
            foreground=[("selected", "#0f172a")],
        )

    @staticmethod
    def _color_button(parent, text: str, command, *, bg: str, active: str, fg: str = "#ffffff", width: int = 7) -> tk.Button:
        return tk.Button(
            parent,
            text=text,
            command=command,
            bg=bg,
            activebackground=active,
            fg=fg,
            activeforeground=fg,
            relief=tk.FLAT,
            bd=0,
            padx=10,
            pady=6,
            width=width,
            font=("Yu Gothic UI", 9, "bold"),
            disabledforeground="#d1d5db",
        )

    def _build_ui(self) -> None:
        self.root.title("OwlView 自動出力ツール")
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        self.root.geometry(safe_geometry(self.cfg.ui.window_geometry or "1200x760+80+40", sw, sh))
        if self.cfg.ui.window_maximized:
            try:
                self.root.state("zoomed")
            except tk.TclError:
                pass
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        main = ttk.Frame(self.root, padding=4)
        main.pack(fill=tk.BOTH, expand=True)
        main.columnconfigure(0, weight=5)
        main.columnconfigure(1, weight=4)
        main.rowconfigure(0, weight=1)

        left = ttk.LabelFrame(main, text="A. パート一覧", padding=4)
        left.grid(row=0, column=0, sticky="nsew")
        right = ttk.LabelFrame(main, text="B. プレビュー / 実行", padding=4)
        right.grid(row=0, column=1, sticky="nsew", padx=(6, 0))
        bottom = ttk.LabelFrame(main, text="C. 実行ログ", padding=4)
        bottom.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(6, 0))
        main.rowconfigure(1, weight=0)

        self._build_parts_area(left)
        self._build_main_controls(right)
        self._build_log_area(bottom)

    def _build_parts_area(self, parent: ttk.Frame) -> None:
        f = ttk.Frame(parent)
        f.pack(fill=tk.X)
        ttk.Label(f, text="検索").pack(side=tk.LEFT)
        e = ttk.Entry(f, textvariable=self.search_var)
        e.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=4)
        e.bind("<KeyRelease>", lambda _e: self._refresh_part_list())

        cols = ("enabled", "selected", "part_name", "output_name", "output_dir", "format", "orientation", "scale")
        self.tree = ttk.Treeview(parent, columns=cols, show="headings", selectmode="extended", height=17, style="PartTree.Treeview")
        headers = {
            "enabled": "使用",
            "selected": "実行対象",
            "part_name": "パート名",
            "output_name": "保存名",
            "output_dir": "保存先",
            "format": "出力形式",
            "orientation": "向き",
            "scale": "倍率",
        }
        widths = {"enabled": 60, "selected": 80, "part_name": 220, "output_name": 180, "output_dir": 260, "format": 100, "orientation": 70, "scale": 70}
        for c in cols:
            anchor = tk.CENTER if c in {"enabled", "selected", "format", "orientation", "scale"} else tk.W
            self.tree.heading(c, text=headers[c], anchor=anchor)
            self.tree.column(c, width=widths[c], anchor=anchor)
        self.tree.tag_configure("run_target", background="#edf6ff")
        self.tree.tag_configure("disabled_row", foreground="#8b93a1")
        self.tree.pack(fill=tk.BOTH, expand=True, pady=6)
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)
        self.tree.bind("<Double-1>", self._toggle_on_double_click)

        b1 = ttk.Frame(parent)
        b1.pack(fill=tk.X)
        for label, cmd in [
            ("追加", self.add_part), ("編集", self.edit_part), ("複製", self.duplicate_selected), ("削除", self.delete_selected),
            ("↑", lambda: self.move_selected(-1)), ("↓", lambda: self.move_selected(1)),
        ]:
            if label == "削除":
                self._color_button(b1, label, cmd, bg="#d97706", active="#b45309").pack(side=tk.LEFT, padx=2)
            elif label in {"追加", "編集", "複製"}:
                self._color_button(b1, label, cmd, bg="#2e7d32", active="#256b2a").pack(side=tk.LEFT, padx=2)
            else:
                ttk.Button(b1, text=label, command=cmd, style="Secondary.TButton").pack(side=tk.LEFT, padx=2)

        b2 = ttk.Frame(parent)
        b2.pack(fill=tk.X, pady=(4, 0))
        ttk.Button(b2, text="一括ON", command=self.select_all_parts, style="Secondary.TButton").pack(side=tk.LEFT, padx=2)
        ttk.Button(b2, text="一括OFF", command=self.clear_all_parts, style="Secondary.TButton").pack(side=tk.LEFT, padx=2)
        ttk.Label(b2, textvariable=self.detail_path_var).pack(side=tk.RIGHT)

    def _build_main_controls(self, parent: ttk.Frame) -> None:
        preview = ttk.LabelFrame(parent, text="プレビュー", padding=6)
        preview.pack(fill=tk.BOTH, expand=True)
        pbar = ttk.Frame(preview)
        pbar.pack(fill=tk.X, pady=(0, 4))
        ttk.Button(pbar, text="更新", command=self.refresh_inline_preview, style="Secondary.TButton").pack(side=tk.LEFT, padx=2)
        ttk.Button(pbar, text="拡大+", command=lambda: self._change_inline_zoom(1.15), style="Secondary.TButton").pack(side=tk.LEFT, padx=2)
        ttk.Button(pbar, text="縮小-", command=lambda: self._change_inline_zoom(1 / 1.15), style="Secondary.TButton").pack(side=tk.LEFT, padx=2)
        ttk.Button(pbar, text="別窓", command=self.open_preview_window, style="Secondary.TButton").pack(side=tk.LEFT, padx=2)
        self.inline_preview_status = tk.StringVar(value="パート選択でプレビュー可能")
        ttk.Label(pbar, textvariable=self.inline_preview_status).pack(side=tk.RIGHT)
        self.inline_canvas = tk.Canvas(preview, bg="#202020", height=320)
        self.inline_canvas.pack(fill=tk.BOTH, expand=True)

        ops = ttk.LabelFrame(parent, text="主操作", padding=8)
        ops.pack(fill=tk.X, pady=(8, 0))
        run_row = ttk.Frame(ops)
        run_row.pack(fill=tk.X, pady=(0, 8))
        ttk.Label(run_row, text="実行").pack(side=tk.LEFT, padx=(0, 8))
        self._color_button(run_row, "個別", self.run_single, bg="#0b63ce", active="#0a57b5", width=8).pack(side=tk.LEFT, padx=3)
        self._color_button(run_row, "選択", self.run_selected, bg="#0b63ce", active="#0a57b5", width=8).pack(side=tk.LEFT, padx=3)
        self._color_button(run_row, "範囲", self.run_range, bg="#0b63ce", active="#0a57b5", width=8).pack(side=tk.LEFT, padx=3)
        self._color_button(run_row, "全件", self.run_all, bg="#084c9e", active="#073f82", width=10).pack(side=tk.LEFT, padx=(6, 3))

        stop_row = ttk.Frame(ops)
        stop_row.pack(fill=tk.X, pady=(0, 8))
        ttk.Label(stop_row, text="停止").pack(side=tk.LEFT, padx=(0, 8))
        self._color_button(stop_row, "停止", self.stop_run, bg="#c62828", active="#a61f1f", width=12).pack(side=tk.LEFT, padx=(6, 3))

        attach_row = ttk.Frame(ops)
        attach_row.pack(fill=tk.X)
        ttk.Label(attach_row, text="付帯設定").pack(side=tk.LEFT, padx=(0, 8))
        ttk.Checkbutton(attach_row, text="Excel出力のみ実行", variable=self.excel_only_var).pack(side=tk.LEFT, padx=2)
        ttk.Checkbutton(attach_row, text="FTP", variable=self.main_ftp_var).pack(side=tk.LEFT, padx=2)
        ttk.Checkbutton(attach_row, text="印刷", variable=self.main_print_var).pack(side=tk.LEFT, padx=2)
        ttk.Label(attach_row, text="プリンタ").pack(side=tk.LEFT, padx=(8, 2))
        self.printer_combo = ttk.Combobox(attach_row, textvariable=self.main_printer_var, state="readonly", width=18)
        self.printer_combo.pack(side=tk.LEFT, padx=2)
        ttk.Button(attach_row, text="再取得", command=self.reload_printers, style="Secondary.TButton").pack(side=tk.LEFT, padx=4)

        util = ttk.Frame(parent)
        util.pack(fill=tk.X, pady=(8, 0))
        ttk.Separator(parent, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=(6, 6))
        ttk.Button(util, text="詳細設定", command=self.open_detail_settings, style="Secondary.TButton").pack(side=tk.LEFT, padx=2)
        ttk.Button(util, text="環境チェック", command=self.run_environment_check, style="Secondary.TButton").pack(side=tk.LEFT, padx=2)
        ttk.Button(util, text="出力先を開く", command=self.open_output_dir, style="Secondary.TButton").pack(side=tk.LEFT, padx=2)

    def _build_log_area(self, parent: ttk.Frame) -> None:
        top = ttk.Frame(parent)
        top.pack(fill=tk.X)
        ttk.Progressbar(top, variable=self.progress_var, maximum=100, length=220).pack(side=tk.RIGHT)
        self.log_text = tk.Text(parent, height=10)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        self.log_text.tag_configure("success", foreground="#1b5e20")
        self.log_text.tag_configure("warning", foreground="#b45309")
        self.log_text.tag_configure("error", foreground="#b91c1c")
        self.log_text.tag_configure("part", foreground="#1e40af", font=("Yu Gothic UI", 9, "bold"))
        ttk.Label(parent, textvariable=self.status_var).pack(anchor="w")

    def _render_part_summary_line(self, summary) -> str:
        started = summary.started_at.strftime("%H:%M:%S")
        elapsed = f"{summary.elapsed_sec:.1f}s"
        error = f" / エラー: {summary.error_summary}" if summary.error_summary else ""
        return (
            f"[PART] {summary.part_name} 開始:{started} "
            f"PDF:{summary.pdf} JPG:{summary.jpg} FTP:{summary.ftp} 印刷:{summary.printing} "
            f"出力先:{summary.output_dir} 所要:{elapsed}{error}"
        )

    def _set_preview_progress(self, value: float, text: str) -> None:
        self.progress_var.set(max(0.0, min(100.0, value)))
        self.status_var.set(text)
        self.root.update_idletasks()

    @staticmethod
    def _pdf_page_count(pdf_path: Path) -> int:
        try:
            import fitz  # type: ignore
        except Exception:
            return 1
        doc = fitz.open(pdf_path)
        try:
            return max(1, int(doc.page_count))
        finally:
            doc.close()

    def _change_inline_zoom(self, ratio: float) -> None:
        now = float(self.preview_zoom_var.get())
        self.preview_zoom_var.set(max(0.2, min(4.0, now * ratio)))
        self._draw_inline_preview()

    def _base_preview_image(self, pdf_path: Path, dpi: int = 160) -> Image.Image:
        stat = pdf_path.stat()
        image_key = f"{pdf_path}:{int(stat.st_mtime)}:{dpi}"
        cached = self.preview_image_cache.get(image_key)
        if cached is not None:
            return cached.copy()
        rendered = render_pdf_first_page_image(pdf_path, dpi=dpi)
        self.preview_image_cache[image_key] = rendered
        if len(self.preview_image_cache) > 8:
            oldest = next(iter(self.preview_image_cache.keys()))
            self.preview_image_cache.pop(oldest, None)
        return rendered.copy()

    def _draw_inline_preview(self) -> None:
        if not self.inline_preview_base:
            return
        img = self.inline_preview_base.copy()
        if self.inline_preview_part:
            draw = ImageDraw.Draw(img)
            p = self.inline_preview_part
            pad_l = int(p.margin_left * 20)
            pad_r = int(p.margin_right * 20)
            pad_t = int(p.margin_top * 20)
            pad_b = int(p.margin_bottom * 20)
            draw.rectangle((pad_l, pad_t, img.width - pad_r, img.height - pad_b), outline="#d22", width=3)
        zoom = float(self.preview_zoom_var.get())
        zw = max(1, int(img.width * zoom))
        zh = max(1, int(img.height * zoom))
        img = img.resize((zw, zh), Image.Resampling.LANCZOS)
        self.inline_preview_photo = ImageTk.PhotoImage(img)
        self.inline_canvas.delete("all")
        self.inline_canvas.create_image(8, 8, image=self.inline_preview_photo, anchor="nw")

    def refresh_inline_preview(self) -> None:
        if not self.selected_ids:
            self.inline_preview_status.set("パートを選択してください")
            return
        part = self.cfg.parts[self.selected_ids[0]]
        self.inline_preview_part = PartConfig(**asdict(part))
        try:
            self._set_preview_progress(10, "プレビューPDF生成を開始")
            pdf_path = self.generate_preview_pdf(self.inline_preview_part)
            self._set_preview_progress(70, "PDFを描画しています")
            self.inline_preview_base = self._base_preview_image(pdf_path)
            self._draw_inline_preview()
            self.inline_preview_status.set(f"表示中: {pdf_path.name}")
            self._set_preview_progress(100, "プレビュー完了")
        except Exception as exc:
            self.inline_preview_status.set(f"失敗: {exc}")
            self._append_stacktrace(exc)
            self._set_preview_progress(0, "プレビュー失敗")

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
                tags=tuple(
                    tag
                    for tag, enabled in [("run_target", p.selected and p.enabled), ("disabled_row", not p.enabled)]
                    if enabled
                ),
                values=("✓" if p.enabled else "-", "✓" if p.selected else "-", p.part_name, p.output_name, shorten_path(p.output_dir), FORMAT_LABELS.get(p.output_format, p.output_format), ORIENTATION_LABELS.get(p.orientation, p.orientation), f"{p.scale:g}"),
            )

    def _on_tree_select(self, _e=None) -> None:
        self.selected_ids = [int(i) for i in self.tree.selection()]
        if self.selected_ids:
            self.detail_path_var.set(self.cfg.parts[self.selected_ids[0]].output_dir)
            if self.cfg.ui.preview_auto_refresh:
                self.refresh_inline_preview()

    def _toggle_on_double_click(self, e) -> None:
        row = self.tree.identify_row(e.y)
        col = self.tree.identify_column(e.x)
        if not row:
            return
        idx = int(row)
        if col == "#1":
            self.cfg.parts[idx].enabled = not self.cfg.parts[idx].enabled
        elif col == "#2":
            self.cfg.parts[idx].selected = not self.cfg.parts[idx].selected
        else:
            return
        self._refresh_part_list()
        self.auto_save()

    def _part_dialog(self, part: PartConfig | None = None) -> PartConfig | None:
        d = tk.Toplevel(self.root)
        d.title("パート編集")
        d.geometry("600x560")
        d.columnconfigure(1, weight=1)
        p = part or PartConfig()
        vars = {
            "enabled": tk.BooleanVar(value=p.enabled),
            "selected": tk.BooleanVar(value=p.selected),
            "part_name": tk.StringVar(value=p.part_name),
            "output_name": tk.StringVar(value=p.output_name),
            "output_dir": tk.StringVar(value=p.output_dir),
            "format": tk.StringVar(value=FORMAT_LABELS.get(p.output_format, "PDF")),
            "scale": tk.DoubleVar(value=p.scale),
            "orientation": tk.StringVar(value=ORIENTATION_LABELS.get(p.orientation, "縦")),
            "print_range": tk.StringVar(value=p.print_range),
            "margin_top": tk.DoubleVar(value=p.margin_top),
            "margin_bottom": tk.DoubleVar(value=p.margin_bottom),
            "margin_left": tk.DoubleVar(value=p.margin_left),
            "margin_right": tk.DoubleVar(value=p.margin_right),
            "paper_width": tk.DoubleVar(value=p.paper_width),
            "paper_height": tk.DoubleVar(value=p.paper_height),
            "jpg_quality": tk.IntVar(value=p.jpg_quality),
            "print_copies": tk.IntVar(value=p.print_copies or 1),
            "enable_inputtable_excel_export": tk.BooleanVar(value=p.enable_inputtable_excel_export),
            "inputtable_excel_output_dir": tk.StringVar(value=p.inputtable_excel_output_dir),
        }

        row = 0
        for label, key in [("パート名", "part_name"), ("保存名", "output_name"), ("保存先", "output_dir")]:
            ttk.Label(d, text=label).grid(row=row, column=0, sticky="w", padx=6, pady=3)
            ttk.Entry(d, textvariable=vars[key]).grid(row=row, column=1, sticky="ew", padx=6)
            row += 1
        ttk.Button(d, text="参照", command=lambda: vars["output_dir"].set(filedialog.askdirectory() or vars["output_dir"].get())).grid(row=2, column=2, padx=6)

        ttk.Label(d, text="出力形式").grid(row=row, column=0, sticky="w", padx=6, pady=3)
        ttk.Combobox(d, textvariable=vars["format"], values=["PDF", "JPG", "JPGとPDF"], state="readonly").grid(row=row, column=1, sticky="ew", padx=6)
        row += 1
        ttk.Label(d, text="向き").grid(row=row, column=0, sticky="w", padx=6, pady=3)
        ttk.Combobox(d, textvariable=vars["orientation"], values=["縦", "横"], state="readonly").grid(row=row, column=1, sticky="ew", padx=6)
        row += 1

        for label, key in [("倍率", "scale"), ("印刷範囲", "print_range"), ("余白 上", "margin_top"), ("余白 下", "margin_bottom"), ("余白 左", "margin_left"), ("余白 右", "margin_right"), ("用紙幅", "paper_width"), ("用紙高", "paper_height"), ("JPG品質", "jpg_quality"), ("印刷部数(0=共通)", "print_copies")]:
            ttk.Label(d, text=label).grid(row=row, column=0, sticky="w", padx=6, pady=3)
            ttk.Entry(d, textvariable=vars[key]).grid(row=row, column=1, sticky="ew", padx=6)
            row += 1

        ttk.Checkbutton(
            d,
            text="inputtable からExcelを出力してから report へ進む",
            variable=vars["enable_inputtable_excel_export"],
        ).grid(row=row, column=0, columnspan=2, sticky="w", padx=6, pady=3)
        row += 1
        ttk.Label(d, text="Excel保存先(yymmdd可)").grid(row=row, column=0, sticky="w", padx=6, pady=3)
        ttk.Entry(d, textvariable=vars["inputtable_excel_output_dir"]).grid(row=row, column=1, sticky="ew", padx=6)
        ttk.Button(d, text="参照", command=lambda: vars["inputtable_excel_output_dir"].set(filedialog.askdirectory() or vars["inputtable_excel_output_dir"].get())).grid(row=row, column=2, padx=6)
        row += 1

        ttk.Checkbutton(d, text="使用する", variable=vars["enabled"]).grid(row=row, column=1, sticky="w", padx=6); row += 1
        ttk.Checkbutton(d, text="実行対象に含める", variable=vars["selected"]).grid(row=row, column=1, sticky="w", padx=6); row += 1

        ok = {"v": False}
        ttk.Button(d, text="保存", command=lambda: (ok.__setitem__("v", True), d.destroy())).grid(row=row, column=1, sticky="e", padx=6, pady=8)
        d.transient(self.root); d.grab_set(); d.wait_window()
        if not ok["v"]:
            return None

        new = PartConfig(
            enabled=bool(vars["enabled"].get()),
            selected=bool(vars["selected"].get()),
            part_name=vars["part_name"].get(),
            output_name=vars["output_name"].get(),
            output_dir=vars["output_dir"].get(),
            output_format=FORMAT_FROM_LABEL[vars["format"].get()],
            scale=float(vars["scale"].get()),
            orientation=ORIENTATION_FROM_LABEL[vars["orientation"].get()],
            print_range=vars["print_range"].get(),
            margin_top=float(vars["margin_top"].get()),
            margin_bottom=float(vars["margin_bottom"].get()),
            margin_left=float(vars["margin_left"].get()),
            margin_right=float(vars["margin_right"].get()),
            paper_width=float(vars["paper_width"].get()),
            paper_height=float(vars["paper_height"].get()),
            jpg_quality=int(vars["jpg_quality"].get()),
            local_copy_enabled=True,
            print_copies=int(vars["print_copies"].get()),
            enable_inputtable_excel_export=bool(vars["enable_inputtable_excel_export"].get()),
            inputtable_excel_output_dir=vars["inputtable_excel_output_dir"].get().strip(),
        )
        errs = new.validate()
        if errs:
            messagebox.showerror("入力エラー", "\n".join(errs))
            return None
        return new

    # unchanged settings mostly
    def open_detail_settings(self) -> None:
        c = self.cfg.common
        d = tk.Toplevel(self.root); d.title("詳細設定"); d.geometry("640x520")
        dbg = c.debug
        vars = {"home": tk.StringVar(value=c.owlview_home_url), "report": tk.StringVar(value=c.owlview_report_url), "xpath": tk.StringVar(value=c.xpath_input_box), "home_xpath": tk.StringVar(value=c.xpath_home_input_box), "inputtable_xpath": tk.StringVar(value=c.xpath_inputtable_input_box), "report_ready_xpath": tk.StringVar(value=c.xpath_report_ready), "search_ready_xpath": tk.StringVar(value=c.xpath_search_ready), "wait": tk.IntVar(value=c.selenium_wait_sec), "local_dir": tk.StringVar(value=c.default_local_copy_dir), "curl": tk.StringVar(value=c.curl_path), "sumatra": tk.StringVar(value=c.sumatra_path), "ftp_default": tk.BooleanVar(value=self.cfg.job.ftp_default_enabled), "ftp_encryption": tk.StringVar(value=c.ftp_encryption), "ftp_host": tk.StringVar(value=c.ftp_host), "ftp_port": tk.IntVar(value=c.ftp_port), "ftp_user": tk.StringVar(value=c.ftp_username), "ftp_pass": tk.StringVar(value=c.ftp_password), "ftp_path": tk.StringVar(value=c.ftp_remote_path_template), "print_default": tk.BooleanVar(value=self.cfg.job.print_default_enabled), "default_printer": tk.StringVar(value=c.default_printer_name), "default_copies": tk.IntVar(value=c.default_print_copies), "auto_save": tk.BooleanVar(value=c.auto_save_settings), "strict_episode": tk.BooleanVar(value=c.excel_only_fail_on_episode_mismatch), "mismatch_suffix": tk.BooleanVar(value=c.inputtable_episode_mismatch_suffix), "dbg_enabled": tk.BooleanVar(value=dbg.enabled), "dbg_headless": tk.BooleanVar(value=dbg.headless), "dbg_verbose": tk.BooleanVar(value=dbg.verbose_log), "dbg_shot": tk.BooleanVar(value=dbg.save_screenshot_on_error), "dbg_html": tk.BooleanVar(value=dbg.save_html_on_error), "dbg_wait": tk.IntVar(value=dbg.selenium_wait_timeout), "dbg_settle": tk.DoubleVar(value=dbg.input_settle_wait), "dbg_report_direct": tk.BooleanVar(value=dbg.report_direct_navigation)}

        outer = ttk.Frame(d, padding=6)
        outer.pack(fill=tk.BOTH, expand=True)
        canvas = tk.Canvas(outer, highlightthickness=0)
        vbar = ttk.Scrollbar(outer, orient=tk.VERTICAL, command=canvas.yview)
        canvas.configure(yscrollcommand=vbar.set)
        vbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        body = ttk.Frame(canvas)
        canvas.create_window((0, 0), window=body, anchor="nw")
        body.bind("<Configure>", lambda _e: canvas.configure(scrollregion=canvas.bbox("all")))

        def _add_entry(frame, r, label, key, show=""):
            ttk.Label(frame, text=label).grid(row=r, column=0, sticky="w", padx=4, pady=2)
            ttk.Entry(frame, textvariable=vars[key], show=show).grid(row=r, column=1, sticky="ew", padx=4, pady=2)

        sec_web = ttk.LabelFrame(body, text="OwlView/Selenium", padding=6); sec_web.pack(fill=tk.X, pady=(0, 6)); sec_web.columnconfigure(1, weight=1)
        _add_entry(sec_web, 0, "Home URL", "home")
        _add_entry(sec_web, 1, "Report URL", "report")
        _add_entry(sec_web, 2, "入力 XPath", "xpath")
        _add_entry(sec_web, 3, "Home 入力 XPath", "home_xpath")
        _add_entry(sec_web, 4, "inputtable 入力 XPath", "inputtable_xpath")
        _add_entry(sec_web, 5, "検索反映待機 XPath(任意)", "search_ready_xpath")
        _add_entry(sec_web, 6, "Report到達要素 XPath(任意)", "report_ready_xpath")
        _add_entry(sec_web, 7, "待機秒数", "wait")

        sec_tool = ttk.LabelFrame(body, text="外部ツール/出力", padding=6); sec_tool.pack(fill=tk.X, pady=(0, 6)); sec_tool.columnconfigure(1, weight=1)
        _add_entry(sec_tool, 0, "ローカルコピー先", "local_dir")
        _add_entry(sec_tool, 1, "curl パス", "curl")
        _add_entry(sec_tool, 2, "SumatraPDF パス", "sumatra")

        sec_ftp = ttk.LabelFrame(body, text="FTP", padding=6); sec_ftp.pack(fill=tk.X, pady=(0, 6)); sec_ftp.columnconfigure(1, weight=1)
        ttk.Label(sec_ftp, text="FTPデフォルト").grid(row=0, column=0, sticky="w", padx=4, pady=2)
        ttk.Checkbutton(sec_ftp, variable=vars["ftp_default"]).grid(row=0, column=1, sticky="w")
        ttk.Label(sec_ftp, text="Encryption").grid(row=1, column=0, sticky="w", padx=4, pady=2)
        ttk.Combobox(sec_ftp, textvariable=vars["ftp_encryption"], values=["Implicit TLS/SSL", "Explicit TLS/SSL", "None"], state="readonly").grid(row=1, column=1, sticky="ew", padx=4, pady=2)
        _add_entry(sec_ftp, 2, "Host", "ftp_host")
        _add_entry(sec_ftp, 3, "Port", "ftp_port")
        _add_entry(sec_ftp, 4, "Username", "ftp_user")
        _add_entry(sec_ftp, 5, "Password", "ftp_pass", show="*")
        _add_entry(sec_ftp, 6, "Remote Path Template", "ftp_path")

        sec_print = ttk.LabelFrame(body, text="印刷/保存", padding=6); sec_print.pack(fill=tk.X); sec_print.columnconfigure(1, weight=1)
        ttk.Label(sec_print, text="印刷デフォルト").grid(row=0, column=0, sticky="w", padx=4, pady=2)
        ttk.Checkbutton(sec_print, variable=vars["print_default"]).grid(row=0, column=1, sticky="w")
        ttk.Label(sec_print, text="デフォルトプリンタ").grid(row=1, column=0, sticky="w", padx=4, pady=2)
        ttk.Combobox(sec_print, textvariable=vars["default_printer"], values=printer_list()).grid(row=1, column=1, sticky="ew", padx=4, pady=2)
        _add_entry(sec_print, 2, "デフォルト部数", "default_copies")
        ttk.Label(sec_print, text="自動保存").grid(row=3, column=0, sticky="w", padx=4, pady=2)
        ttk.Checkbutton(sec_print, variable=vars["auto_save"]).grid(row=3, column=1, sticky="w")
        ttk.Label(sec_print, text="Excel only 不一致を失敗").grid(row=4, column=0, sticky="w", padx=4, pady=2)
        ttk.Checkbutton(sec_print, variable=vars["strict_episode"]).grid(row=4, column=1, sticky="w")
        ttk.Label(sec_print, text="不一致時 _mismatch 保存").grid(row=5, column=0, sticky="w", padx=4, pady=2)
        ttk.Checkbutton(sec_print, variable=vars["mismatch_suffix"]).grid(row=5, column=1, sticky="w")

        sec_debug = ttk.LabelFrame(body, text="Debug", padding=6); sec_debug.pack(fill=tk.X, pady=(6, 0)); sec_debug.columnconfigure(1, weight=1)
        ttk.Label(sec_debug, text="Debug有効").grid(row=0, column=0, sticky="w", padx=4, pady=2)
        ttk.Checkbutton(sec_debug, variable=vars["dbg_enabled"]).grid(row=0, column=1, sticky="w")
        ttk.Label(sec_debug, text="Headless").grid(row=1, column=0, sticky="w", padx=4, pady=2)
        ttk.Checkbutton(sec_debug, variable=vars["dbg_headless"]).grid(row=1, column=1, sticky="w")
        ttk.Label(sec_debug, text="詳細ログ").grid(row=2, column=0, sticky="w", padx=4, pady=2)
        ttk.Checkbutton(sec_debug, variable=vars["dbg_verbose"]).grid(row=2, column=1, sticky="w")
        ttk.Label(sec_debug, text="エラー時スクショ").grid(row=3, column=0, sticky="w", padx=4, pady=2)
        ttk.Checkbutton(sec_debug, variable=vars["dbg_shot"]).grid(row=3, column=1, sticky="w")
        ttk.Label(sec_debug, text="エラー時HTML").grid(row=4, column=0, sticky="w", padx=4, pady=2)
        ttk.Checkbutton(sec_debug, variable=vars["dbg_html"]).grid(row=4, column=1, sticky="w")
        _add_entry(sec_debug, 5, "待機秒数(override)", "dbg_wait")
        _add_entry(sec_debug, 6, "入力反映待機秒", "dbg_settle")
        ttk.Label(sec_debug, text="Report直遷移").grid(row=7, column=0, sticky="w", padx=4, pady=2)
        ttk.Checkbutton(sec_debug, variable=vars["dbg_report_direct"]).grid(row=7, column=1, sticky="w")

        def _save_detail() -> None:
            c.owlview_home_url = vars["home"].get(); c.owlview_report_url = vars["report"].get(); c.xpath_input_box = vars["xpath"].get(); c.xpath_home_input_box = vars["home_xpath"].get(); c.xpath_inputtable_input_box = vars["inputtable_xpath"].get(); c.xpath_report_ready = vars["report_ready_xpath"].get(); c.xpath_search_ready = vars["search_ready_xpath"].get(); c.selenium_wait_sec = int(vars["wait"].get()); c.default_local_copy_dir = vars["local_dir"].get(); c.curl_path = vars["curl"].get(); c.sumatra_path = vars["sumatra"].get(); c.ftp_encryption = vars["ftp_encryption"].get(); c.ftp_host = vars["ftp_host"].get(); c.ftp_port = int(vars["ftp_port"].get()); c.ftp_username = vars["ftp_user"].get(); c.ftp_password = vars["ftp_pass"].get(); c.ftp_remote_path_template = vars["ftp_path"].get(); c.default_printer_name = vars["default_printer"].get(); c.default_print_copies = int(vars["default_copies"].get()); c.auto_save_settings = bool(vars["auto_save"].get()); c.excel_only_fail_on_episode_mismatch = bool(vars["strict_episode"].get()); c.inputtable_episode_mismatch_suffix = bool(vars["mismatch_suffix"].get())
            self.cfg.job.ftp_default_enabled = bool(vars["ftp_default"].get()); self.cfg.job.print_default_enabled = bool(vars["print_default"].get())
            c.debug.enabled = bool(vars["dbg_enabled"].get()); c.debug.headless = bool(vars["dbg_headless"].get()); c.debug.verbose_log = bool(vars["dbg_verbose"].get()); c.debug.save_screenshot_on_error = bool(vars["dbg_shot"].get()); c.debug.save_html_on_error = bool(vars["dbg_html"].get()); c.debug.selenium_wait_timeout = int(vars["dbg_wait"].get()); c.debug.input_settle_wait = float(vars["dbg_settle"].get()); c.debug.report_direct_navigation = bool(vars["dbg_report_direct"].get())
            self._invalidate_preview_caches()
            self.store.save(self.cfg); self.tools = self._resolve_tools(); self.main_printer_var.set(c.default_printer_name); self.main_ftp_var.set(self.cfg.job.ftp_default_enabled); self.main_print_var.set(self.cfg.job.print_default_enabled); self._refresh_printer_combo(); self._log("詳細設定を保存しました"); d.destroy()
        btns = ttk.Frame(d); btns.pack(fill=tk.X, padx=8, pady=8)
        ttk.Button(btns, text="FTP接続テスト", command=self.test_ftp).pack(side=tk.LEFT, padx=2)
        ttk.Button(btns, text="保存", command=_save_detail).pack(side=tk.RIGHT, padx=2)

    def add_part(self) -> None:
        p = self._part_dialog();
        if p: self.cfg.parts.append(p); self._invalidate_preview_caches(); self._refresh_part_list(); self.auto_save()

    def edit_part(self) -> None:
        if not self.selected_ids: return
        idx = self.selected_ids[0]; p = self._part_dialog(self.cfg.parts[idx])
        if p: self.cfg.parts[idx] = p; self._invalidate_preview_caches(); self._refresh_part_list(); self.auto_save()

    def duplicate_selected(self) -> None:
        for idx in self.selected_ids:
            cp = PartConfig(**asdict(self.cfg.parts[idx])); cp.output_name = f"{cp.output_name}_copy"; self.cfg.parts.append(cp)
        self._invalidate_preview_caches(); self._refresh_part_list(); self.auto_save()

    def delete_selected(self) -> None:
        for idx in sorted(self.selected_ids, reverse=True): del self.cfg.parts[idx]
        self._invalidate_preview_caches(); self._refresh_part_list(); self.auto_save()

    def move_selected(self, offset: int) -> None:
        if len(self.selected_ids) != 1: return
        idx = self.selected_ids[0]; ni = idx + offset
        if 0 <= ni < len(self.cfg.parts): self.cfg.parts[idx], self.cfg.parts[ni] = self.cfg.parts[ni], self.cfg.parts[idx]; self._refresh_part_list(); self.auto_save()

    def select_all_parts(self) -> None:
        for p in self.cfg.parts: p.selected = True; p.enabled = True
        self._refresh_part_list(); self.auto_save()

    def clear_all_parts(self) -> None:
        for p in self.cfg.parts: p.selected = False
        self._refresh_part_list(); self.auto_save()

    def auto_save(self) -> None:
        if self.cfg.common.auto_save_settings: self.store.save(self.cfg)

    def _invalidate_preview_caches(self) -> None:
        self.preview_cache.clear()
        self.preview_image_cache.clear()
        self.inline_preview_base = None

    def _apply_main_toggles(self, parts: list[PartConfig]) -> list[PartConfig]:
        cloned: list[PartConfig] = []
        for p in parts:
            cp = PartConfig(**asdict(p))
            cp.local_copy_enabled = True
            cloned.append(cp)
        return cloned

    def _validate_before_run(self, parts: list[PartConfig]) -> list[str]:
        errs: list[str] = []
        if self.excel_only_var.get():
            if not self.tools.curl.exists() and any(p.enable_inputtable_excel_export or self.excel_only_var.get() for p in parts):
                # Excel出力本体にはcurl不要。環境警告ノイズを避けるため何もしない。
                pass
            return errs
        if self.main_ftp_var.get() and not self.tools.curl.exists(): errs.append(f"curl が見つかりません: {self.tools.curl}")
        if self.main_print_var.get():
            ps = printer_list(); printer = self.main_printer_var.get().strip()
            if not printer: errs.append("印刷する がONですがプリンタ未選択です")
            elif ps and printer not in ps: errs.append(f"指定プリンタが存在しません: {printer}")
            if not self.tools.sumatra.exists(): errs.append(f"SumatraPDF が見つかりません: {self.tools.sumatra}")
        return errs

    def _start_run(self, parts: list[PartConfig]) -> None:
        if self.runner: messagebox.showwarning("実行中", "現在実行中です"); return
        valid = [p for p in parts if p.enabled]
        if not valid: messagebox.showwarning("対象なし", "実行対象がありません"); return
        valid = self._apply_main_toggles(valid)
        errors = self._validate_before_run(valid)
        if errors: messagebox.showerror("実行前バリデーション", "\n".join(errors)); return
        self._log(f"使用プリンタ: {self.main_printer_var.get()}")
        self.runner = Runner(
            self.cfg,
            self.tools,
            self.queue,
            run_ftp_enabled=self.main_ftp_var.get(),
            run_print_enabled=self.main_print_var.get(),
            run_printer_name=self.main_printer_var.get(),
            run_copies=max(1, self.cfg.common.default_print_copies),
            excel_only_mode=self.excel_only_var.get(),
        )
        self.runner.run_async(valid)

    def run_single(self) -> None:
        if self.selected_ids: self._start_run([self.cfg.parts[self.selected_ids[0]]])

    def run_selected(self) -> None:
        targets = [p for p in self.cfg.parts if p.selected] or ([self.cfg.parts[self.selected_ids[0]]] if self.selected_ids else [])
        self._start_run(targets)

    def run_range(self) -> None:
        s = simpledialog.askinteger("範囲", "開始番号(1開始)", parent=self.root); e = simpledialog.askinteger("範囲", "終了番号(1開始)", parent=self.root)
        if not s or not e: return
        self._start_run(self.cfg.parts[min(s, e)-1:max(s, e)])

    def run_all(self) -> None:
        self._start_run(self.cfg.parts)

    def generate_preview_pdf(self, part: PartConfig) -> Path:
        preview_dir = self.base_dir / "Settings" / "_preview"
        key_payload = {
            "part": asdict(part),
            "home": self.cfg.common.owlview_home_url,
            "report": self.cfg.common.owlview_report_url,
            "xpath": self.cfg.common.xpath_input_box,
            "home_xpath": self.cfg.common.xpath_home_input_box,
            "inputtable_xpath": self.cfg.common.xpath_inputtable_input_box,
            "wait": self.cfg.common.selenium_wait_sec,
            "wait_debug": self.cfg.common.debug.selenium_wait_timeout,
            "report_direct_navigation": self.cfg.common.debug.report_direct_navigation,
        }
        cache_key = hashlib.sha1(json.dumps(key_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
        cached = self.preview_cache.get(cache_key)
        if cached and cached.exists():
            self._log(f"プレビューキャッシュ使用: {cached.name}")
            return cached
        runner = Runner(self.cfg, self.tools, self.queue)
        pdf, _ = runner.run_preview(part, preview_dir)
        self.preview_temp_files.append(pdf)
        self.preview_cache[cache_key] = pdf
        for old in self.preview_temp_files[:-5]:
            old.unlink(missing_ok=True)
        self.preview_temp_files = self.preview_temp_files[-5:]
        return pdf

    def open_preview_window(self) -> None:
        if not self.selected_ids:
            messagebox.showwarning("印刷プレビュー", "パートを選択してください")
            return
        idx = self.selected_ids[0]
        if self.preview_window is None or not self.preview_window.win.winfo_exists():
            self.preview_window = PdfPreviewWindow(self)
        self.preview_window.win.deiconify(); self.preview_window.win.lift()
        self.preview_window.load_for_part(idx, self.cfg.parts[idx])

    def test_ftp(self) -> None:
        if not self.tools.curl.exists():
            messagebox.showerror("FTP", f"curl が見つかりません: {self.tools.curl}")
            return
        path_errors = validate_ftp_path_template(self.cfg.common.ftp_remote_path_template)
        expanded = resolved_remote_path(self.cfg.common.ftp_remote_path_template)
        if path_errors: messagebox.showwarning("FTP Path", "バリデーション警告:\n" + "\n".join(path_errors) + f"\n\n展開後: {expanded}")
        try:
            remote, result = ftp_test_connection(self.cfg.common, self.tools.curl)
            msg = "\n".join(["接続成功", f"host={self.cfg.common.ftp_host}", f"port={self.cfg.common.ftp_port}", f"暗号方式={self.cfg.common.ftp_encryption}", f"ユーザー名={self.cfg.common.ftp_username}", f"リモートパス(展開後)={remote}", f"curl={result.command_summary}", f"stdout={result.stdout or '(empty)'}", f"stderr={result.stderr or '(empty)'}"])
            self._log(msg); messagebox.showinfo("FTP", msg)
        except Exception as exc:
            self._log(str(exc)); messagebox.showerror("FTP", f"接続失敗:\n{exc}")

    def reload_printers(self) -> None:
        self._refresh_printer_combo(); ps = printer_list(); messagebox.showinfo("Printer", "\n".join(ps) if ps else "プリンタが見つかりません")

    def run_environment_check(self) -> None:
        checks: list[tuple[str, str, str]] = []

        def add(name: str, status: str, detail: str) -> None:
            checks.append((name, status, detail))

        for name, path in [("curl", self.tools.curl), ("SumatraPDF", self.tools.sumatra)]:
            if path.exists():
                add(name, "成功", f"検出: {path}")
            else:
                add(name, "失敗", f"未検出: 設定値={path}")

        add("Selenium Manager", "情報", "ChromeDriver固定パスは未使用。webdriver.Chrome(...) で自動解決します")

        try:
            ps = printer_list()
            if ps:
                add("プリンタ一覧", "成功", f"{len(ps)}件検出 (先頭: {ps[0]})")
            else:
                add("プリンタ一覧", "警告", "取得できませんでした (環境依存の可能性)")
        except Exception as exc:
            add("プリンタ一覧", "失敗", str(exc))

        add("Settings読み込み", "成功", f"version={self.cfg.version}")

        output_target = ""
        if self.selected_ids:
            output_target = self.cfg.parts[self.selected_ids[0]].output_dir
        output_target = output_target or self.cfg.job.shared_output_dir or str(self.base_dir / "Settings")
        try:
            out = Path(output_target)
            out.mkdir(parents=True, exist_ok=True)
            probe = out / ".write_test.tmp"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
            add("出力先書き込み", "成功", str(out))
        except Exception as exc:
            add("出力先書き込み", "失敗", f"{output_target} / {exc}")

        try:
            with urllib.request.urlopen(self.cfg.common.owlview_home_url, timeout=8) as resp:
                code = getattr(resp, "status", 200)
            add("OwlView home到達", "成功" if int(code) < 400 else "警告", f"status={code} url={self.cfg.common.owlview_home_url}")
        except Exception as exc:
            add("OwlView home到達", "失敗", str(exc))

        if self.main_ftp_var.get():
            if not self.tools.curl.exists():
                add("FTP簡易接続", "失敗", f"curl未検出: {self.tools.curl}")
            else:
                try:
                    remote, _ = ftp_test_connection(self.cfg.common, self.tools.curl)
                    add("FTP簡易接続", "成功", f"remote={remote}")
                except Exception as exc:
                    add("FTP簡易接続", "失敗", str(exc))
        else:
            add("FTP簡易接続", "警告", "FTPがOFFのため未実施")

        dlg = tk.Toplevel(self.root)
        dlg.title("環境チェック結果")
        txt = tk.Text(dlg, width=120, height=24)
        txt.pack(fill=tk.BOTH, expand=True)
        for name, status, detail in checks:
            txt.insert(tk.END, f"[{status}] {name}\n  {detail}\n")
        ttk.Button(dlg, text="閉じる", command=dlg.destroy).pack(side=tk.RIGHT, padx=6, pady=6)
        self._log("環境チェック完了")

    def stop_run(self) -> None:
        if self.runner: self.runner.stop()

    def open_output_dir(self) -> None:
        if self.selected_ids:
            p = self.cfg.parts[self.selected_ids[0]]
            if os.path.isdir(p.output_dir): os.startfile(p.output_dir)  # type: ignore[attr-defined]

    def _poll_queue(self) -> None:
        try:
            while True:
                kind, payload = self.queue.get_nowait()
                if kind == "start": self.progress_var.set(0); self.status_var.set("実行開始")
                elif kind == "progress": self.progress_var.set(payload["value"] / max(payload["total"], 1) * 100); self.status_var.set(payload["text"])
                elif kind == "log": self._log(payload["text"])
                elif kind == "part_summary": self._log(self._render_part_summary_line(payload["summary"]))
                elif kind == "done": self._show_result_dialog(payload["results"]); self.runner = None
        except Empty:
            pass
        self.root.after(150, self._poll_queue)

    def _show_result_dialog(self, results) -> None:
        dlg = tk.Toplevel(self.root); dlg.title("実行結果")
        txt = tk.Text(dlg, width=110, height=22); txt.pack(fill=tk.BOTH, expand=True)
        failed: list[str] = []
        for r in results:
            txt.insert(tk.END, f"{'OK' if r.success else 'NG'} | {r.part_name} | {r.message}\n")
            for status in r.file_statuses:
                txt.insert(tk.END, f"  - {status.file_path.name}: ローカル={status.local_copy} / FTP={status.ftp} / 印刷={status.print_status}\n")
            txt.insert(tk.END, "\n")
            if not r.success:
                failed.append(r.part_name)
        txt.insert(
            tk.END,
            "\n".join(
                [
                    "=== 全体サマリ ===",
                    f"総件数: {len(results)}",
                    f"成功件数: {len([r for r in results if r.success])}",
                    f"失敗件数: {len(failed)}",
                    f"失敗パート一覧: {', '.join(failed) if failed else '-'}",
                    f"保存先フォルダ: {self.base_dir}",
                ]
            ) + "\n",
        )
        ttk.Button(dlg, text="閉じる", command=dlg.destroy).pack(side=tk.RIGHT)

    def _append_stacktrace(self, exc: Exception) -> None:
        log_dir = self.base_dir / "Settings"; log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "error.log"
        with log_file.open("a", encoding="utf-8") as f:
            f.write(f"\n[{datetime.now().isoformat()}] {exc}\n"); f.write(traceback.format_exc())
        self._log(f"詳細ログ保存: {log_file}")

    def _log(self, text: str) -> None:
        tags: list[str] = []
        upper = text.upper()
        if text.startswith("[PART]"):
            tags.append("part")
        elif any(x in upper for x in ["失敗", "ERROR", "NG", "Traceback"]):
            tags.append("error")
        elif "WARNING" in upper or "警告" in text:
            tags.append("warning")
        elif any(x in upper for x in ["成功", "完了", "OK"]):
            tags.append("success")
        self.log_text.insert(tk.END, text + "\n", tuple(tags)); self.log_text.see(tk.END)

    def on_close(self) -> None:
        self.cfg.common.default_printer_name = self.main_printer_var.get()
        self.cfg.ui.window_maximized = self.root.state() == "zoomed"
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        self.cfg.ui.window_geometry = safe_geometry(self.root.geometry(), sw, sh)
        self.cfg.job.ftp_default_enabled = bool(self.main_ftp_var.get())
        self.cfg.job.print_default_enabled = bool(self.main_print_var.get())
        self.cfg.ui.preview_zoom = float(self.preview_zoom_var.get())
        self.store.save(self.cfg)
        for p in self.preview_temp_files: p.unlink(missing_ok=True)
        self.root.destroy()

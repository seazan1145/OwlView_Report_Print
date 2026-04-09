from __future__ import annotations

import tkinter as tk
from pathlib import Path

from owlview_tool.gui import OwlViewApp


def main() -> None:
    root = tk.Tk()
    OwlViewApp(root, Path(__file__).resolve().parent)
    root.mainloop()


if __name__ == "__main__":
    main()

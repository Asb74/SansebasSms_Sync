import tkinter as tk
from tkinter import messagebox
from typing import Callable, Any


def _safe(root: tk.Misc | None, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    if root is None:
        return
    toplevel = root.winfo_toplevel() if hasattr(root, "winfo_toplevel") else root
    if isinstance(toplevel, tk.Tk):
        toplevel.after(0, lambda: fn(*args, **kwargs))


def info(root: tk.Misc | None, title: str, message: str) -> None:
    _safe(root, messagebox.showinfo, title, message)


def warn(root: tk.Misc | None, title: str, message: str) -> None:
    _safe(root, messagebox.showwarning, title, message)


def error(root: tk.Misc | None, title: str, message: str) -> None:
    _safe(root, messagebox.showerror, title, message)

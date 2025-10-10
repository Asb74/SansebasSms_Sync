import tkinter as tk
from tkinter import messagebox
from typing import Callable


def _safe(root: tk.Misc, fn: Callable, *args, **kwargs) -> None:
    if isinstance(root, tk.Misc):
        root.after(0, lambda: fn(*args, **kwargs))


def info(root: tk.Misc, title: str, message: str) -> None:
    _safe(root, messagebox.showinfo, title, message)


def warn(root: tk.Misc, title: str, message: str) -> None:
    _safe(root, messagebox.showwarning, title, message)


def error(root: tk.Misc, title: str, message: str) -> None:
    _safe(root, messagebox.showerror, title, message)

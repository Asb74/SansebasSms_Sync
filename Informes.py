"""Ventana principal de informes para Sansebassms Desk."""
from __future__ import annotations

import logging
from typing import Optional

import tkinter as tk
from tkinter import messagebox, ttk

from firebase_admin import firestore

try:  # pragma: no cover - icono opcional
    from PIL import Image, ImageTk
except Exception:  # pragma: no cover - opcional
    Image = None  # type: ignore
    ImageTk = None  # type: ignore

from InformeAsistencia import abrir_informe_asistencia

logger = logging.getLogger(__name__)


_ventana: Optional[tk.Toplevel] = None
_db: Optional[firestore.Client] = None
_sa_path: Optional[str] = None
_project_id: Optional[str] = None


def abrir_informes(
    db: firestore.Client,
    sa_path: Optional[str] = None,
    project_id: Optional[str] = None,
) -> None:
    """Abre la ventana principal de informes."""

    global _ventana, _db, _sa_path, _project_id

    if _ventana is not None and _ventana.winfo_exists():
        _ventana.lift()
        _ventana.focus_force()
        return

    _db = db
    _sa_path = sa_path
    _project_id = project_id

    _ventana = tk.Toplevel()
    _ventana.title("Informes")
    _ventana.geometry("380x220")
    _ventana.resizable(False, False)

    try:  # pragma: no cover - icono opcional
        _ventana.iconphoto(True, tk.PhotoImage(file="icono_app.png"))
    except Exception:
        pass

    contenedor = ttk.Frame(_ventana, padding=20)
    contenedor.pack(fill="both", expand=True)

    if Image is not None and ImageTk is not None:
        try:
            img = Image.open("icono_app.png")
            img = img.resize((64, 64))
            logo = ImageTk.PhotoImage(img)
            etiqueta_logo = ttk.Label(contenedor, image=logo)
            etiqueta_logo.image = logo  # type: ignore[attr-defined]
            etiqueta_logo.pack(pady=(0, 15))
        except Exception:  # pragma: no cover - icono opcional
            pass

    ttk.Label(
        contenedor,
        text="Selecciona el informe que deseas abrir",
        font=("Segoe UI", 11, "bold"),
    ).pack(pady=(0, 20))

    ttk.Button(
        contenedor,
        text="Control de asistencia (Fichajes001)",
        command=_abrir_informe_asistencia,
        width=36,
    ).pack()

    def _cerrar() -> None:
        global _ventana
        if _ventana is not None:
            try:
                _ventana.destroy()
            except Exception:
                pass
        _ventana = None

    _ventana.protocol("WM_DELETE_WINDOW", _cerrar)


def _abrir_informe_asistencia() -> None:
    if _db is None:
        messagebox.showerror("Informes", "No se ha inicializado la conexión a Firestore.")
        return

    try:
        abrir_informe_asistencia(_db, _sa_path, _project_id)
    except Exception as exc:  # pragma: no cover - errores inesperados
        logger.exception("Error al abrir el informe de asistencia")
        messagebox.showerror("Informes", f"No se pudo abrir el informe: {exc}")

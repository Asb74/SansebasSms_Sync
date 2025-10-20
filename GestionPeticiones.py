import csv
import logging
import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Any, Dict, List, Optional

from firebase_admin import firestore
from google.cloud import firestore as _firestore

from thread_utils import run_bg
from ui_safety import error, info

from main import (
    _is_valid_fcm_token,
    enviar_fcm,
    get_doc_safe,
    obtener_token_oauth,
)

try:
    from tkcalendar import DateEntry
except Exception:
    DateEntry = None

logger = logging.getLogger(__name__)

ventana_peticiones: Optional[tk.Toplevel] = None
ventana_peticiones_app: Optional["GestionPeticionesUI"] = None


def _to_local(dt):
    from datetime import timezone

    if not dt:
        return None
    if getattr(dt, "tzinfo", None) is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone()


def _fmt_fecha(dt):
    local_dt = _to_local(dt)
    return local_dt.strftime("%d-%m-%Y") if local_dt else ""


def _fmt_fechahora(dt):
    local_dt = _to_local(dt)
    return local_dt.strftime("%d-%m-%Y %H:%M") if local_dt else ""


def normalizar_estado(v: str | None) -> str:
    s = (v or "").strip().lower()
    if s == "ok":
        return "Ok"
    if s in {"denegado", "denegada"}:
        return "Denegada"
    if s in {"aprobado", "aprobada"}:
        return "Ok"
    return "Pendiente"


def _parse_fecha_text(s: str):
    from datetime import datetime

    if not s:
        return None
    s = s.strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None


def _actualizar_peticion(db, peticion_id: str, decision: str, *, respondida_por: str | None = None):
    # decision esperada: "OK" o "Denegada"
    estado_map = {"OK": "Aprobada", "Denegada": "Denegada"}
    db.collection("Peticiones").document(peticion_id).update(
        {
            "estado": estado_map.get(decision, decision),
            "respuesta": decision,
            "fechaRespuesta": _firestore.SERVER_TIMESTAMP,
            "respondidaPor": respondida_por
            or os.getenv("USERNAME")
            or os.getenv("USER")
            or "sistema",
        },
        timeout=30.0,
    )


def _texto_notif(nombre: str | None, fecha: str | None, decision: str) -> tuple[str, str]:
    # Devuelve (title, body) para la notificación
    fecha_txt = f" ({fecha})" if fecha else ""
    if decision == "OK":
        return ("✅ Día libre aprobado", f"Tu solicitud{fecha_txt} ha sido APROBADA.")
    else:
        return ("❌ Día libre denegado", f"Tu solicitud{fecha_txt} ha sido DENEGADA.")


def _dialogo_responder_peticion(parent, *, detalle: str, on_ok, on_denegar, titulo="Responder petición"):
    win = tk.Toplevel(parent)
    win.title(titulo)
    win.transient(parent)
    win.grab_set()
    win.resizable(False, False)

    frm = tk.Frame(win, padx=16, pady=16)
    frm.pack(fill="both", expand=True)

    tk.Label(frm, text=detalle, justify="left").pack(pady=(0, 12))

    btns = tk.Frame(frm)
    btns.pack(fill="x")

    def cerrar():
        try:
            win.grab_release()
        except Exception:
            pass
        win.destroy()

    tk.Button(btns, text="OK", width=12, command=lambda: (on_ok(), cerrar())).pack(
        side="left", padx=4
    )
    tk.Button(btns, text="Denegar", width=12, command=lambda: (on_denegar(), cerrar())).pack(
        side="left", padx=4
    )
    tk.Button(btns, text="Cancelar", width=12, command=cerrar).pack(side="right", padx=4)

    parent.update_idletasks()
    x = parent.winfo_rootx() + parent.winfo_width() // 2 - 160
    y = parent.winfo_rooty() + parent.winfo_height() // 2 - 60
    win.geometry(f"+{x}+{y}")

    win.wait_window(win)


class ToolTip:
    def __init__(self, widget: tk.Widget) -> None:
        self.widget = widget
        self.tipwindow: Optional[tk.Toplevel] = None

    def show(self, text: str, x: int, y: int) -> None:
        text = (text or "").strip()
        if not text:
            self.hide()
            return

        self.hide()

        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(
            f"+{self.widget.winfo_rootx() + x + 20}+{self.widget.winfo_rooty() + y + 20}"
        )

        label = tk.Label(
            tw,
            text=text,
            justify="left",
            background="#ffffe0",
            relief="solid",
            borderwidth=1,
            wraplength=400,
        )
        label.pack(ipadx=4, ipady=2)

    def hide(self) -> None:
        if self.tipwindow is not None:
            self.tipwindow.destroy()
            self.tipwindow = None


class GestionPeticionesUI:
    def __init__(self, window: tk.Toplevel, db: firestore.Client, on_close) -> None:
        self.window = window
        self.db = db
        self.on_close = on_close
        self.data_rows: List[Dict[str, Any]] = []
        self.tree_items_info: Dict[str, Dict[str, Any]] = {}
        self._tooltip_state: Dict[str, Optional[str]] = {"item": None, "text": None}

        self._build_ui()
        self.actualizar()

    def _build_ui(self) -> None:
        top_bar = ttk.Frame(self.window, padding=10)
        top_bar.grid(row=0, column=0, sticky="ew")
        top_bar.grid_columnconfigure(0, weight=1)

        btn_actualizar = ttk.Button(top_bar, text="Actualizar", command=self.actualizar)
        btn_actualizar.grid(row=0, column=0, sticky="w")

        frame_filtros = ttk.Frame(self.window)
        frame_filtros.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 6))
        frame_filtros.grid_columnconfigure(1, weight=1)

        ttk.Label(frame_filtros, text="Nombre").grid(row=0, column=0, sticky="w", padx=(0, 4))
        self.ent_nombre = ttk.Entry(frame_filtros, width=24)
        self.ent_nombre.grid(row=0, column=1, sticky="w", padx=(0, 10))

        ttk.Label(frame_filtros, text="Fecha").grid(row=0, column=2, sticky="w", padx=(0, 4))
        if DateEntry:
            self.ent_fecha = DateEntry(frame_filtros, date_pattern="dd-mm-yyyy", width=12)
            self.ent_fecha.grid(row=0, column=3, sticky="w", padx=(0, 10))
            self.ent_fecha.delete(0, tk.END)
            self.ent_fecha.bind("<Return>", lambda e: self.aplicar_filtros())
        else:
            self.ent_fecha = ttk.Entry(frame_filtros, width=12)
            self.ent_fecha.grid(row=0, column=3, sticky="w", padx=(0, 10))
            self.ent_fecha.insert(0, "dd-mm-YYYY")
            self.ent_fecha.bind("<Return>", lambda e: self.aplicar_filtros())

        ttk.Label(frame_filtros, text="Estado").grid(row=0, column=4, sticky="w", padx=(0, 4))
        self.cmb_estado = ttk.Combobox(
            frame_filtros,
            values=("Todos", "Ok", "Denegada", "Pendiente"),
            state="readonly",
            width=12,
        )
        self.cmb_estado.grid(row=0, column=5, sticky="w", padx=(0, 10))
        self.cmb_estado.current(0)

        btn_filtrar = ttk.Button(frame_filtros, text="Filtrar", command=self.aplicar_filtros)
        btn_filtrar.grid(row=0, column=6, sticky="w", padx=(0, 6))

        btn_limpiar = ttk.Button(frame_filtros, text="Limpiar", command=self.limpiar_filtros)
        btn_limpiar.grid(row=0, column=7, sticky="w")

        self.ent_nombre.bind("<Return>", lambda e: self.aplicar_filtros())
        self.cmb_estado.bind("<<ComboboxSelected>>", lambda e: self.aplicar_filtros())

        tree_frame = ttk.Frame(self.window, padding=(10, 0, 10, 0))
        tree_frame.grid(row=2, column=0, sticky="nsew")
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

        columnas = (
            "_PeticionId",
            "_Uid",
            "Nombre",
            "Fecha",
            "CreadoEn",
            "Motivo",
            "Admitido",
        )
        self.tree = ttk.Treeview(
            tree_frame,
            columns=columnas,
            show="headings",
            selectmode="browse",
        )

        yscroll = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        xscroll = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")

        self.tree.heading("_PeticionId", text="")
        self.tree.heading("_Uid", text="")
        self.tree.heading("Nombre", text="Nombre")
        self.tree.heading("Fecha", text="Fecha")
        self.tree.heading("CreadoEn", text="CreadoEn")
        self.tree.heading("Motivo", text="Motivo")
        self.tree.heading("Admitido", text="Admitido")

        self.tree.column("_PeticionId", width=0, stretch=False, minwidth=0)
        self.tree.column("_Uid", width=0, stretch=False, minwidth=0)
        self.tree.column("Nombre", width=220, anchor="w")
        self.tree.column("Fecha", width=140, anchor="center")
        self.tree.column("CreadoEn", width=200, anchor="center")
        self.tree.column("Motivo", width=320, anchor="w", stretch=True)
        self.tree.column("Admitido", width=120, anchor="center")

        self.tooltip = ToolTip(self.tree)
        self._clear_tooltip()

        self.tree.bind("<Motion>", self._on_tree_motion)
        self.tree.bind("<Leave>", lambda _e: self._clear_tooltip())

        self.tree.unbind("<Button-1>")
        self.tree.bind("<Double-1>", self._on_tree_double_click)

        bottom_bar = ttk.Frame(self.window, padding=10)
        bottom_bar.grid(row=3, column=0, sticky="ew")
        bottom_bar.grid_columnconfigure(0, weight=1)

        ttk.Button(bottom_bar, text="Exportar CSV", command=self.exportar_csv).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Button(bottom_bar, text="Cerrar", command=self.on_close).grid(
            row=0, column=1, sticky="e"
        )

    def _cerrar_editor(self) -> None:
        self._clear_tooltip()

    def _clear_tooltip(self) -> None:
        if hasattr(self, "tooltip"):
            self.tooltip.hide()
        self._tooltip_state = {"item": None, "text": None}

    def _populate_tree(self, rows: List[Dict[str, Any]]) -> None:
        self._cerrar_editor()
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.tree_items_info.clear()

        self._clear_tooltip()

        for row in rows:
            nombre = row.get("Nombre") or "Falta"
            fecha_str = _fmt_fecha(row.get("Fecha"))
            creado_str = _fmt_fechahora(row.get("CreadoEn"))
            motivo_val = row.get("Motivo") or ""
            estado = normalizar_estado(row.get("Admitido"))
            vals = (
                row.get("doc_id"),
                row.get("uid"),
                nombre,
                fecha_str,
                creado_str,
                motivo_val,
                estado,
            )
            iid = row.get("doc_id") or f"row_{len(self.tree_items_info)}"
            item_id = self.tree.insert("", "end", iid=iid, values=vals)
            self.tree_items_info[item_id] = row

    def actualizar(self) -> None:
        self._cerrar_editor()
        try:
            snapshot = self.db.collection("Peticiones").stream()
        except Exception as e:
            messagebox.showerror("Error", f"No se pudieron leer las peticiones: {e}")
            return

        rows: List[Dict[str, Any]] = []
        for doc in snapshot:
            data = doc.to_dict() or {}
            uid = data.get("uid") or ""
            nombre = "Falta"
            token = None
            try:
                if uid:
                    user_doc = self.db.collection("UsuariosAutorizados").document(uid).get()
                    if user_doc.exists:
                        user_data = user_doc.to_dict() or {}
                        nombre = user_data.get("Nombre") or "Falta"
                        token = user_data.get("fcmToken")
            except Exception as err:
                print(f"❌ Error obteniendo usuario {uid}: {err}")

            row = {
                "doc_id": doc.id,
                "uid": uid,
                "fcmToken": token,
                "Nombre": nombre or "Falta",
                "Fecha": data.get("Fecha"),
                "CreadoEn": data.get("creadoEn"),
                "Admitido": data.get("respuesta")
                or data.get("Admitido")
                or data.get("estado")
                or "",
                "Motivo": (data.get("Motivo") or "").strip(),
            }
            rows.append(row)

        self.data_rows = rows
        self.aplicar_filtros()

    def aplicar_filtros(self, *_args) -> None:
        nombre_filter = (self.ent_nombre.get() or "").strip().lower()
        fecha_text = (self.ent_fecha.get() or "").strip()
        if not DateEntry and fecha_text.lower() == "dd-mm-yyyy":
            fecha_text = ""
        fecha_dt = _parse_fecha_text(fecha_text)
        estado_filter = self.cmb_estado.get()

        def pasa_estado(fila: Dict[str, Any]) -> bool:
            estado = normalizar_estado(fila.get("Admitido"))
            return (estado_filter == "Todos") or (estado == estado_filter)

        filtrados: List[Dict[str, Any]] = []
        for row in self.data_rows:
            nombre = (row.get("Nombre") or "").lower()
            if nombre_filter and nombre_filter not in nombre:
                continue

            if fecha_dt:
                row_fecha = _to_local(row.get("Fecha"))
                if not row_fecha or row_fecha.date() != fecha_dt.date():
                    continue

            if not pasa_estado(row):
                continue

            filtrados.append(row)

        self._populate_tree(filtrados)

    def limpiar_filtros(self) -> None:
        self.ent_nombre.delete(0, tk.END)
        if DateEntry and isinstance(self.ent_fecha, DateEntry):
            try:
                self.ent_fecha.set_date("")
            except Exception:
                self.ent_fecha.delete(0, tk.END)
            else:
                self.ent_fecha.delete(0, tk.END)
        else:
            self.ent_fecha.delete(0, tk.END)
        self.cmb_estado.current(0)
        self.aplicar_filtros()

    def exportar_csv(self) -> None:
        items = self.tree.get_children("")
        if not items:
            messagebox.showinfo("Exportar CSV", "No hay datos para exportar.")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            initialfile="peticiones.csv",
        )
        if not path:
            return
        cols = ["Nombre", "Fecha", "CreadoEn", "Motivo", "Admitido"]
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(cols)
            for item in items:
                values = list(self.tree.item(item, "values"))
                values = values[2:]
                writer.writerow(values)
        messagebox.showinfo("Exportar CSV", "Exportación completada.")

    def _on_tree_motion(self, event) -> None:
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            self._clear_tooltip()
            return

        column = self.tree.identify_column(event.x)
        if column != "#6":
            self._clear_tooltip()
            return

        item_id = self.tree.identify_row(event.y)
        if not item_id:
            self._clear_tooltip()
            return

        text = (self.tree.set(item_id, "Motivo") or "").strip()
        if not text or len(text) <= 48:
            self._clear_tooltip()
            return

        if (
            self._tooltip_state.get("item") == item_id
            and self._tooltip_state.get("text") == text
        ):
            return

        self._tooltip_state = {"item": item_id, "text": text}
        self.tooltip.show(text, event.x, event.y)

    def _on_tree_double_click(self, event) -> None:
        tree = event.widget
        row_id = tree.identify_row(event.y)
        if not row_id:
            return

        item = tree.item(row_id)
        values = item.get("values", [])
        if not values:
            return

        peticion_id = values[0] if len(values) > 0 else None
        uid = values[1] if len(values) > 1 else None
        nombre = values[2] if len(values) > 2 else ""
        fecha_str = values[3] if len(values) > 3 else ""

        if not peticion_id or not uid:
            return

        parent = tree.winfo_toplevel()

        def _procesar(decision: str):
            def worker():
                try:
                    _actualizar_peticion(self.db, peticion_id, decision)

                    usuario_doc = self.db.collection("UsuariosAutorizados").document(uid)
                    usuario = get_doc_safe(usuario_doc) or {}
                    token = usuario.get("fcmToken")
                    if not _is_valid_fcm_token(token):
                        info(
                            parent,
                            "Aviso",
                            "El usuario no tiene un token FCM válido. No se envió la notificación.",
                        )
                    else:
                        token_oauth = obtener_token_oauth()
                        title, body = _texto_notif(nombre, fecha_str, decision)
                        enviado = enviar_fcm(
                            uid,
                            token,
                            token_oauth,
                            notification={"title": title, "body": body},
                            data={"accion": "abrir_usuario_screen"},
                        )
                        if enviado:
                            info(parent, "Notificación", "✅ Notificación enviada al solicitante.")
                        else:
                            error(
                                parent,
                                "Notificación",
                                "❌ No se pudo enviar la notificación al solicitante.",
                            )

                    self.actualizar()
                except Exception as exc:
                    logger.exception("Fallo al responder petición")
                    error(parent, "Error", f"No se pudo completar la operación: {exc}")

            run_bg(worker, _thread_name=f"responder_peticion_{peticion_id}")

        detalle = (
            f"Solicitante: {nombre}\n"
            f"Fecha: {fecha_str}\n\n¿Quieres aprobar o denegar esta petición?"
        )
        _dialogo_responder_peticion(
            parent,
            detalle=detalle,
            on_ok=lambda: _procesar("OK"),
            on_denegar=lambda: _procesar("Denegada"),
            titulo="Responder petición",
        )


def abrir_gestion_peticiones(
    db: firestore.Client, sa_path: Optional[str] = None, project_id: Optional[str] = None
) -> None:
    """
    db: instancia de firestore.Client creada en main.py
    """

    global ventana_peticiones, ventana_peticiones_app

    assert db is not None, "Se requiere 'db' inicializado desde main.py (firebase_admin.initialize_app en main y firestore.client())."

    if not sa_path or not project_id:
        messagebox.showerror("Error", "Faltan credenciales de Firebase.")
        return

    if ventana_peticiones and ventana_peticiones.winfo_exists():
        ventana_peticiones.lift()
        ventana_peticiones.focus_force()
        return

    ventana_peticiones = tk.Toplevel()
    ventana_peticiones.title("Gestión de Peticiones de Días Libres")
    ventana_peticiones.geometry("900x600")
    ventana_peticiones.minsize(760, 480)

    ventana_peticiones.grid_rowconfigure(2, weight=1)
    ventana_peticiones.grid_columnconfigure(0, weight=1)

    def on_close():
        global ventana_peticiones, ventana_peticiones_app
        if ventana_peticiones_app:
            ventana_peticiones_app._cerrar_editor()
        win = ventana_peticiones
        ventana_peticiones = None
        ventana_peticiones_app = None
        if win is not None:
            win.destroy()

    app = GestionPeticionesUI(ventana_peticiones, db, on_close)
    ventana_peticiones_app = app

    ventana_peticiones.protocol("WM_DELETE_WINDOW", on_close)
    ventana_peticiones.focus_force()

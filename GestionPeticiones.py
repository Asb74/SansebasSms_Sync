import csv
import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Any, Callable, Dict, List, Optional

import requests
import google.oauth2.service_account
import google.auth.transport.requests
from firebase_admin import firestore

try:
    from tkcalendar import DateEntry
except Exception:
    DateEntry = None

SCOPES = ["https://www.googleapis.com/auth/firebase.messaging"]

SERVICE_ACCOUNT_JSON: Optional[str] = None
PROJECT_ID: Optional[str] = None

ventana_peticiones: Optional[tk.Toplevel] = None
ventana_peticiones_app: Optional["GestionPeticionesUI"] = None

RESPONDER_IDENTIDAD = os.getenv("SANSEBASSMS_RESPONDER", "desktop_app")


class PeticionNoEncontradaError(RuntimeError):
    """Error raised when the request document does not exist."""


class PeticionYaRespondidaError(RuntimeError):
    """Error raised when trying to respond a request already handled."""


def mostrar_dialogo_respuesta(
    root: tk.Misc, titulo: str = "Responder petición"
) -> Optional[str]:
    dlg = tk.Toplevel(root)
    dlg.title(titulo)
    dlg.transient(root)
    dlg.grab_set()
    dlg.resizable(False, False)

    tk.Label(
        dlg,
        text="Selecciona una respuesta para la petición de día libre:",
        wraplength=360,
        justify="left",
    ).pack(padx=16, pady=12)

    choice: Dict[str, Optional[str]] = {"val": None}

    def _set_choice(val: Optional[str]) -> None:
        choice["val"] = val
        dlg.destroy()

    buttons_frame = tk.Frame(dlg)
    buttons_frame.pack(padx=16, pady=(4, 16), fill="x")

    tk.Button(
        buttons_frame,
        text="✅ OK",
        width=12,
        command=lambda: _set_choice("APROBADO"),
    ).pack(side="left", expand=True, padx=4)
    tk.Button(
        buttons_frame,
        text="⛔ Denegar",
        width=12,
        command=lambda: _set_choice("DENEGADO"),
    ).pack(side="left", expand=True, padx=4)
    tk.Button(
        buttons_frame,
        text="Cancelar",
        width=12,
        command=lambda: _set_choice(None),
    ).pack(side="right", expand=True, padx=4)

    dlg.update_idletasks()
    try:
        x = root.winfo_rootx() + (root.winfo_width() - dlg.winfo_width()) // 2
        y = root.winfo_rooty() + (root.winfo_height() - dlg.winfo_height()) // 2
        dlg.geometry(f"+{x}+{y}")
    except Exception:
        pass

    root.wait_window(dlg)
    return choice["val"]


def _cola_notificacion(
    db_client: firestore.Client, uid_solicitante: str, estado: str, solicitud_id: str
) -> None:
    cuerpo = (
        "✅ Tu día libre ha sido aprobado."
        if estado == "APROBADO"
        else "⛔ Tu día libre ha sido denegado."
    )
    db_client.collection("NotificacionesPendientes").add(
        {
            "uid": uid_solicitante,
            "titulo": "Respuesta a tu petición",
            "body": cuerpo,
            "tipo": "dia_libre",
            "solicitudId": solicitud_id,
            "createdAt": firestore.SERVER_TIMESTAMP,
        }
    )


def responder_peticion(
    root: tk.Misc,
    solicitud_id: str,
    uid_solicitante: str,
    on_refrescar_ui: Callable[[], None],
    *,
    db_client: firestore.Client,
    responded_by: str,
) -> None:
    eleccion = mostrar_dialogo_respuesta(root)
    if eleccion is None:
        return

    if not solicitud_id:
        messagebox.showerror("Error", "No se encontró el identificador de la solicitud.")
        return

    if not uid_solicitante:
        messagebox.showerror("Error", "No se encontró el solicitante asociado.")
        return

    root.configure(cursor="watch")
    root.update_idletasks()

    doc_ref = db_client.collection("PeticionesDiaLibre").document(solicitud_id)

    transaction = db_client.transaction()

    @firestore.transactional
    def _ejecutar(trans) -> None:  # type: ignore[override]
        snap = doc_ref.get(transaction=trans)
        if not snap.exists:
            raise PeticionNoEncontradaError()

        data = snap.to_dict() or {}
        estado_actual = (data.get("estado") or data.get("Admitido") or "").upper() or "PENDIENTE"
        if estado_actual != "PENDIENTE":
            raise PeticionYaRespondidaError()

        trans.update(
            doc_ref,
            {
                "estado": eleccion,
                "respondidoPor": responded_by,
                "respondidoEn": firestore.SERVER_TIMESTAMP,
            },
        )

        historial_ref = doc_ref.collection("historial").document()
        trans.set(
            historial_ref,
            {
                "evento": "RESPUESTA",
                "nuevoEstado": eleccion,
                "timestamp": firestore.SERVER_TIMESTAMP,
                "respondidoPor": responded_by,
            },
        )

    try:
        _ejecutar(transaction)
    except PeticionNoEncontradaError:
        messagebox.showerror("Error", "La solicitud seleccionada no existe.")
        try:
            on_refrescar_ui()
        except Exception:
            pass
        return
    except PeticionYaRespondidaError:
        messagebox.showwarning(
            "Aviso",
            "La solicitud ya fue respondida previamente.",
        )
        try:
            on_refrescar_ui()
        except Exception:
            pass
        return
    except Exception as exc:  # noqa: BLE001
        messagebox.showerror(
            "Error",
            f"No se pudo registrar la respuesta:\n{exc}",
        )
        return
    finally:
        root.configure(cursor="")

    try:
        _cola_notificacion(db_client, uid_solicitante, eleccion, solicitud_id)
    except Exception as exc:  # noqa: BLE001
        messagebox.showwarning(
            "Aviso",
            f"La respuesta se guardó pero no se pudo encolar la notificación:\n{exc}",
        )
    else:
        messagebox.showinfo("Respuesta registrada", "Se notificó al solicitante.")

    try:
        on_refrescar_ui()
    except Exception:
        pass


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
    if s in {"ok", "aprobado"}:
        return "Aprobado"
    if s in {"denegado"}:
        return "Denegado"
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


def _get_access_token(sa_path: str) -> str:
    credentials = google.oauth2.service_account.Credentials.from_service_account_file(
        sa_path, scopes=SCOPES
    )
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    return credentials.token


def send_push_to_token(sa_path: str, project_id: str, token: str, title: str, body: str):
    if not token:
        raise ValueError("FCM token vacío")
    access_token = _get_access_token(sa_path)
    url = f"https://fcm.googleapis.com/v1/projects/{project_id}/messages:send"
    payload = {
        "message": {
            "token": token,
            "notification": {"title": title, "body": body},
            "data": {"tipo": "peticion_dia_libre"},
        }
    }
    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        json=payload,
        timeout=10,
    )
    r.raise_for_status()


def enviar_push_resultado(
    db: firestore.Client,
    uid: str,
    fecha_str: str,
    admitido: str,
    token_prefetch: Optional[str] = None,
) -> None:
    token = token_prefetch
    if not token:
        doc = db.collection("UsuariosAutorizados").document(uid).get()
        if not doc.exists:
            messagebox.showwarning("Push no enviado", "Usuario no encontrado.")
            return
        token = (doc.to_dict() or {}).get("fcmToken")
    if not token:
        messagebox.showwarning("Push no enviado", "El usuario no tiene fcmToken.")
        return
    if not SERVICE_ACCOUNT_JSON or not PROJECT_ID:
        raise RuntimeError("Credenciales de servicio no configuradas.")
    title = "Petición de día libre"
    body = f"Tu petición para {fecha_str} ha sido {admitido}."
    send_push_to_token(SERVICE_ACCOUNT_JSON, PROJECT_ID, token, title, body)


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
        self._operacion_en_progreso = False

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
            values=("Todos", "Aprobado", "Denegado", "Pendiente"),
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

        columnas = ("Nombre", "Fecha", "CreadoEn", "Motivo", "Estado")
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

        self.tree.heading("Nombre", text="Nombre")
        self.tree.heading("Fecha", text="Fecha")
        self.tree.heading("CreadoEn", text="CreadoEn")
        self.tree.heading("Motivo", text="Motivo")
        self.tree.heading("Estado", text="Estado")

        self.tree.column("Nombre", width=220, anchor="w")
        self.tree.column("Fecha", width=140, anchor="center")
        self.tree.column("CreadoEn", width=200, anchor="center")
        self.tree.column("Motivo", width=320, anchor="w", stretch=True)
        self.tree.column("Estado", width=120, anchor="center")

        self.tooltip = ToolTip(self.tree)
        self._clear_tooltip()

        self.tree.bind("<Double-1>", self._on_tree_double_click)
        self.tree.bind("<Motion>", self._on_tree_motion)
        self.tree.bind("<Leave>", lambda _e: self._clear_tooltip())

        bottom_bar = ttk.Frame(self.window, padding=10)
        bottom_bar.grid(row=3, column=0, sticky="ew")
        bottom_bar.grid_columnconfigure(0, weight=1)

        ttk.Button(
            bottom_bar,
            text="Responder",
            command=self.responder_seleccionada,
        ).grid(row=0, column=0, sticky="w", padx=(0, 8))
        ttk.Button(bottom_bar, text="Exportar CSV", command=self.exportar_csv).grid(
            row=0, column=1, sticky="w"
        )
        ttk.Button(bottom_bar, text="Cerrar", command=self.on_close).grid(
            row=0, column=2, sticky="e"
        )

    def _clear_tooltip(self) -> None:
        if hasattr(self, "tooltip"):
            self.tooltip.hide()
        self._tooltip_state = {"item": None, "text": None}

    def _populate_tree(self, rows: List[Dict[str, Any]]) -> None:
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
            vals = (nombre, fecha_str, creado_str, motivo_val, estado)
            iid = row.get("doc_id") or f"row_{len(self.tree_items_info)}"
            item_id = self.tree.insert("", "end", iid=iid, values=vals)
            self.tree_items_info[item_id] = row

    def actualizar(self) -> None:
        try:
            snapshot = self.db.collection("PeticionesDiaLibre").stream()
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
                "Fecha": data.get("Fecha") or data.get("fechaSolicitada"),
                "CreadoEn": data.get("creadoEn") or data.get("creado_en"),
                "Admitido": data.get("estado") or data.get("Admitido") or data.get("estadoActual") or "",
                "Motivo": (data.get("Motivo") or data.get("motivo") or "").strip(),
            }
            rows.append(row)

        self.data_rows = rows
        self.aplicar_filtros()

    def _obtener_item_seleccionado(self) -> Optional[str]:
        selection = self.tree.focus()
        if selection:
            return selection
        seleccionados = self.tree.selection()
        return seleccionados[0] if seleccionados else None

    def responder_seleccionada(self) -> None:
        item_id = self._obtener_item_seleccionado()
        if not item_id:
            messagebox.showinfo("Responder", "Selecciona una petición primero.")
            return
        self._responder_item(item_id)

    def _responder_item(self, item_id: str) -> None:
        if self._operacion_en_progreso:
            return

        row_info = self.tree_items_info.get(item_id)
        if not row_info:
            messagebox.showerror(
                "Error",
                "No se encontró la información de la petición seleccionada.",
            )
            return

        estado_actual = (row_info.get("Admitido") or "").strip().upper() or "PENDIENTE"
        if estado_actual != "PENDIENTE":
            messagebox.showwarning(
                "Solicitud respondida",
                "Esta petición ya cuenta con una respuesta registrada.",
            )
            return

        self._operacion_en_progreso = True
        try:
            responder_peticion(
                self.window,
                row_info.get("doc_id") or "",
                row_info.get("uid") or "",
                lambda: self.window.after(0, self.actualizar),
                db_client=self.db,
                responded_by=RESPONDER_IDENTIDAD,
            )
        finally:
            self._operacion_en_progreso = False

    def _on_tree_double_click(self, event) -> None:
        item_id = self.tree.identify_row(event.y)
        if not item_id:
            return
        self._responder_item(item_id)

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
        cols = ["Nombre", "Fecha", "CreadoEn", "Motivo", "Estado"]
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(cols)
            for item in items:
                values = list(self.tree.item(item, "values"))
                writer.writerow(values)
        messagebox.showinfo("Exportar CSV", "Exportación completada.")

    def _on_tree_motion(self, event) -> None:
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            self._clear_tooltip()
            return

        column = self.tree.identify_column(event.x)
        if column != "#4":
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


def abrir_gestion_peticiones(db: firestore.Client, sa_path: Optional[str], project_id: Optional[str]) -> None:
    global ventana_peticiones, ventana_peticiones_app, SERVICE_ACCOUNT_JSON, PROJECT_ID

    if not sa_path or not project_id:
        messagebox.showerror("Error", "Faltan credenciales de Firebase.")
        return

    SERVICE_ACCOUNT_JSON = sa_path
    PROJECT_ID = project_id

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

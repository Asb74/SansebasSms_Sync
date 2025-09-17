import csv
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from typing import Any, Dict, List, Optional

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


class GestionPeticionesUI:
    def __init__(self, window: tk.Toplevel, db: firestore.Client, on_close) -> None:
        self.window = window
        self.db = db
        self.on_close = on_close
        self.data_rows: List[Dict[str, Any]] = []
        self.tree_items_info: Dict[str, Dict[str, Any]] = {}
        self.editor_state: Dict[str, Any] = {"widget": None, "item": None, "old": None}

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
            values=["Todos", "Ok", "Denegado", "Vacío"],
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

        columnas = ("Nombre", "Fecha", "CreadoEn", "Admitido")
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
        self.tree.heading("Admitido", text="Admitido")

        self.tree.column("Nombre", width=220, anchor="w")
        self.tree.column("Fecha", width=140, anchor="center")
        self.tree.column("CreadoEn", width=200, anchor="center")
        self.tree.column("Admitido", width=120, anchor="center")

        self.tree.bind("<Double-1>", self._iniciar_edicion)

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
        widget = self.editor_state.get("widget")
        if widget is not None:
            widget.destroy()
        self.editor_state = {"widget": None, "item": None, "old": None}

    def _populate_tree(self, rows: List[Dict[str, Any]]) -> None:
        self._cerrar_editor()
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.tree_items_info.clear()

        for row in rows:
            nombre = row.get("Nombre") or "Falta"
            fecha_str = _fmt_fecha(row.get("Fecha"))
            creado_str = _fmt_fechahora(row.get("CreadoEn"))
            admitido_val = row.get("Admitido") or ""
            item_id = self.tree.insert(
                "",
                "end",
                values=(nombre, fecha_str, creado_str, admitido_val),
            )
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
                "Nombre": nombre,
                "Fecha": data.get("Fecha"),
                "CreadoEn": data.get("creadoEn"),
                "Admitido": data.get("Admitido"),
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

        filtrados: List[Dict[str, Any]] = []
        for row in self.data_rows:
            nombre = (row.get("Nombre") or "").lower()
            if nombre_filter and nombre_filter not in nombre:
                continue

            if fecha_dt:
                row_fecha = _to_local(row.get("Fecha"))
                if not row_fecha or row_fecha.date() != fecha_dt.date():
                    continue

            admitido_val = row.get("Admitido")
            if estado_filter == "Vacío":
                if admitido_val not in (None, ""):
                    continue
            elif estado_filter != "Todos":
                if (admitido_val or "") != estado_filter:
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
        cols = ["Nombre", "Fecha", "CreadoEn", "Admitido"]
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(cols)
            for item in items:
                values = list(self.tree.item(item, "values"))
                writer.writerow(values)
        messagebox.showinfo("Exportar CSV", "Exportación completada.")

    def _guardar_cambio(self, nuevo_valor: str) -> None:
        widget = self.editor_state.get("widget")
        item_id = self.editor_state.get("item")
        old_value = self.editor_state.get("old") or ""
        if not widget or not item_id:
            return

        widget.destroy()
        self.editor_state = {"widget": None, "item": None, "old": None}

        nuevo_valor = (nuevo_valor or "").strip()
        if not nuevo_valor or nuevo_valor == old_value:
            self.tree.set(item_id, "Admitido", old_value)
            return

        row_info = self.tree_items_info.get(item_id)
        if not row_info:
            messagebox.showerror("Error", "No se encontró la información de la petición.")
            self.tree.set(item_id, "Admitido", old_value)
            return

        try:
            self.db.collection("Peticiones").document(row_info["doc_id"]).update({"Admitido": nuevo_valor})
            fecha_str = _fmt_fecha(row_info.get("Fecha"))
            enviar_push_resultado(
                self.db,
                row_info.get("uid") or "",
                fecha_str,
                nuevo_valor,
                row_info.get("fcmToken"),
            )
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo actualizar la petición: {e}")
            self.tree.set(item_id, "Admitido", old_value)
            return

        row_info["Admitido"] = nuevo_valor
        self.tree.set(item_id, "Admitido", nuevo_valor)
        self.aplicar_filtros()

    def _iniciar_edicion(self, event) -> None:
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            return
        column = self.tree.identify_column(event.x)
        if column != "#4":
            return
        item_id = self.tree.identify_row(event.y)
        if not item_id:
            return

        bbox = self.tree.bbox(item_id, column)
        if not bbox:
            return

        self._cerrar_editor()

        x, y, width, height = bbox
        current_value = self.tree.set(item_id, "Admitido")
        combo = ttk.Combobox(
            self.tree,
            values=["Ok", "Denegado"],
            state="readonly",
        )
        combo.place(x=x, y=y, width=width, height=height)
        combo.set(current_value if current_value in ["Ok", "Denegado"] else "Ok")
        combo.focus_set()

        self.editor_state = {"widget": combo, "item": item_id, "old": current_value}

        def _commit(event=None):
            self._guardar_cambio(combo.get())

        def _cancel(event=None):
            self._cerrar_editor()

        combo.bind("<<ComboboxSelected>>", _commit)
        combo.bind("<FocusOut>", _commit)
        combo.bind("<Return>", _commit)
        combo.bind("<Escape>", _cancel)


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

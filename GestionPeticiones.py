import tkinter as tk
from tkinter import ttk, messagebox
from datetime import timezone
from typing import Dict, Optional, Any

import requests
import google.oauth2.service_account
import google.auth.transport.requests
from firebase_admin import firestore

SCOPES = ["https://www.googleapis.com/auth/firebase.messaging"]

SERVICE_ACCOUNT_JSON: Optional[str] = None
PROJECT_ID: Optional[str] = None

ventana_peticiones: Optional[tk.Toplevel] = None

tree_items_info: Dict[str, Dict[str, Any]] = {}
editor_state: Dict[str, Any] = {"widget": None, "item": None, "old": None}


def to_local(dt):
    if dt is None:
        return None
    if getattr(dt, "tzinfo", None) is None:
        return dt.replace(tzinfo=timezone.utc).astimezone()
    return dt.astimezone()


def fmt_fecha(d):
    return d.strftime("%d-%m-%Y") if d else ""


def fmt_fechahora(d):
    return d.strftime("%d-%m-%Y %H:%M") if d else ""


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


def _limpiar_tree(tree: ttk.Treeview) -> None:
    for item in tree.get_children():
        tree.delete(item)
    tree_items_info.clear()


def _cargar_peticiones(db: firestore.Client, tree: ttk.Treeview) -> None:
    _limpiar_tree(tree)
    try:
        snapshot = db.collection("Peticiones").stream()
    except Exception as e:
        messagebox.showerror("Error", f"No se pudieron leer las peticiones: {e}")
        return

    for doc in snapshot:
        data = doc.to_dict() or {}
        uid = data.get("uid") or ""
        nombre = "Falta"
        token = None
        try:
            if uid:
                user_doc = db.collection("UsuariosAutorizados").document(uid).get()
                if user_doc.exists:
                    user_data = user_doc.to_dict() or {}
                    nombre = user_data.get("Nombre") or "Falta"
                    token = user_data.get("fcmToken")
        except Exception as err:
            print(f"❌ Error obteniendo usuario {uid}: {err}")

        fecha_val = to_local(data.get("Fecha")) if data.get("Fecha") else None
        creado_en_val = to_local(data.get("creadoEn")) if data.get("creadoEn") else None

        fecha_str = fmt_fecha(fecha_val)
        creado_str = fmt_fechahora(creado_en_val)
        admitido = data.get("Admitido") or ""

        item_id = tree.insert(
            "",
            "end",
            values=(nombre, fecha_str, creado_str, admitido),
        )
        tree_items_info[item_id] = {
            "doc_id": doc.id,
            "uid": uid,
            "fcmToken": token,
            "fecha_str": fecha_str,
            "admitido": admitido,
        }


def _cerrar_editor() -> None:
    widget = editor_state.get("widget")
    if widget is not None:
        widget.destroy()
    editor_state.update({"widget": None, "item": None, "old": None})


def _guardar_cambio(db: firestore.Client, tree: ttk.Treeview, nuevo_valor: str) -> None:
    widget = editor_state.get("widget")
    item_id = editor_state.get("item")
    old_value = editor_state.get("old") or ""
    if not widget or not item_id:
        return

    widget.destroy()
    editor_state.update({"widget": None, "item": None, "old": None})

    nuevo_valor = (nuevo_valor or "").strip()
    if not nuevo_valor or nuevo_valor == old_value:
        tree.set(item_id, "Admitido", old_value)
        return

    info = tree_items_info.get(item_id)
    if not info:
        messagebox.showerror("Error", "No se encontró la información de la petición.")
        tree.set(item_id, "Admitido", old_value)
        return

    try:
        db.collection("Peticiones").document(info["doc_id"]).update({"Admitido": nuevo_valor})
        enviar_push_resultado(db, info["uid"], info["fecha_str"], nuevo_valor, info.get("fcmToken"))
    except Exception as e:
        messagebox.showerror("Error", f"No se pudo actualizar la petición: {e}")
        tree.set(item_id, "Admitido", old_value)
        return

    tree.set(item_id, "Admitido", nuevo_valor)
    info["admitido"] = nuevo_valor


def _iniciar_edicion(event, db: firestore.Client, tree: ttk.Treeview) -> None:
    region = tree.identify("region", event.x, event.y)
    if region != "cell":
        return
    column = tree.identify_column(event.x)
    if column != "#4":
        return
    item_id = tree.identify_row(event.y)
    if not item_id:
        return

    bbox = tree.bbox(item_id, column)
    if not bbox:
        return

    _cerrar_editor()

    x, y, width, height = bbox
    current_value = tree.set(item_id, "Admitido")
    combo = ttk.Combobox(tree, values=["Ok", "Denegado"], state="readonly")
    combo.place(x=x, y=y, width=width, height=height)
    combo.set(current_value if current_value in ["Ok", "Denegado"] else "Ok")
    combo.focus_set()

    editor_state.update({"widget": combo, "item": item_id, "old": current_value})

    def _commit(event=None):
        _guardar_cambio(db, tree, combo.get())

    def _cancel(event=None):
        _cerrar_editor()

    combo.bind("<<ComboboxSelected>>", _commit)
    combo.bind("<FocusOut>", _commit)
    combo.bind("<Return>", _commit)
    combo.bind("<Escape>", _cancel)


def abrir_gestion_peticiones(db: firestore.Client, sa_path: Optional[str], project_id: Optional[str]) -> None:
    global ventana_peticiones, SERVICE_ACCOUNT_JSON, PROJECT_ID

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

    ventana_peticiones.grid_rowconfigure(1, weight=1)
    ventana_peticiones.grid_columnconfigure(0, weight=1)

    top_bar = ttk.Frame(ventana_peticiones, padding=10)
    top_bar.grid(row=0, column=0, sticky="ew")
    top_bar.grid_columnconfigure(0, weight=1)

    btn_actualizar = ttk.Button(top_bar, text="Actualizar", command=lambda: _cargar_peticiones(db, tree))
    btn_actualizar.grid(row=0, column=0, sticky="w")

    tree_frame = ttk.Frame(ventana_peticiones, padding=(10, 0, 10, 0))
    tree_frame.grid(row=1, column=0, sticky="nsew")
    tree_frame.grid_rowconfigure(0, weight=1)
    tree_frame.grid_columnconfigure(0, weight=1)

    columnas = ("Nombre", "Fecha", "CreadoEn", "Admitido")

    tree = ttk.Treeview(
        tree_frame,
        columns=columnas,
        show="headings",
        selectmode="browse",
    )

    yscroll = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
    xscroll = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
    tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

    tree.grid(row=0, column=0, sticky="nsew")
    yscroll.grid(row=0, column=1, sticky="ns")
    xscroll.grid(row=1, column=0, sticky="ew")

    tree.heading("Nombre", text="Nombre")
    tree.heading("Fecha", text="Fecha")
    tree.heading("CreadoEn", text="CreadoEn")
    tree.heading("Admitido", text="Admitido")

    tree.column("Nombre", width=220, anchor="w")
    tree.column("Fecha", width=140, anchor="center")
    tree.column("CreadoEn", width=200, anchor="center")
    tree.column("Admitido", width=120, anchor="center")

    tree.bind("<Double-1>", lambda e: _iniciar_edicion(e, db, tree))

    bottom_bar = ttk.Frame(ventana_peticiones, padding=10)
    bottom_bar.grid(row=2, column=0, sticky="ew")
    bottom_bar.grid_columnconfigure(0, weight=1)

    def on_close():
        global ventana_peticiones
        win = ventana_peticiones
        ventana_peticiones = None
        if win is not None:
            win.destroy()

    ttk.Button(bottom_bar, text="Cerrar", command=on_close).grid(
        row=0, column=0, sticky="e"
    )

    ventana_peticiones.protocol("WM_DELETE_WINDOW", on_close)

    _cargar_peticiones(db, tree)

    ventana_peticiones.focus_force()


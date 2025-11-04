import csv
import tkinter as tk
from tkinter import filedialog, simpledialog, ttk, messagebox
import logging
from logging_setup import install_global_excepthook
install_global_excepthook()
logging.info("SansebasSms Sync iniciado")
from ui_safety import info, error
from thread_utils import run_bg
logger = logging.getLogger(__name__)
import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
import datetime
import os
import json
import requests
import time
from dateutil import parser
from PIL import Image, ImageTk
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from GestionUsuarios import abrir_gestion_usuarios
from GestionMensajes import abrir_gestion_mensajes
from GenerarMensajes import abrir_generar_mensajes
from notificaciones_push import NOTI_DB, enviar_push_por_mensaje
from utils_mensajes import build_mensaje_id
import re
from decimal import Decimal
from typing import List, Optional, Tuple
from google.cloud.firestore_v1.base_query import FieldFilter

try:
    from tkcalendar import DateEntry
except Exception:
    DateEntry = None  # type: ignore
    logger.warning(
        "Instale tkcalendar para habilitar selectores de fecha: pip install tkcalendar"
    )


def _safe_str(v) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s else None
    if isinstance(v, (int, float, Decimal)):
        return str(v)
    if isinstance(v, (bytes, bytearray)):
        for enc in ("utf-8", "latin-1", "cp1252"):
            try:
                s = v.decode(enc, errors="ignore").strip()
                return s if s else None
            except Exception:
                continue
        return None
    s = str(v).strip()
    return s if s else None


_FCM_TOKEN_RE = re.compile(r"^[A-Za-z0-9_\-:.]{100,}$")


def _is_valid_fcm_token(token: Optional[str]) -> bool:
    if not token:
        return False
    if not isinstance(token, str):
        token = _safe_str(token)
        if not token:
            return False
    return bool(_FCM_TOKEN_RE.match(token))


# 🔧 Configuración inicial
credenciales_dinamicas = {"ruta": "sansebassms.json"}
project_info = {"id": None}
carpeta_excel = {"ruta": None}
archivo_notificados = NOTI_DB

ventana: Optional[tk.Misc] = None
estado: Optional[tk.StringVar] = None


def _get_root() -> Optional[tk.Misc]:
    if ventana is None:
        return None
    try:
        return ventana.winfo_toplevel()
    except Exception:
        return ventana


def _set_estado_async(texto: str) -> None:
    root_ref = _get_root()
    if root_ref is None or estado is None:
        return
    root_ref.after(0, lambda: estado.set(texto))


def _leer_notificados_local() -> list[str]:
    if not os.path.exists(archivo_notificados):
        return []
    try:
        with open(archivo_notificados, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            ids = data.get("ids", [])
        elif isinstance(data, list):
            ids = data
        else:
            ids = []
        return [str(i) for i in ids if i]
    except Exception:
        logger.exception("No se pudo leer %s", archivo_notificados)
        return []


def _guardar_notificados_local(ids: list[str]) -> None:
    try:
        with open(archivo_notificados, "w", encoding="utf-8") as f:
            json.dump({"ids": sorted(set(ids))}, f, ensure_ascii=False, indent=2)
    except Exception:
        logger.exception("No se pudo guardar %s", archivo_notificados)


def with_retry(fn, tries: int = 3, base: float = 0.5, cap: float = 5.0):
    """Ejecuta `fn` con reintentos exponenciales."""

    last_exc: Optional[Exception] = None
    for intento in range(1, tries + 1):
        try:
            return fn()
        except Exception as exc:  # pragma: no cover - logging side effect
            last_exc = exc
            if intento >= tries:
                logger.exception("Operación falló tras %s intentos", tries)
                raise
            delay = min(cap, base * (2 ** (intento - 1)))
            logger.warning(
                "Intento %s/%s fallido, reintentando en %.2f s", intento, tries, delay, exc_info=exc
            )
            time.sleep(delay)
    if last_exc is not None:
        raise last_exc


def _commit_with_retry(batch, max_tries: int = 4, base_sleep: float = 0.7):
    """Confirma un batch con reintentos y backoff exponencial."""

    for intento in range(1, max_tries + 1):
        try:
            return batch.commit(timeout=60.0)
        except Exception as exc:  # pragma: no cover - logging side effect
            if intento >= max_tries:
                logger.exception(
                    "No se pudo confirmar el lote tras %s intentos", max_tries
                )
                raise
            delay = min(6.0, base_sleep * (2 ** (intento - 1)))
            logger.warning(
                "Error al confirmar lote (intento %s/%s). Reintentando en %.2f s",
                intento,
                max_tries,
                delay,
                exc_info=exc,
            )
            time.sleep(delay)


def _paged_query(
    collection_ref,
    where_tuple: Tuple[str, str, object],
    order_field: str = "__name__",
    page_size: int = 200,
    start_after=None,
    timeout: float = 30.0,
) -> List:
    """Obtiene una página de documentos ordenados y filtrados."""

    field, op, value = where_tuple
    query = collection_ref.where(filter=FieldFilter(field, op, value)).order_by(order_field)
    if start_after is not None:
        query = query.start_after(start_after)
    return list(query.limit(page_size).stream(timeout=timeout))


def get_doc_safe(doc_ref):
    try:
        doc = doc_ref.get()
    except Exception as exc:
        logger.exception("Error al obtener documento %s", getattr(doc_ref, "id", doc_ref))
        return None
    if not getattr(doc, "exists", False):
        return None
    try:
        return doc.to_dict() or {}
    except Exception as exc:
        logger.exception("Error al convertir documento %s a dict", getattr(doc, "id", doc_ref))
        return None


def iter_collection_safe(col_ref):
    try:
        for doc in col_ref.stream():
            yield doc
    except Exception:
        logger.exception("Error al iterar colección %s", getattr(col_ref, "id", col_ref))


def enviar_fcm(uid: str, token: Optional[str], token_oauth: str, *, notification: dict, data: Optional[dict] = None) -> bool:
    if not _is_valid_fcm_token(token):
        logger.warning("Token FCM inválido para %s, se omite", uid)
        return False

    payload = {"message": {"token": token, "notification": notification}}
    if data:
        payload["message"]["data"] = data

    headers = {
        "Authorization": f"Bearer {token_oauth}",
        "Content-Type": "application/json",
    }

    url = f"https://fcm.googleapis.com/v1/projects/{project_info['id']}/messages:send"
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
    except Exception:
        logger.exception("Error enviando notificación a %s", uid)
        return False

    if response.status_code == 200:
        logger.info("Notificación enviada a %s", uid)
        return True

    logger.error("Error al enviar a %s: %s", uid, response.text)
    return False

# Inicializar Firebase
try:
    if not os.path.exists(credenciales_dinamicas["ruta"]):
        root_dialog = tk.Tk()
        root_dialog.withdraw()
        nueva_ruta = filedialog.askopenfilename(
            title="Selecciona archivo de credenciales",
            filetypes=[("Archivos JSON", "*.json")]
        )
        if not nueva_ruta:
            messagebox.showinfo(
                "Credenciales",
                "No se seleccionó archivo de credenciales. La aplicación se cerrará."
            )
            root_dialog.destroy()
            raise SystemExit(1)
        credenciales_dinamicas["ruta"] = nueva_ruta
        root_dialog.destroy()

    with open(credenciales_dinamicas["ruta"], "r", encoding="utf-8") as f:
        data = json.load(f)
        project_info["id"] = data.get("project_id")

    cred = credentials.Certificate(credenciales_dinamicas["ruta"])
    firebase_admin.initialize_app(cred)
    db = firestore.client()
except Exception as exc:
    logger.exception("Error al inicializar Firebase")
    root_dialog = tk.Tk()
    root_dialog.withdraw()
    messagebox.showerror(
        "Firebase",
        f"No se pudo inicializar Firebase: {exc}"
    )
    root_dialog.destroy()
    raise SystemExit(1)


def abrir_gestion_peticiones(db):
    from GestionPeticiones import abrir_gestion_peticiones as abrir

    sa_path = credenciales_dinamicas.get("ruta")
    project_id = project_info.get("id")
    abrir(db, sa_path, project_id)


def abrir_informes():
    from Informes import abrir_informes as abrir

    sa_path = credenciales_dinamicas.get("ruta")
    project_id = project_info.get("id")
    abrir(db, sa_path, project_id)


def obtener_token_oauth():
    try:
        creds = service_account.Credentials.from_service_account_file(
            credenciales_dinamicas["ruta"],
            scopes=["https://www.googleapis.com/auth/firebase.messaging"]
        )
        creds.refresh(Request())
        return creds.token
    except Exception as exc:
        logger.exception("No se pudo obtener el token de acceso")
        raise RuntimeError(f"No se pudo obtener el token de acceso: {exc}") from exc

def abrir_estado_notificaciones():
    root = _get_root()
    if root is None:
        return

    top = tk.Toplevel(root)
    top.title("📊 Estado notificaciones")
    top.geometry("1200x720")
    top.transient(root)

    filtro_frame = ttk.Frame(top, padding=(10, 10, 10, 0))
    filtro_frame.pack(fill="x")

    ttk.Label(filtro_frame, text="Fecha desde (opcional):").grid(row=0, column=0, sticky="w")

    if DateEntry:
        fecha_desde_widget = DateEntry(filtro_frame, date_pattern="yyyy-mm-dd")
        fecha_desde_widget.grid(row=0, column=1, padx=6, pady=2, sticky="w")
    else:
        fecha_desde_var = tk.StringVar()
        fecha_desde_widget = ttk.Entry(filtro_frame, textvariable=fecha_desde_var, width=12)
        fecha_desde_widget.grid(row=0, column=1, padx=6, pady=2, sticky="w")

    btn_refrescar = ttk.Button(filtro_frame, text="Refrescar")
    btn_refrescar.grid(row=0, column=2, padx=(12, 0), pady=2)

    pendiente_frame = ttk.LabelFrame(top, text="Pendientes de enviar", padding=10)
    pendiente_frame.pack(fill="both", expand=True, padx=10, pady=(10, 5))

    pendiente_columns = (
        "fecha",
        "hora",
        "uid",
        "nombre",
        "telefono",
        "tipo",
        "mensaje",
        "estado",
        "mensaje_id",
    )
    pendiente_headers = (
        "Fecha",
        "Hora",
        "UID",
        "Nombre",
        "Teléfono",
        "Tipo",
        "Mensaje",
        "Estado",
        "MensajeID",
    )
    tree_pend = ttk.Treeview(
        pendiente_frame,
        columns=pendiente_columns,
        show="headings",
        selectmode="extended",
    )
    vsb_pend = ttk.Scrollbar(pendiente_frame, orient="vertical", command=tree_pend.yview)
    hsb_pend = ttk.Scrollbar(pendiente_frame, orient="horizontal", command=tree_pend.xview)
    tree_pend.configure(yscrollcommand=vsb_pend.set, xscrollcommand=hsb_pend.set)
    tree_pend.grid(row=0, column=0, sticky="nsew")
    vsb_pend.grid(row=0, column=1, sticky="ns")
    hsb_pend.grid(row=1, column=0, sticky="ew")
    pendiente_frame.columnconfigure(0, weight=1)
    pendiente_frame.rowconfigure(0, weight=1)

    for col, header in zip(pendiente_columns, pendiente_headers):
        tree_pend.heading(col, text=header)
        if col == "mensaje":
            tree_pend.column(col, width=280, stretch=True)
        elif col in {"fecha", "hora", "estado"}:
            tree_pend.column(col, width=100, stretch=False)
        elif col == "telefono":
            tree_pend.column(col, width=120, stretch=False)
        elif col == "mensaje_id":
            tree_pend.column(col, width=260, stretch=True)
        else:
            tree_pend.column(col, width=140, stretch=False)

    pend_footer = ttk.Frame(pendiente_frame)
    pend_footer.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(6, 0))
    pend_footer.columnconfigure(0, weight=1)

    pend_total_var = tk.StringVar(value="Total: 0")
    ttk.Label(pend_footer, textvariable=pend_total_var).grid(row=0, column=0, sticky="w")

    btn_export_pend = ttk.Button(pend_footer, text="Exportar CSV")
    btn_export_pend.grid(row=0, column=1, padx=4)
    btn_reintentar_pend = ttk.Button(pend_footer, text="Reintentar seleccionados")
    btn_reintentar_pend.grid(row=0, column=2, padx=4)

    incid_frame = ttk.LabelFrame(top, text="Con incidencias", padding=10)
    incid_frame.pack(fill="both", expand=True, padx=10, pady=(5, 10))

    incid_columns = pendiente_columns + ("push_error", "push_enviados", "push_fallidos")
    incid_headers = pendiente_headers + ("pushError", "pushEnviados", "pushFallidos")
    tree_inc = ttk.Treeview(
        incid_frame,
        columns=incid_columns,
        show="headings",
        selectmode="extended",
    )
    vsb_inc = ttk.Scrollbar(incid_frame, orient="vertical", command=tree_inc.yview)
    hsb_inc = ttk.Scrollbar(incid_frame, orient="horizontal", command=tree_inc.xview)
    tree_inc.configure(yscrollcommand=vsb_inc.set, xscrollcommand=hsb_inc.set)
    tree_inc.grid(row=0, column=0, sticky="nsew")
    vsb_inc.grid(row=0, column=1, sticky="ns")
    hsb_inc.grid(row=1, column=0, sticky="ew")
    incid_frame.columnconfigure(0, weight=1)
    incid_frame.rowconfigure(0, weight=1)

    for col, header in zip(incid_columns, incid_headers):
        tree_inc.heading(col, text=header)
        if col == "mensaje":
            tree_inc.column(col, width=280, stretch=True)
        elif col in {"fecha", "hora", "estado"}:
            tree_inc.column(col, width=100, stretch=False)
        elif col == "telefono":
            tree_inc.column(col, width=120, stretch=False)
        elif col == "mensaje_id":
            tree_inc.column(col, width=260, stretch=True)
        elif col in {"push_enviados", "push_fallidos"}:
            tree_inc.column(col, width=110, stretch=False, anchor="center")
        elif col == "push_error":
            tree_inc.column(col, width=260, stretch=True)
        else:
            tree_inc.column(col, width=140, stretch=False)

    incid_footer = ttk.Frame(incid_frame)
    incid_footer.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(6, 0))
    incid_footer.columnconfigure(0, weight=1)

    incid_total_var = tk.StringVar(value="Total: 0")
    ttk.Label(incid_footer, textvariable=incid_total_var).grid(row=0, column=0, sticky="w")

    btn_export_inc = ttk.Button(incid_footer, text="Exportar CSV")
    btn_export_inc.grid(row=0, column=1, padx=4)
    btn_reintentar_inc = ttk.Button(incid_footer, text="Reintentar seleccionados")
    btn_reintentar_inc.grid(row=0, column=2, padx=4)

    pend_data: dict[str, dict] = {}
    inc_data: dict[str, dict] = {}

    def _safe_str(value) -> str:
        if value is None:
            return ""
        return str(value)

    def _to_datetime(value: object) -> Optional[datetime.datetime]:
        if value is None:
            return None
        if hasattr(value, "to_datetime"):
            try:
                value = value.to_datetime()
            except Exception:
                return None
        if isinstance(value, datetime.datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=datetime.timezone.utc)
            return value
        return None

    def _parse_fecha_desde() -> Optional[datetime.datetime]:
        if DateEntry and isinstance(fecha_desde_widget, DateEntry):
            try:
                selected = fecha_desde_widget.get_date()
            except Exception:
                return None
            if not selected:
                return None
            tz_local = datetime.datetime.now().astimezone().tzinfo or datetime.timezone.utc
            return datetime.datetime(selected.year, selected.month, selected.day, tzinfo=tz_local).astimezone(datetime.timezone.utc)
        valor = fecha_desde_widget.get().strip()
        if not valor:
            return None
        try:
            fecha = datetime.datetime.strptime(valor, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError("Fecha inválida. Usa AAAA-MM-DD.") from exc
        tz_local = datetime.datetime.now().astimezone().tzinfo or datetime.timezone.utc
        return datetime.datetime(fecha.year, fecha.month, fecha.day, tzinfo=tz_local).astimezone(datetime.timezone.utc)

    def _actualizar_botones():
        btn_export_pend.config(state="normal" if pend_data else "disabled")
        btn_reintentar_pend.config(state="normal" if pend_data else "disabled")
        btn_export_inc.config(state="normal" if inc_data else "disabled")
        btn_reintentar_inc.config(state="normal" if inc_data else "disabled")

    def _exportar_csv(nombre_base: str, data_map: dict[str, dict], headers: tuple[str, ...]):
        if not data_map:
            info(root, "Exportar", "No hay datos para exportar.")
            return
        ruta = filedialog.asksaveasfilename(
            parent=top,
            defaultextension=".csv",
            initialfile=f"{nombre_base}.csv",
            filetypes=[("CSV", "*.csv"), ("Todos", "*.*")],
        )
        if not ruta:
            return
        try:
            with open(ruta, "w", encoding="utf-8", newline="") as fh:
                writer = csv.writer(fh)
                writer.writerow(headers)
                for item in data_map.values():
                    writer.writerow(item["values"])
        except Exception as exc:
            logger.exception("No se pudo exportar CSV")
            error(root, "Exportar", f"No se pudo exportar el CSV: {exc}")
            return
        info(root, "Exportar", f"Archivo exportado: {ruta}")

    def _datos_para_tabla(doc_id: str, doc_data: dict, user_data: dict, include_push: bool) -> tuple[str, ...]:
        fecha = _safe_str(doc_data.get("dia"))
        hora = _safe_str(doc_data.get("hora"))
        fecha_ts = _to_datetime(doc_data.get("fechaHora"))
        if fecha_ts:
            local_dt = fecha_ts.astimezone()
            if not fecha:
                fecha = local_dt.strftime("%Y-%m-%d")
            if not hora:
                hora = local_dt.strftime("%H:%M")
        uid = _safe_str(doc_data.get("uid"))
        nombre = _safe_str(user_data.get("Nombre") or doc_data.get("Nombre"))
        telefono = _safe_str(
            doc_data.get("telefono")
            or user_data.get("Telefono")
            or user_data.get("telefono")
        )
        tipo = _safe_str(doc_data.get("tipo"))
        mensaje = _safe_str(doc_data.get("mensaje") or doc_data.get("cuerpo"))
        estado = _safe_str(doc_data.get("estado"))
        base = (
            fecha,
            hora,
            uid,
            nombre,
            telefono,
            tipo,
            mensaje,
            estado,
            doc_id,
        )
        if not include_push:
            return base
        push_error = _safe_str(doc_data.get("pushError"))
        push_enviados = _safe_str(doc_data.get("pushEnviados"))
        push_fallidos = _safe_str(doc_data.get("pushFallidos"))
        return base + (push_error, push_enviados, push_fallidos)

    def _consultar(fecha_desde_utc: Optional[datetime.datetime]):
        pendientes: list[tuple[str, dict]] = []
        incidencias: list[tuple[str, dict]] = []
        vistos: set[str] = set()

        query_pend = db.collection("Mensajes").where(filter=FieldFilter("estado", "==", "Pendiente"))
        if fecha_desde_utc is not None:
            query_pend = query_pend.where(filter=FieldFilter("fechaHora", ">=", fecha_desde_utc))
        snapshot_pend = with_retry(lambda: list(query_pend.stream()))
        for doc in snapshot_pend:
            data = doc.to_dict() or {}
            pendientes.append((doc.id, data))
            vistos.add(doc.id)

        try:
            query_null = db.collection("Mensajes").where(filter=FieldFilter("estado", "==", None))
            if fecha_desde_utc is not None:
                query_null = query_null.where(filter=FieldFilter("fechaHora", ">=", fecha_desde_utc))
            snapshot_null = with_retry(lambda: list(query_null.stream()))
        except Exception:
            logger.exception("No se pudo consultar mensajes con estado nulo")
            snapshot_null = []
        for doc in snapshot_null:
            if doc.id in vistos:
                continue
            data = doc.to_dict() or {}
            pendientes.append((doc.id, data))
            vistos.add(doc.id)

        try:
            if fecha_desde_utc is not None:
                extra_query = (
                    db.collection("Mensajes")
                    .where(filter=FieldFilter("fechaHora", ">=", fecha_desde_utc))
                    .order_by("fechaHora")
                )
            else:
                extra_query = (
                    db.collection("Mensajes")
                    .order_by("fechaHora", direction=firestore.Query.DESCENDING)
                    .limit(200)
                )
            extra_docs = with_retry(lambda: list(extra_query.stream()))
        except Exception:
            logger.exception("No se pudo obtener mensajes adicionales para pendientes")
            extra_docs = []
        for doc in extra_docs:
            if doc.id in vistos:
                continue
            data = doc.to_dict() or {}
            estado_val = data.get("estado")
            if estado_val in (None, "", "Pendiente"):
                pendientes.append((doc.id, data))
                vistos.add(doc.id)

        query_inc = db.collection("Mensajes").where(
            filter=FieldFilter("estado", "in", ["ErrorPush", "Parcial", "SinToken"])
        )
        if fecha_desde_utc is not None:
            query_inc = query_inc.where(filter=FieldFilter("fechaHora", ">=", fecha_desde_utc))
        snapshot_inc = with_retry(lambda: list(query_inc.stream()))
        for doc in snapshot_inc:
            data = doc.to_dict() or {}
            incidencias.append((doc.id, data))

        return pendientes, incidencias

    def refrescar():
        try:
            fecha_desde_utc = _parse_fecha_desde()
        except ValueError as exc:
            messagebox.showerror("Fecha", str(exc), parent=top)
            return

        btn_refrescar.config(state="disabled")
        pend_total_var.set("Total: …")
        incid_total_var.set("Total: …")

        def worker():
            try:
                pendientes, incidencias = _consultar(fecha_desde_utc)
            except Exception as exc:
                logger.exception("No se pudo cargar el estado de notificaciones")

                def _error():
                    btn_refrescar.config(state="normal")
                    error(root, "Estado notificaciones", f"No se pudo cargar el estado: {exc}")

                top.after(0, _error)
                return

            uid_cache: dict[str, dict] = {}

            def _usuario(uid: str) -> dict:
                if not uid:
                    return {}
                if uid not in uid_cache:
                    uid_cache[uid] = get_doc_safe(
                        db.collection("UsuariosAutorizados").document(uid)
                    ) or {}
                return uid_cache[uid]

            pend_rows: list[dict] = []
            for doc_id, data in pendientes:
                usuario = _usuario(_safe_str(data.get("uid")))
                valores = _datos_para_tabla(doc_id, data, usuario, include_push=False)
                pend_rows.append({"doc_id": doc_id, "doc": data, "usuario": usuario, "values": valores})

            inc_rows: list[dict] = []
            for doc_id, data in incidencias:
                usuario = _usuario(_safe_str(data.get("uid")))
                valores = _datos_para_tabla(doc_id, data, usuario, include_push=True)
                inc_rows.append({"doc_id": doc_id, "doc": data, "usuario": usuario, "values": valores})

            def _actualizar():
                btn_refrescar.config(state="normal")

                for tree in (tree_pend, tree_inc):
                    tree.delete(*tree.get_children())

                pend_data.clear()
                for row in pend_rows:
                    tree_pend.insert("", "end", iid=row["doc_id"], values=row["values"])
                    pend_data[row["doc_id"]] = row
                pend_total_var.set(f"Total: {len(pend_rows)}")

                inc_data.clear()
                for row in inc_rows:
                    tree_inc.insert("", "end", iid=row["doc_id"], values=row["values"])
                    inc_data[row["doc_id"]] = row
                incid_total_var.set(f"Total: {len(inc_rows)}")

                _actualizar_botones()

            top.after(0, _actualizar)

        run_bg(worker, _thread_name="estado_notificaciones_refresh")

    def _toggle_operaciones(state: str):
        btn_refrescar.config(state=state)
        btn_reintentar_pend.config(state=state)
        btn_reintentar_inc.config(state=state)

    def _reintentar_desde(tree_widget: ttk.Treeview):
        seleccion = list(tree_widget.selection())
        if not seleccion:
            info(root, "Reintentar", "Selecciona al menos un mensaje para reintentar.")
            return

        _toggle_operaciones("disabled")

        def worker():
            total_env = total_fall = dedupe = 0
            errores_locales: list[str] = []

            for doc_id in seleccion:
                try:
                    snap = with_retry(lambda: db.collection("Mensajes").document(doc_id).get())
                except Exception as exc:
                    logger.exception("No se pudo obtener el mensaje %s", doc_id)
                    errores_locales.append(f"{doc_id}: {exc}")
                    continue
                if not getattr(snap, "exists", False):
                    errores_locales.append(f"{doc_id}: documento inexistente")
                    continue
                datos = snap.to_dict() or {}
                uid = _safe_str(datos.get("uid"))
                usuario = get_doc_safe(db.collection("UsuariosAutorizados").document(uid)) if uid else {}
                resultado = enviar_push_por_mensaje(
                    db,
                    doc_id,
                    datos,
                    usuario or {},
                    actualizar_estado=True,
                    force=True,
                )
                env = int(resultado.get("enviados", 0))
                fall = int(resultado.get("fallidos", 0))
                if env == 0 and fall == 0:
                    dedupe += 1
                total_env += env
                total_fall += fall

            def _fin():
                _toggle_operaciones("normal")
                mensajes = []
                if total_env:
                    mensajes.append(f"Enviadas: {total_env}")
                if total_fall:
                    mensajes.append(f"Fallidas: {total_fall}")
                if dedupe:
                    mensajes.append(f"Duplicadas: {dedupe}")
                if errores_locales:
                    detalle = "; ".join(errores_locales[:5])
                    if len(errores_locales) > 5:
                        detalle += " …"
                    mensajes.append(f"Errores: {detalle}")
                texto = ", ".join(mensajes) or "No se envió ninguna notificación."
                if total_env and not total_fall and not errores_locales:
                    info(root, "Reintentar", f"✅ {texto}")
                else:
                    info(root, "Reintentar", texto)
                refrescar()

            top.after(0, _fin)

        run_bg(worker, _thread_name="estado_notificaciones_reintentar")

    def exportar_pendientes():
        _exportar_csv("notificaciones_pendientes", pend_data, pendiente_headers)

    def exportar_incidencias():
        _exportar_csv("notificaciones_incidencias", inc_data, incid_headers)

    btn_refrescar.config(command=refrescar)
    btn_export_pend.config(command=exportar_pendientes)
    btn_export_inc.config(command=exportar_incidencias)
    btn_reintentar_pend.config(command=lambda: _reintentar_desde(tree_pend))
    btn_reintentar_inc.config(command=lambda: _reintentar_desde(tree_inc))

    _actualizar_botones()
    refrescar()

def crear_mensajes_para_todos():
    mensaje = simpledialog.askstring(
        "Nuevo mensaje",
        "Escribe el mensaje que deseas enviar a los usuarios:",
        parent=ventana,
    )
    if not mensaje:
        return

    run_bg(
        lambda: _crear_mensajes_para_todos_bg(mensaje),
        _thread_name="crear_mensajes_para_todos",
    )


def _crear_mensajes_para_todos_bg(mensaje: str) -> None:
    root = _get_root()
    try:
        usuarios_col = db.collection("UsuariosAutorizados")
        mensajes_col = db.collection("Mensajes")

        total_creados = 0
        pagina_actual = 0
        ultimo_doc = None

        while True:
            pagina = _paged_query(
                usuarios_col,
                ("Mensaje", "==", True),
                page_size=200,
                start_after=ultimo_doc,
            )

            if not pagina:
                if pagina_actual == 0 and total_creados == 0:
                    info(root, "Sin usuarios", "No hay usuarios con 'Mensaje = true'.")
                    return
                break

            pagina_actual += 1
            _set_estado_async(
                f"📄 Procesando página {pagina_actual} (mensajes creados: {total_creados})"
            )

            batch = db.batch()
            batch_count = 0

            for usuario in pagina:
                uid = usuario.id
                data = usuario.to_dict() or {}
                telefono = data.get("Telefono", "")

                ahora = datetime.datetime.now(datetime.timezone.utc)
                doc_id = build_mensaje_id(uid, ahora)
                local_now = ahora.astimezone()
                doc = {
                    "estado": "Pendiente",
                    "fechaHora": ahora,
                    "mensaje": mensaje,
                    "telefono": telefono,
                    "uid": uid,
                    "dia": local_now.strftime("%Y-%m-%d"),
                    "hora": local_now.strftime("%H:%M"),
                }

                batch.set(mensajes_col.document(doc_id), doc, timeout=30.0)
                batch_count += 1

                if batch_count >= 100:
                    _set_estado_async(
                        f"⬆️ Confirmando lote página {pagina_actual} (total: {total_creados + batch_count})"
                    )
                    _commit_with_retry(batch)
                    total_creados += batch_count
                    batch = db.batch()
                    batch_count = 0

            if batch_count:
                _set_estado_async(
                    f"⬆️ Confirmando lote final página {pagina_actual} (total: {total_creados + batch_count})"
                )
                _commit_with_retry(batch)
                total_creados += batch_count

            ultimo_doc = pagina[-1]

        _set_estado_async(f"✅ Mensajes creados: {total_creados}")
        info(root, "Éxito", f"✅ Se han creado mensajes para {total_creados} usuarios.")
        logger.info("Se crearon %s mensajes pendientes", total_creados)
    except Exception as exc:  # pragma: no cover - logging side effect
        logger.exception("No se pudieron crear los mensajes automáticos")
        _set_estado_async("❌ Error al crear mensajes.")
        error(root, "Error", f"No se pudieron crear los mensajes: {exc}")

# Funciones de sincronización (descargar, subir, etc.)
def seleccionar_carpeta_destino():
    carpeta = filedialog.askdirectory()
    if carpeta:
        carpeta_excel["ruta"] = carpeta
        if estado is not None:
            estado.set(f"📁 Carpeta de destino seleccionada:\n{carpeta}")

def limpiar_fechas(doc):
    limpio = {}
    for k, v in doc.items():
        if isinstance(v, datetime.datetime):
            v = v.astimezone(datetime.timezone.utc).replace(tzinfo=None).isoformat()
        limpio[k] = v
    return limpio

def tipo_de_valor(valor):
    if isinstance(valor, bool): return "bool"
    if isinstance(valor, int): return "int"
    if isinstance(valor, float): return "float"
    if isinstance(valor, datetime.datetime): return "datetime"
    if isinstance(valor, list): return "list"
    if isinstance(valor, dict): return "dict"
    return "str"

def convertir_desde_tipos(dic, tipos):
    resultado = {}
    for k, v in dic.items():
        tipo = tipos.get(k, "str")
        try:
            if tipo == "datetime" and isinstance(v, str):
                resultado[k] = parser.isoparse(v)
            elif tipo == "int":
                resultado[k] = int(v)
            elif tipo == "float":
                resultado[k] = float(v)
            elif tipo == "bool":
                resultado[k] = str(v).strip().lower() in ["true", "sí"]
            elif tipo in ["list", "dict"]:
                import ast
                resultado[k] = ast.literal_eval(v) if isinstance(v, str) else v
            else:
                resultado[k] = str(v)
        except:
            resultado[k] = v
    return resultado

def descargar_todo():
    if not carpeta_excel["ruta"]:
        error(_get_root(), "Carpeta no seleccionada", "Debes seleccionar una carpeta de destino primero.")
        return
    root = _get_root()

    def worker():
        try:
            colecciones = list(db.collections())
            if not colecciones:
                info(root, "Descarga", "No se encontraron colecciones en Firestore.")
                _set_estado_async("Sin colecciones para descargar.")
                return

            for coleccion in colecciones:
                nombre = getattr(coleccion, "id", "coleccion")
                _set_estado_async(f"⏳ Descargando: {nombre}...")

                datos: list[dict] = []
                tipos: dict[str, str] = {}

                for doc in iter_collection_safe(coleccion):
                    raw = doc.to_dict() or {}
                    limpio = limpiar_fechas(raw)
                    limpio["_id"] = doc.id
                    datos.append(limpio)
                    for k, v in raw.items():
                        tipos[k] = tipo_de_valor(v)

                if datos:
                    ruta_archivo = os.path.join(carpeta_excel["ruta"], f"{nombre}.xlsx")
                    with pd.ExcelWriter(ruta_archivo, engine="openpyxl") as writer:
                        pd.DataFrame(datos).to_excel(writer, sheet_name="datos", index=False)
                        pd.DataFrame([
                            {"campo": k, "tipo": v} for k, v in tipos.items()
                        ]).to_excel(writer, sheet_name="tipos", index=False)

            info(root, "Éxito", "Todas las colecciones fueron exportadas.")
            _set_estado_async("✅ Descarga completada.")
        except Exception as exc:
            logger.exception("Error al descargar colecciones")
            error(root, "Error", str(exc))
            _set_estado_async("❌ Error al descargar.")

    run_bg(worker, _thread_name="descargar_todo")

def subir_archivo():
    archivo = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
    if not archivo:
        return
    try:
        eliminar_faltantes = bool(eliminar_var.get()) if eliminar_var is not None else True
    except Exception:
        eliminar_faltantes = True

    root = _get_root()

    def worker():
        try:
            nombre_coleccion = os.path.splitext(os.path.basename(archivo))[0]
            df = pd.read_excel(archivo, sheet_name="datos")
            df_tipos = pd.read_excel(archivo, sheet_name="tipos")
            tipos_dict = dict(zip(df_tipos["campo"], df_tipos["tipo"]))

            if "_id" not in df.columns:
                error(root, "Error", "El archivo no contiene una columna '_id'")
                return

            _set_estado_async(f"⬆️ Subiendo: {nombre_coleccion}...")

            ids_excel = set()
            for _, fila in df.iterrows():
                doc_id = str(fila["_id"])
                datos_limpios = fila.drop("_id").dropna().to_dict()
                data = convertir_desde_tipos(datos_limpios, tipos_dict)
                db.collection(nombre_coleccion).document(doc_id).set(data)
                ids_excel.add(doc_id)

            if eliminar_faltantes:
                for doc in iter_collection_safe(db.collection(nombre_coleccion)):
                    if doc.id not in ids_excel:
                        db.collection(nombre_coleccion).document(doc.id).delete()

            info(root, "Éxito", f"Archivo '{nombre_coleccion}.xlsx' sincronizado.")
            _set_estado_async("✅ Subida completada.")
        except Exception as exc:
            logger.exception("Error al subir archivo")
            error(root, "Error", str(exc))
            _set_estado_async("❌ Error al subir archivo.")

    run_bg(worker, _thread_name="subir_archivo")

def revisar_mensajes():
    root = _get_root()

    def worker():
        try:
            notificados = _leer_notificados_local()

            nuevos = []
            snapshot = with_retry(
                lambda: db.collection("Mensajes").where(
                    filter=FieldFilter("estado", "==", "Pendiente")
                ).get()
            )
            for doc in snapshot:
                if doc.id not in notificados:
                    mensaje = (doc.to_dict() or {}).get("mensaje", "(sin mensaje)")
                    nuevos.append((doc.id, mensaje))
                    info(root, "📨 Mensaje nuevo", f"{mensaje}")
                    notificados.append(doc.id)

            if nuevos:
                _guardar_notificados_local(notificados)
            else:
                info(root, "Mensajes", "No hay mensajes pendientes nuevos.")
        except Exception as exc:
            logger.exception("Error al revisar mensajes")
            error(root, "Error", f"Error al revisar mensajes: {exc}")

    run_bg(worker, _thread_name="revisar_mensajes")

# Interfaz
ventana = tk.Tk()
ventana.title("Sansebassms Sync")
ventana.geometry("500x580")
ventana.resizable(False, False)

try:
    # Establecer el icono como predeterminado para todas las ventanas
    ventana.iconphoto(True, tk.PhotoImage(file="icono_app.png"))
except:
    pass

frame = tk.Frame(ventana, padx=20, pady=20)
frame.pack(fill="both", expand=True)

try:
    img = Image.open("icono_app.png")
    img = img.resize((64, 64), Image.Resampling.LANCZOS)
    img_tk = ImageTk.PhotoImage(img)
    tk.Label(frame, image=img_tk).pack(pady=(0, 10))
except:
    pass

tk.Button(frame, text="📁 Seleccionar carpeta de destino", command=seleccionar_carpeta_destino, height=2, width=40).pack(pady=5)
tk.Button(frame, text="📥 Descargar todas las colecciones", command=descargar_todo, height=2, width=40).pack(pady=5)
tk.Button(frame, text="📤 Subir archivo Excel a Firebase", command=subir_archivo, height=2, width=40).pack(pady=5)
SHOW_REVISAR_BTN = False
# Botón ocultado a petición: "Revisar mensajes pendientes"
btn_revisar = tk.Button(frame, text="📨 Revisar mensajes pendientes", command=revisar_mensajes, height=2, width=40)
btn_revisar.pack(pady=5)
if not SHOW_REVISAR_BTN:
    btn_revisar.pack_forget()

btn_crear_auto = tk.Button(
    frame,
    text="📝 Crear mensajes automáticos",
    command=crear_mensajes_para_todos,
    height=2,
    width=40,
    bg="lightblue",
)
btn_crear_auto.pack(pady=5)
btn_crear_auto.pack_forget()  # Botón ocultado a petición: "Crear mensajes automáticos"
tk.Button(
    frame,
    text="📊 Estado notificaciones",
    command=abrir_estado_notificaciones,
    height=2,
    width=40,
    bg="lightgreen",
).pack(pady=5)
tk.Button(frame, text="👥 Gestionar Usuarios", command=lambda: abrir_gestion_usuarios(db), height=2, width=40, bg="lightyellow").pack(pady=5)
tk.Button(frame, text="📜 Gestionar Mensajes", command=lambda: abrir_gestion_mensajes(db), height=2, width=40).pack(pady=5)
tk.Button(frame, text="Peticiones de Días Libres", command=lambda: abrir_gestion_peticiones(db), height=2, width=40).pack(pady=5)
tk.Button(frame, text="Informe", command=abrir_informes, height=2, width=40).pack(pady=5)
tk.Button(frame, text="🆕 Generar mensajes", command=lambda: abrir_generar_mensajes(db), height=2, width=40).pack(pady=5)


eliminar_var = tk.BooleanVar(value=True)
tk.Checkbutton(frame, text="Eliminar documentos no presentes en el Excel", variable=eliminar_var).pack(pady=5)

estado = tk.StringVar(value="Estado: Esperando acción...")
tk.Label(frame, textvariable=estado, fg="blue").pack(pady=10)

ventana.mainloop()

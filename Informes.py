"""Módulo de informes Fichajes001 (Tkinter + Firestore + Access)."""
from __future__ import annotations

import csv
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, time as time_cls, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

try:
    from tkcalendar import DateEntry
except Exception:  # pragma: no cover - tkcalendar opcional
    DateEntry = None  # type: ignore

try:  # pragma: no cover - pandas opcional en tiempo de ejecución
    import pandas as pd
except Exception:  # pragma: no cover - pandas opcional
    pd = None  # type: ignore

try:  # pragma: no cover - pyodbc puede no estar disponible en todas las plataformas
    import pyodbc
except Exception:  # pragma: no cover - pyodbc opcional
    pyodbc = None  # type: ignore

from firebase_admin import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

from thread_utils import run_bg

logger = logging.getLogger(__name__)

# Intentamos reutilizar utilidades existentes para cálculos horarios y conexión Access
try:  # pragma: no cover - importación condicional
    from GestionUsuarios import (  # type: ignore
        ACCESS_DB_PATH as _ACCESS_DB_PATH,
        end_of_day_local_to_utc as _end_of_day_local_to_utc,
        start_of_day_local_to_utc as _start_of_day_local_to_utc,
    )
except Exception:  # pragma: no cover - el módulo puede no estar disponible en tests
    _ACCESS_DB_PATH = None
    _start_of_day_local_to_utc = None
    _end_of_day_local_to_utc = None


ACCESS_DB_PATH = _ACCESS_DB_PATH or r"X:\\ENLACES\\Power BI\\Campaña\\PercecoBi(Campaña).mdb"


def _start_of_day(dt_value: date) -> datetime:
    if _start_of_day_local_to_utc:
        return _start_of_day_local_to_utc(dt_value)
    tz = datetime.now().astimezone().tzinfo or timezone.utc
    return datetime(dt_value.year, dt_value.month, dt_value.day, tzinfo=tz).astimezone(timezone.utc)


def _end_of_day(dt_value: date) -> datetime:
    if _end_of_day_local_to_utc:
        return _end_of_day_local_to_utc(dt_value)
    tz = datetime.now().astimezone().tzinfo or timezone.utc
    local = datetime(dt_value.year, dt_value.month, dt_value.day, 23, 59, 59, 999000, tzinfo=tz)
    return local.astimezone(timezone.utc)


@dataclass(frozen=True, order=True)
class _SortKey:
    priority: int
    value: Any


ventana_informes: Optional[tk.Toplevel] = None
_db: Optional[firestore.Client] = None
_date_picker: Optional[Any] = None
_date_var: Optional[tk.StringVar] = None
btn_generar: Optional[tk.Button] = None

tree_llamados: Optional[ttk.Treeview] = None
tree_sin_mensaje: Optional[ttk.Treeview] = None

COLUMNAS_LLAMADOS: Sequence[str] = (
    "Fecha",
    "HoraEnvio",
    "UID",
    "Nombre",
    "DNI",
    "Telefono",
    "Turno",
    "MensajeID",
    "EstadoMensaje",
    "Respuesta",
    "Asistio",
    "Codigo",
)

COLUMNAS_SIN_MENSAJE: Sequence[str] = (
    "Fecha",
    "DNI",
    "Nombre",
    "Telefono",
    "Turno",
    "Codigo",
)

datos_llamados: List[Dict[str, Any]] = []
datos_sin_mensaje: List[Dict[str, Any]] = []
fecha_informe_actual: Optional[date] = None

_RESPUESTAS_CACHE: Dict[str, str] = {}


def abrir_informes(db: firestore.Client, _sa_path: Optional[str] = None, _project_id: Optional[str] = None) -> None:
    """Abre (o enfoca) la ventana de informes."""

    global ventana_informes, _db, _date_picker, _date_var, btn_generar

    if ventana_informes is not None and ventana_informes.winfo_exists():
        ventana_informes.lift()
        ventana_informes.focus_force()
        return

    _db = db

    ventana_informes = tk.Toplevel()
    ventana_informes.title("Informes - Fichajes001")
    ventana_informes.geometry("1200x720")
    ventana_informes.minsize(1000, 600)

    try:
        ventana_informes.iconphoto(True, tk.PhotoImage(file="icono_app.png"))
    except Exception:  # pragma: no cover - icono opcional
        pass

    ventana_informes.grid_rowconfigure(1, weight=1)
    ventana_informes.grid_columnconfigure(0, weight=1)

    cabecera = ttk.Frame(ventana_informes, padding=(15, 10))
    cabecera.grid(row=0, column=0, sticky="ew")
    cabecera.columnconfigure(1, weight=1)

    ttk.Label(cabecera, text="Fecha:", font=("Segoe UI", 10, "bold")).grid(row=0, column=0, sticky="w")

    if DateEntry is not None:
        selector = DateEntry(cabecera, width=12, date_pattern="dd-mm-yyyy")
        selector.set_date(date.today())
        _date_picker = selector
        selector.grid(row=0, column=1, sticky="w", padx=(8, 20))
    else:
        _date_var = tk.StringVar(value=date.today().strftime("%d-%m-%Y"))
        entry = ttk.Entry(cabecera, textvariable=_date_var, width=14)
        entry.grid(row=0, column=1, sticky="w", padx=(8, 20))
        _date_picker = entry

    btn_generar = ttk.Button(cabecera, text="Generar", command=generar_fichajes001, width=18)
    btn_generar.grid(row=0, column=2, sticky="w")

    cuerpo = ttk.Frame(ventana_informes, padding=(15, 0, 15, 15))
    cuerpo.grid(row=1, column=0, sticky="nsew")
    cuerpo.grid_rowconfigure(0, weight=1)
    cuerpo.grid_rowconfigure(1, weight=1)
    cuerpo.grid_columnconfigure(0, weight=1)

    frame_a = ttk.LabelFrame(cuerpo, text="Personas llamadas (mensajes del día)")
    frame_a.grid(row=0, column=0, sticky="nsew", pady=(0, 10))
    frame_a.grid_columnconfigure(0, weight=1)
    frame_a.grid_rowconfigure(0, weight=1)

    frame_b = ttk.LabelFrame(cuerpo, text="Asistieron sin mensaje")
    frame_b.grid(row=1, column=0, sticky="nsew")
    frame_b.grid_columnconfigure(0, weight=1)
    frame_b.grid_rowconfigure(0, weight=1)

    global tree_llamados, tree_sin_mensaje

    contenedor_a = ttk.Frame(frame_a)
    contenedor_a.grid(row=0, column=0, sticky="nsew")
    contenedor_a.grid_rowconfigure(0, weight=1)
    contenedor_a.grid_columnconfigure(0, weight=1)

    tree_llamados = _crear_treeview(contenedor_a, COLUMNAS_LLAMADOS)
    _configurar_colores_tree_llamados()

    botones_a = ttk.Frame(frame_a)
    botones_a.grid(row=1, column=0, sticky="ew", pady=(8, 0))
    botones_a.columnconfigure(0, weight=1)
    botones_a.columnconfigure(1, weight=1)

    ttk.Button(
        botones_a,
        text="Exportar CSV",
        command=exportar_llamados_csv,
    ).grid(row=0, column=0, sticky="ew", padx=(0, 6))

    ttk.Button(
        botones_a,
        text="Exportar Excel",
        command=exportar_llamados_excel,
    ).grid(row=0, column=1, sticky="ew", padx=(6, 0))

    contenedor_b = ttk.Frame(frame_b)
    contenedor_b.grid(row=0, column=0, sticky="nsew")
    contenedor_b.grid_rowconfigure(0, weight=1)
    contenedor_b.grid_columnconfigure(0, weight=1)

    tree_sin_mensaje = _crear_treeview(contenedor_b, COLUMNAS_SIN_MENSAJE)

    botones_b = ttk.Frame(frame_b)
    botones_b.grid(row=1, column=0, sticky="ew", pady=(8, 0))
    botones_b.columnconfigure(0, weight=1)
    botones_b.columnconfigure(1, weight=1)

    ttk.Button(
        botones_b,
        text="Exportar CSV",
        command=exportar_sin_mensaje_csv,
    ).grid(row=0, column=0, sticky="ew", padx=(0, 6))

    ttk.Button(
        botones_b,
        text="Exportar Excel",
        command=exportar_sin_mensaje_excel,
    ).grid(row=0, column=1, sticky="ew", padx=(6, 0))

    def _on_close() -> None:
        global ventana_informes, tree_llamados, tree_sin_mensaje, _date_picker, btn_generar
        if ventana_informes is not None:
            ventana_informes.destroy()
        ventana_informes = None
        tree_llamados = None
        tree_sin_mensaje = None
        _date_picker = None
        btn_generar = None

    ventana_informes.protocol("WM_DELETE_WINDOW", _on_close)


def _crear_treeview(parent: tk.Misc, columnas: Sequence[str]) -> ttk.Treeview:
    tree = ttk.Treeview(parent, columns=columnas, show="headings", selectmode="browse")

    xscroll = ttk.Scrollbar(parent, orient="horizontal", command=tree.xview)
    yscroll = ttk.Scrollbar(parent, orient="vertical", command=tree.yview)
    tree.configure(xscrollcommand=xscroll.set, yscrollcommand=yscroll.set)

    tree.grid(row=0, column=0, sticky="nsew")
    yscroll.grid(row=0, column=1, sticky="ns")
    xscroll.grid(row=1, column=0, sticky="ew")

    for col in columnas:
        tree.heading(col, text=col, anchor="center")
        tree.column(col, anchor="center", width=_width_sugerido(col))

    _configurar_ordenacion_columnas(tree, columnas)
    return tree


def _width_sugerido(col: str) -> int:
    largos: Dict[str, int] = {
        "Fecha": 90,
        "HoraEnvio": 90,
        "UID": 140,
        "Nombre": 160,
        "DNI": 110,
        "Telefono": 120,
        "Turno": 80,
        "MensajeID": 150,
        "EstadoMensaje": 140,
        "Respuesta": 180,
        "Asistio": 90,
        "Codigo": 100,
    }
    return largos.get(col, 120)


def _configurar_ordenacion_columnas(tree: ttk.Treeview, columnas: Sequence[str]) -> None:
    estados: Dict[str, Optional[str]] = {col: None for col in columnas}

    def ordenar(col: str) -> None:
        datos = [(tree.set(iid, col), iid) for iid in tree.get_children("")]
        reverse = estados[col] == "asc"

        datos.sort(key=lambda par: _sort_key(par[0]), reverse=reverse)

        for idx, (_, iid) in enumerate(datos):
            tree.move(iid, "", idx)

        estados[col] = "desc" if reverse else "asc"
        for c in columnas:
            texto = c
            if estados[c] == "asc":
                texto += " ▲"
            elif estados[c] == "desc":
                texto += " ▼"
            tree.heading(c, text=texto, command=lambda col=c: ordenar(col))

    for col in columnas:
        tree.heading(col, text=col, command=lambda col=col: ordenar(col))


def _sort_key(value: Any) -> _SortKey:
    if value is None:
        return _SortKey(5, "")
    if isinstance(value, (int, float)):
        return _SortKey(0, float(value))
    texto = str(value).strip()
    if not texto:
        return _SortKey(5, "")
    for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            return _SortKey(0, datetime.strptime(texto, fmt))
        except Exception:
            pass
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return _SortKey(1, datetime.strptime(texto, fmt))
        except Exception:
            pass
    try:
        return _SortKey(2, float(texto.replace(",", ".")))
    except Exception:
        pass
    return _SortKey(3, texto.lower())


def _configurar_colores_tree_llamados() -> None:
    if tree_llamados is None:
        return
    tree_llamados.tag_configure("asistio_si", background="#e8f6ec")
    tree_llamados.tag_configure("asistio_no", background="#fcebea")


def _obtener_fecha_seleccionada() -> date:
    if DateEntry is not None and isinstance(_date_picker, DateEntry):
        return _date_picker.get_date()  # type: ignore[return-value]
    if _date_picker is None:
        raise ValueError("No se ha inicializado el selector de fecha")
    texto = _date_picker.get().strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(texto, fmt).date()
        except Exception:
            continue
    raise ValueError("Fecha inválida. Utiliza el formato DD-MM-YYYY")


def generar_fichajes001() -> None:
    if _db is None:
        messagebox.showerror("Informes", "No hay conexión con Firestore.")
        return

    try:
        fecha = _obtener_fecha_seleccionada()
    except ValueError as exc:
        messagebox.showwarning("Fecha", str(exc))
        return

    if btn_generar is not None:
        btn_generar.configure(state=tk.DISABLED)

    run_bg(lambda: _generar_fichajes001_bg(fecha), _thread_name="informe_fichajes001")


def _generar_fichajes001_bg(fecha: date) -> None:
    global datos_llamados, datos_sin_mensaje, fecha_informe_actual, _RESPUESTAS_CACHE

    try:
        mensajes = get_mensajes_por_fecha(_db, fecha)
        usuarios_map = get_usuarios_map(_db)
        _RESPUESTAS_CACHE = obtener_respuestas_por_fecha(_db, fecha)

        usuarios_por_dni: Dict[str, Dict[str, Any]] = {}
        usuarios_por_tel: Dict[str, Dict[str, Any]] = {}
        for uid, data in usuarios_map.items():
            datos_usuario = dict(data)
            datos_usuario["UID"] = uid
            dni = _normalizar_dni(data.get("DNI"))
            tel = _normalizar_tel(data.get("Telefono"))
            if dni:
                usuarios_por_dni[dni] = datos_usuario
            if tel:
                usuarios_por_tel[tel] = datos_usuario

        conn = _abrir_access()
        cursor = conn.cursor() if conn else None
        try:
            dni_presentes = get_dni_presentes_access(fecha, cursor)
            codigo_cache: Dict[str, str] = {}

            filas_llamados: List[Dict[str, Any]] = []
            dni_con_mensaje: set[str] = set()

            for mensaje in mensajes:
                uid = mensaje.get("uid")
                telefono_msg = _normalizar_tel(mensaje.get("telefono"))
                usuario = usuarios_map.get(uid) if uid else None
                if usuario is None and telefono_msg:
                    encontrado = usuarios_por_tel.get(telefono_msg)
                    if encontrado:
                        usuario = encontrado
                        uid = encontrado.get("UID")

                dni = _normalizar_dni((usuario or {}).get("DNI"))
                if not dni and telefono_msg:
                    por_tel = usuarios_por_tel.get(telefono_msg)
                    if por_tel:
                        dni = _normalizar_dni(por_tel.get("DNI"))
                        if usuario is None:
                            usuario = por_tel
                            uid = por_tel.get("UID")

                if dni:
                    dni_con_mensaje.add(dni)

                fecha_hora_envio = _to_local_datetime(mensaje.get("fechaHora")) or datetime.combine(fecha, time_cls.min)
                fecha_texto = fecha_hora_envio.strftime("%d/%m/%Y")
                hora_texto = fecha_hora_envio.strftime("%H:%M")

                respuesta = mensaje.get("Respuesta") or mensaje.get("respuesta")
                if not respuesta and uid:
                    respuesta = obtener_respuesta(uid, fecha)
                if not respuesta:
                    respuesta = "N/D"

                asistio = "N/D"
                if dni:
                    asistio = "Sí" if dni in dni_presentes else "No"

                codigo = "N/D"
                if dni:
                    codigo = codigo_cache.get(dni) or get_codigo_access_por_dni(dni, cursor)
                    if codigo:
                        codigo_cache[dni] = codigo
                    else:
                        codigo = "N/D"

                fila = {
                    "Fecha": fecha_texto,
                    "HoraEnvio": hora_texto,
                    "UID": uid or mensaje.get("uid") or "",
                    "Nombre": (usuario or {}).get("Nombre")
                    or mensaje.get("Nombre")
                    or mensaje.get("nombre")
                    or "N/D",
                    "DNI": dni or "DNI N/D",
                    "Telefono": mensaje.get("telefono") or (usuario or {}).get("Telefono") or "N/D",
                    "Turno": (usuario or {}).get("Turno") or "N/D",
                    "MensajeID": mensaje.get("id") or "",
                    "EstadoMensaje": mensaje.get("estado") or "",
                    "Respuesta": respuesta,
                    "Asistio": asistio,
                    "Codigo": codigo,
                }
                filas_llamados.append(fila)

            dni_sin_mensaje = sorted(dni_presentes - dni_con_mensaje)
            filas_sin_mensaje: List[Dict[str, Any]] = []
            for dni in dni_sin_mensaje:
                usuario = usuarios_por_dni.get(dni, {})
                codigo = codigo_cache.get(dni) or get_codigo_access_por_dni(dni, cursor)
                if codigo:
                    codigo_cache[dni] = codigo
                else:
                    codigo = "N/D"
                filas_sin_mensaje.append(
                    {
                        "Fecha": fecha.strftime("%d/%m/%Y"),
                        "DNI": dni,
                        "Nombre": usuario.get("Nombre") or "N/D",
                        "Telefono": usuario.get("Telefono") or "N/D",
                        "Turno": usuario.get("Turno") or "N/D",
                        "Codigo": codigo,
                    }
                )
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

        datos_llamados = filas_llamados
        datos_sin_mensaje = filas_sin_mensaje
        fecha_informe_actual = fecha

        logger.info(
            "Informe Fichajes001 generado para %s: llamados=%s, sin_mensaje=%s",
            fecha.isoformat(),
            len(filas_llamados),
            len(filas_sin_mensaje),
        )

        if ventana_informes and ventana_informes.winfo_exists():
            ventana_informes.after(0, lambda: _actualizar_tablas(filas_llamados, filas_sin_mensaje))
    except Exception as exc:  # pragma: no cover - logging
        logger.exception("Error generando informe Fichajes001")
        if ventana_informes and ventana_informes.winfo_exists():
            ventana_informes.after(0, lambda: messagebox.showerror("Informes", f"No se pudo generar el informe: {exc}"))
    finally:
        if btn_generar is not None and btn_generar.winfo_exists():
            btn_generar.after(0, lambda: btn_generar.configure(state=tk.NORMAL))


def _actualizar_tablas(
    filas_llamados: Sequence[Dict[str, Any]],
    filas_sin_mensaje: Sequence[Dict[str, Any]],
) -> None:
    if tree_llamados is not None:
        tree_llamados.delete(*tree_llamados.get_children(""))
        for fila in filas_llamados:
            tags = ()
            if fila.get("Asistio") == "Sí":
                tags = ("asistio_si",)
            elif fila.get("Asistio") == "No":
                tags = ("asistio_no",)
            tree_llamados.insert(
                "",
                "end",
                values=[_valor_celda(fila, col) for col in COLUMNAS_LLAMADOS],
                tags=tags,
            )

    if tree_sin_mensaje is not None:
        tree_sin_mensaje.delete(*tree_sin_mensaje.get_children(""))
        for fila in filas_sin_mensaje:
            tree_sin_mensaje.insert(
                "",
                "end",
                values=[_valor_celda(fila, col) for col in COLUMNAS_SIN_MENSAJE],
            )


def _abrir_access() -> Optional[pyodbc.Connection]:  # type: ignore[valid-type]
    if pyodbc is None:
        logger.warning("pyodbc no está disponible. Saltando consultas Access.")
        return None
    if not ACCESS_DB_PATH or not os.path.exists(ACCESS_DB_PATH):
        logger.warning("Base Access no encontrada en %s", ACCESS_DB_PATH)
        return None
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={ACCESS_DB_PATH};"
    )
    try:
        return pyodbc.connect(conn_str)
    except Exception as exc:  # pragma: no cover - dependiente del entorno
        logger.warning("No se pudo abrir la base Access: %s", exc)
        return None


def get_dni_presentes_access(fecha: date, cursor: Optional[pyodbc.Cursor] = None) -> set[str]:  # type: ignore[valid-type]
    if pyodbc is None:
        return set()
    close_cursor = False
    conn: Optional[pyodbc.Connection] = None  # type: ignore[valid-type]
    if cursor is None:
        conn = _abrir_access()
        if conn is None:
            return set()
        cursor = conn.cursor()
        close_cursor = True
    try:
        inicio = datetime.combine(fecha, time_cls.min)
        fin = inicio + timedelta(days=1)
        query = (
            "SELECT DISTINCT Trim(UCase(DNI)) AS DNI "
            "FROM DATOS_AJUSTADOS "
            "WHERE FECHA >= ? AND FECHA < ?"
        )
        dni_set: set[str] = set()
        for row in cursor.execute(query, (inicio, fin)).fetchall():
            valor = getattr(row, "DNI", None) if hasattr(row, "DNI") else row[0]
            dni = _normalizar_dni(valor)
            if dni:
                dni_set.add(dni)
        return dni_set
    except Exception as exc:  # pragma: no cover - dependiente del entorno
        logger.warning("Error obteniendo DNI presentes para %s: %s", fecha, exc)
        return set()
    finally:
        if close_cursor and cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def get_codigo_access_por_dni(dni: Optional[str], cursor: Optional[pyodbc.Cursor] = None) -> Optional[str]:  # type: ignore[valid-type]
    if pyodbc is None:
        return None
    dni_norm = _normalizar_dni(dni)
    if not dni_norm:
        return None
    close_cursor = False
    conn: Optional[pyodbc.Connection] = None  # type: ignore[valid-type]
    if cursor is None:
        conn = _abrir_access()
        if conn is None:
            return None
        cursor = conn.cursor()
        close_cursor = True
    try:
        query = (
            "SELECT TOP 1 CODIGO "
            "FROM TRABAJADORES "
            "WHERE Trim(UCase(DNI)) = ? AND CENTRO = '00005' "
            "ORDER BY FECHAALTA DESC"
        )
        row = cursor.execute(query, dni_norm).fetchone()
        if not row:
            return None
        codigo = getattr(row, "CODIGO", None) if hasattr(row, "CODIGO") else row[0]
        if codigo is None:
            return None
        codigo_str = str(codigo).strip()
        return codigo_str or None
    except Exception as exc:  # pragma: no cover - dependiente del entorno
        logger.warning("Error obteniendo código para DNI %s: %s", dni_norm, exc)
        return None
    finally:
        if close_cursor and cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def get_mensajes_por_fecha(db: firestore.Client, fecha: date) -> List[Dict[str, Any]]:
    if db is None:
        return []
    inicio = _start_of_day(fecha)
    fin = _end_of_day(fecha)
    try:
        query = (
            db.collection("Mensajes")
            .where(filter=FieldFilter("fechaHora", ">=", inicio))
            .where(filter=FieldFilter("fechaHora", "<", fin))
        )
        resultados: List[Dict[str, Any]] = []
        for doc in query.stream():
            datos = doc.to_dict() or {}
            datos.setdefault("id", doc.id)
            resultados.append(datos)
        return resultados
    except Exception as exc:  # pragma: no cover - dependiente de Firestore
        logger.warning("Error obteniendo mensajes del %s: %s", fecha, exc)
        return []


def get_usuarios_map(db: firestore.Client) -> Dict[str, Dict[str, Any]]:
    if db is None:
        return {}
    try:
        usuarios: Dict[str, Dict[str, Any]] = {}
        for doc in db.collection("UsuariosAutorizados").stream():
            usuarios[doc.id] = doc.to_dict() or {}
        return usuarios
    except Exception as exc:  # pragma: no cover - dependiente de Firestore
        logger.warning("Error obteniendo usuarios autorizados: %s", exc)
        return {}


def obtener_respuestas_por_fecha(db: firestore.Client, fecha: date) -> Dict[str, str]:
    if db is None:
        return {}
    inicio = _start_of_day(fecha)
    fin = _end_of_day(fecha)
    respuestas: Dict[str, str] = {}
    try:
        coleccion = db.collection("Respuestas")
    except Exception:
        return respuestas
    try:
        consulta = (
            coleccion.where(filter=FieldFilter("fecha", ">=", inicio)).where(filter=FieldFilter("fecha", "<", fin))
        )
        for doc in consulta.stream():
            datos = doc.to_dict() or {}
            uid = datos.get("uid") or datos.get("UID")
            if not uid:
                continue
            for campo in ("respuesta", "Respuesta", "texto", "mensaje"):
                valor = datos.get(campo)
                if valor:
                    respuestas[uid] = str(valor)
                    break
        return respuestas
    except Exception:
        # Puede que la colección no tenga índices o campos esperados; intentamos por documento directo
        for doc in coleccion.stream():
            datos = doc.to_dict() or {}
            fecha_doc = datos.get("fecha") or datos.get("fechaHora")
            dt_value = _to_local_datetime(fecha_doc)
            if not dt_value or dt_value.date() != fecha:
                continue
            uid = datos.get("uid") or datos.get("UID")
            if not uid:
                continue
            for campo in ("respuesta", "Respuesta", "texto", "mensaje"):
                valor = datos.get(campo)
                if valor:
                    respuestas[uid] = str(valor)
                    break
        return respuestas


def obtener_respuesta(uid: Optional[str], fecha: date) -> Optional[str]:
    if not uid:
        return None
    return _RESPUESTAS_CACHE.get(uid)


def exportar_llamados_csv(path: Optional[str] = None) -> None:
    if not datos_llamados:
        messagebox.showinfo("Exportar", "No hay datos para exportar.")
        return
    ruta = path or _dialogo_guardar("csv", sufijo="llamados")
    if not ruta:
        return
    try:
        with open(ruta, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(COLUMNAS_LLAMADOS)
            for fila in datos_llamados:
                writer.writerow([fila.get(col, "") for col in COLUMNAS_LLAMADOS])
        messagebox.showinfo("Exportar", f"Archivo CSV guardado en\n{ruta}")
    except Exception as exc:  # pragma: no cover - IO dependiente
        logger.exception("Error exportando CSV de llamados")
        messagebox.showerror("Exportar", f"No se pudo guardar el CSV: {exc}")


def exportar_sin_mensaje_csv(path: Optional[str] = None) -> None:
    if not datos_sin_mensaje:
        messagebox.showinfo("Exportar", "No hay datos para exportar.")
        return
    ruta = path or _dialogo_guardar("csv", sufijo="sin_mensaje")
    if not ruta:
        return
    try:
        with open(ruta, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(COLUMNAS_SIN_MENSAJE)
            for fila in datos_sin_mensaje:
                writer.writerow([fila.get(col, "") for col in COLUMNAS_SIN_MENSAJE])
        messagebox.showinfo("Exportar", f"Archivo CSV guardado en\n{ruta}")
    except Exception as exc:  # pragma: no cover
        logger.exception("Error exportando CSV sin mensaje")
        messagebox.showerror("Exportar", f"No se pudo guardar el CSV: {exc}")


def exportar_llamados_excel(path: Optional[str] = None) -> None:
    if pd is None:
        messagebox.showwarning("Exportar", "Pandas no está disponible. Instale pandas para exportar a Excel.")
        return
    if not datos_llamados:
        messagebox.showinfo("Exportar", "No hay datos para exportar.")
        return
    ruta = path or _dialogo_guardar("xlsx", sufijo="llamados")
    if not ruta:
        return
    try:
        df = pd.DataFrame(datos_llamados, columns=COLUMNAS_LLAMADOS)
        df.to_excel(ruta, index=False)
        messagebox.showinfo("Exportar", f"Archivo Excel guardado en\n{ruta}")
    except Exception as exc:  # pragma: no cover
        logger.exception("Error exportando Excel de llamados")
        messagebox.showerror("Exportar", f"No se pudo guardar el Excel: {exc}")


def exportar_sin_mensaje_excel(path: Optional[str] = None) -> None:
    if pd is None:
        messagebox.showwarning("Exportar", "Pandas no está disponible. Instale pandas para exportar a Excel.")
        return
    if not datos_sin_mensaje:
        messagebox.showinfo("Exportar", "No hay datos para exportar.")
        return
    ruta = path or _dialogo_guardar("xlsx", sufijo="sin_mensaje")
    if not ruta:
        return
    try:
        df = pd.DataFrame(datos_sin_mensaje, columns=COLUMNAS_SIN_MENSAJE)
        df.to_excel(ruta, index=False)
        messagebox.showinfo("Exportar", f"Archivo Excel guardado en\n{ruta}")
    except Exception as exc:  # pragma: no cover
        logger.exception("Error exportando Excel sin mensaje")
        messagebox.showerror("Exportar", f"No se pudo guardar el Excel: {exc}")


def _dialogo_guardar(extension: str, sufijo: str) -> Optional[str]:
    fecha_texto = fecha_informe_actual.strftime("%Y%m%d") if fecha_informe_actual else datetime.now().strftime("%Y%m%d")
    nombre = f"fichajes001_{sufijo}_{fecha_texto}.{extension}"
    return filedialog.asksaveasfilename(
        defaultextension=f".{extension}",
        filetypes=[(extension.upper(), f"*.{extension}"), ("Todos", "*.*")],
        initialfile=nombre,
        title="Guardar como",
    )


def _to_local_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if hasattr(value, "to_datetime"):
        try:
            value = value.to_datetime()
        except Exception:
            return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc).astimezone()
        return value.astimezone()
    if isinstance(value, date):
        return datetime.combine(value, time_cls.min)
    return None


def _normalizar_dni(valor: Any) -> Optional[str]:
    if not valor:
        return None
    texto = str(valor).strip().upper()
    return texto or None


def _normalizar_tel(valor: Any) -> Optional[str]:
    if not valor:
        return None
    texto = "".join(ch for ch in str(valor) if ch.isdigit())
    return texto or None


def _valor_celda(fila: Dict[str, Any], columna: str) -> str:
    valor = fila.get(columna, "")
    if valor is None:
        return ""
    return str(valor)


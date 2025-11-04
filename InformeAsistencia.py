"""Informe de Control de Asistencia (FICHAJES001)."""
from __future__ import annotations

import csv
import json
import logging
import os
import webbrowser
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

try:  # pragma: no cover - tkcalendar opcional en ejecución
    from tkcalendar import DateEntry
except Exception:  # pragma: no cover - tkcalendar opcional
    DateEntry = None  # type: ignore

try:  # pragma: no cover - pandas opcional
    import pandas as pd
except Exception:  # pragma: no cover - pandas opcional
    pd = None  # type: ignore

try:  # pragma: no cover - pyodbc puede no estar disponible
    import pyodbc
except Exception:  # pragma: no cover - pyodbc opcional
    pyodbc = None  # type: ignore

if pyodbc is not None:
    ProgrammingError = pyodbc.ProgrammingError
else:  # pragma: no cover - pyodbc ausente
    class ProgrammingError(Exception):
        pass

from firebase_admin import firestore
from google.api_core.exceptions import FailedPrecondition

from thread_utils import run_bg

logger = logging.getLogger(__name__)


CONFIG_PATH = Path("config.json")


class TablaFichajesNoEncontradaError(RuntimeError):
    """Excepción usada cuando la tabla de fichajes no está disponible."""

    def __init__(self, mensaje: str) -> None:
        super().__init__(mensaje)
        self.already_notified = True


_cfg_cache: Optional[Dict[str, Any]] = None
_conn_fich: Optional[Any] = None
_conn_fich_path: Optional[str] = None


_COLUMNAS_LLAMADOS: Sequence[str] = (
    "Fecha",
    "Hora",
    "Mensaje",
    "Estado Mensaje",
    "Nombre",
    "Turno",
    "Codigo",
    "Asiste",
)

_COLUMNAS_SIN_MENSAJE: Sequence[str] = (
    "Fecha",
    "Nombre",
    "Turno",
    "Codigo",
)


_ventana: Optional[tk.Toplevel] = None
_db: Optional[firestore.Client] = None
_sa_path: Optional[str] = None
_project_id: Optional[str] = None
_date_widget: Optional[Any] = None
_date_var: Optional[tk.StringVar] = None
_tipo_var: Optional[tk.StringVar] = None
_tipo_combo: Optional[ttk.Combobox] = None
_btn_generar: Optional[ttk.Button] = None
_btn_probar_indice: Optional[ttk.Button] = None
_tree_llamados: Optional[ttk.Treeview] = None
_tree_sin_mensaje: Optional[ttk.Treeview] = None
_total_llamados_var: Optional[tk.StringVar] = None
_total_sin_msg_var: Optional[tk.StringVar] = None

_datos_llamados: List[Dict[str, Any]] = []
_datos_sin_mensaje: List[Dict[str, Any]] = []
_fecha_actual: Optional[date] = None


def _load_cfg() -> Dict[str, Any]:
    global _cfg_cache
    if _cfg_cache is not None:
        return _cfg_cache

    if not CONFIG_PATH.exists():
        logger.warning("Archivo de configuración no encontrado: %s", CONFIG_PATH)
        _cfg_cache = {}
        return _cfg_cache

    try:
        with CONFIG_PATH.open("r", encoding="utf-8") as fh:
            _cfg_cache = json.load(fh)
    except Exception:
        logger.exception("No se pudo leer la configuración desde %s", CONFIG_PATH)
        _cfg_cache = {}
    return _cfg_cache


def open_access_conn(db_path: str):
    """Abre una conexión a un archivo Access usando pyodbc."""

    if pyodbc is None:
        raise RuntimeError("pyodbc no está disponible. Instálalo para consultar Access.")

    ruta = (db_path or "").strip()
    if not ruta:
        raise RuntimeError("No se configuró la ruta de la base de datos de fichajes.")
    if not os.path.exists(ruta):
        raise FileNotFoundError(f"No se encontró la base de datos Access en {ruta}")

    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={ruta};"
    )
    return pyodbc.connect(conn_str)


def list_tables(conn: Optional[Any]) -> List[str]:
    """Devuelve la lista de tablas disponibles en la conexión Access."""

    if pyodbc is None or conn is None:
        return []

    tablas: List[str] = []
    cursor = None
    try:
        cursor = conn.cursor()
        for row in cursor.tables(tableType="TABLE"):
            nombre = getattr(row, "table_name", None)
            if nombre:
                tablas.append(str(nombre))
    except Exception:
        logger.exception("No se pudieron listar las tablas de la base de datos de fichajes")
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
    return tablas


def _abrir_conexion_fichajes() -> None:
    """Abre la conexión configurada para la base de datos de fichajes."""

    global _conn_fich, _conn_fich_path

    cfg = _load_cfg()
    ruta = str(cfg.get("access_fichajes_mdb") or "").strip()

    if pyodbc is None:
        logger.warning("pyodbc no está disponible; no se abrirá la base de datos de fichajes")
        _conn_fich = None
        _conn_fich_path = ruta or None
        return

    if _conn_fich is not None:
        try:
            _conn_fich.close()
        except Exception:
            pass
        _conn_fich = None

    if not ruta:
        logger.warning("Ruta de base de datos de fichajes no configurada en config.json")
        _conn_fich_path = None
        return

    try:
        _conn_fich = open_access_conn(ruta)
    except Exception:
        _conn_fich_path = ruta
        raise

    _conn_fich_path = ruta

    tablas = list_tables(_conn_fich)
    resumen = ", ".join(tablas[:30]) if tablas else "(sin tablas)"
    if len(tablas) > 30:
        resumen += ", ..."
    logger.info("Tablas disponibles en %s: %s", ruta, resumen)


def _cerrar_conexion_fichajes() -> None:
    global _conn_fich, _conn_fich_path
    if _conn_fich is not None:
        try:
            _conn_fich.close()
        except Exception:
            pass
    _conn_fich = None
    _conn_fich_path = None


def abrir_informe_asistencia(
    db: firestore.Client,
    sa_path: Optional[str] = None,
    project_id: Optional[str] = None,
) -> None:
    """Abre el informe de control de asistencia."""

    global _ventana, _db, _sa_path, _project_id

    if _ventana is not None and _ventana.winfo_exists():
        _ventana.lift()
        _ventana.focus_force()
        return

    _db = db
    _sa_path = sa_path
    _project_id = project_id

    _ventana = tk.Toplevel()
    _ventana.title("Informe - Control de asistencia")
    _ventana.geometry("1080x680")
    _ventana.minsize(960, 600)

    try:  # pragma: no cover - icono opcional
        _ventana.iconphoto(True, tk.PhotoImage(file="icono_app.png"))
    except Exception:
        pass

    _construir_ui(_ventana)
    _cargar_tipos_async()

    try:
        _abrir_conexion_fichajes()
    except Exception as exc:
        logger.exception("No se pudo abrir la base de datos de fichajes")
        messagebox.showerror(
            "Informe",
            f"No se pudo abrir la base de datos de fichajes:\n\n{exc}",
        )

    def _al_cerrar() -> None:
        global _ventana, _date_widget, _date_var, _tipo_var, _tipo_combo
        global _tree_llamados, _tree_sin_mensaje, _btn_generar, _btn_probar_indice
        global _datos_llamados, _datos_sin_mensaje, _fecha_actual
        global _total_llamados_var, _total_sin_msg_var

        if _ventana is not None:
            try:
                _ventana.destroy()
            except Exception:
                pass
        _ventana = None
        _date_widget = None
        _date_var = None
        _tipo_var = None
        _tipo_combo = None
        _tree_llamados = None
        _tree_sin_mensaje = None
        _btn_generar = None
        _btn_probar_indice = None
        _datos_llamados = []
        _datos_sin_mensaje = []
        _fecha_actual = None
        _total_llamados_var = None
        _total_sin_msg_var = None
        _cerrar_conexion_fichajes()

    _ventana.protocol("WM_DELETE_WINDOW", _al_cerrar)


def _construir_ui(root: tk.Toplevel) -> None:
    global _date_widget, _date_var, _tipo_var, _tipo_combo, _btn_generar
    global _btn_probar_indice
    global _tree_llamados, _tree_sin_mensaje
    global _total_llamados_var, _total_sin_msg_var

    root.grid_rowconfigure(1, weight=1)
    root.grid_columnconfigure(0, weight=1)

    filtros = ttk.Frame(root, padding=(18, 14))
    filtros.grid(row=0, column=0, sticky="ew")
    filtros.columnconfigure(5, weight=1)

    ttk.Label(filtros, text="Fecha:", font=("Segoe UI", 10, "bold")).grid(
        row=0, column=0, sticky="w"
    )

    if DateEntry is not None:
        selector = DateEntry(filtros, width=12, date_pattern="dd-mm-yyyy")
        selector.set_date(date.today())
        selector.grid(row=0, column=1, sticky="w", padx=(6, 16))
        _date_widget = selector
    else:
        _date_var = tk.StringVar(value=date.today().strftime("%d-%m-%Y"))
        entry = ttk.Entry(filtros, textvariable=_date_var, width=14)
        entry.grid(row=0, column=1, sticky="w", padx=(6, 16))
        _date_widget = entry

    ttk.Label(filtros, text="Tipo de mensaje:", font=("Segoe UI", 10, "bold")).grid(
        row=0, column=2, sticky="w"
    )

    _tipo_var = tk.StringVar()
    _tipo_combo = ttk.Combobox(filtros, textvariable=_tipo_var, state="readonly", width=30)
    _tipo_combo.grid(row=0, column=3, sticky="w", padx=(6, 16))

    _btn_generar = ttk.Button(filtros, text="Generar", command=_generar)
    _btn_generar.grid(row=0, column=4, sticky="w")

    _btn_probar_indice = ttk.Button(
        filtros,
        text="Probar índice",
        command=_probar_indice,
    )
    _btn_probar_indice.grid(row=0, column=5, sticky="w", padx=(6, 0))

    cuerpo = ttk.Frame(root, padding=(18, 0, 18, 18))
    cuerpo.grid(row=1, column=0, sticky="nsew")
    cuerpo.grid_rowconfigure(0, weight=1)
    cuerpo.grid_rowconfigure(1, weight=1)
    cuerpo.grid_columnconfigure(0, weight=1)

    frame_a = ttk.LabelFrame(cuerpo, text="Personas llamadas")
    frame_a.grid(row=0, column=0, sticky="nsew", pady=(0, 12))
    frame_a.grid_rowconfigure(0, weight=1)
    frame_a.grid_columnconfigure(0, weight=1)

    frame_b = ttk.LabelFrame(cuerpo, text="Asistieron sin mensaje")
    frame_b.grid(row=1, column=0, sticky="nsew")
    frame_b.grid_rowconfigure(0, weight=1)
    frame_b.grid_columnconfigure(0, weight=1)

    cont_a = ttk.Frame(frame_a)
    cont_a.grid(row=0, column=0, sticky="nsew")
    cont_a.grid_rowconfigure(0, weight=1)
    cont_a.grid_columnconfigure(0, weight=1)

    _tree_llamados = _crear_treeview(cont_a, _COLUMNAS_LLAMADOS)
    _tree_llamados.tag_configure("asiste_si", background="#e3f8e6")
    _tree_llamados.tag_configure("asiste_no", background="#fde4e4")

    global _total_llamados_var
    _total_llamados_var = tk.StringVar(value="Total personas llamadas: 0")
    ttk.Label(frame_a, textvariable=_total_llamados_var).grid(
        row=1, column=0, sticky="w", padx=0, pady=(6, 0)
    )

    botones_a = ttk.Frame(frame_a)
    botones_a.grid(row=2, column=0, sticky="ew", pady=(8, 0))
    botones_a.columnconfigure(0, weight=1)
    botones_a.columnconfigure(1, weight=1)

    ttk.Button(
        botones_a,
        text="Exportar CSV",
        command=lambda: _exportar_llamados("csv"),
    ).grid(row=0, column=0, sticky="ew", padx=(0, 6))

    ttk.Button(
        botones_a,
        text="Exportar Excel",
        command=lambda: _exportar_llamados("excel"),
    ).grid(row=0, column=1, sticky="ew", padx=(6, 0))

    cont_b = ttk.Frame(frame_b)
    cont_b.grid(row=0, column=0, sticky="nsew")
    cont_b.grid_rowconfigure(0, weight=1)
    cont_b.grid_columnconfigure(0, weight=1)

    _tree_sin_mensaje = _crear_treeview(cont_b, _COLUMNAS_SIN_MENSAJE)

    global _total_sin_msg_var
    _total_sin_msg_var = tk.StringVar(value="Total asistieron sin mensaje: 0")
    ttk.Label(frame_b, textvariable=_total_sin_msg_var).grid(
        row=1, column=0, sticky="w", padx=0, pady=(6, 0)
    )

    botones_b = ttk.Frame(frame_b)
    botones_b.grid(row=2, column=0, sticky="ew", pady=(8, 0))
    botones_b.columnconfigure(0, weight=1)
    botones_b.columnconfigure(1, weight=1)

    ttk.Button(
        botones_b,
        text="Exportar CSV",
        command=lambda: _exportar_sin_mensaje("csv"),
    ).grid(row=0, column=0, sticky="ew", padx=(0, 6))

    ttk.Button(
        botones_b,
        text="Exportar Excel",
        command=lambda: _exportar_sin_mensaje("excel"),
    ).grid(row=0, column=1, sticky="ew", padx=(6, 0))


def _crear_treeview(parent: tk.Misc, columnas: Sequence[str]) -> ttk.Treeview:
    tree = ttk.Treeview(parent, columns=columnas, show="headings", selectmode="browse")

    xscroll = ttk.Scrollbar(parent, orient="horizontal", command=tree.xview)
    yscroll = ttk.Scrollbar(parent, orient="vertical", command=tree.yview)
    tree.configure(xscrollcommand=xscroll.set, yscrollcommand=yscroll.set)

    tree.grid(row=0, column=0, sticky="nsew")
    yscroll.grid(row=0, column=1, sticky="ns")
    xscroll.grid(row=1, column=0, sticky="ew")

    for col in columnas:
        tree.heading(col, text=col)
        tree.column(col, anchor="center", width=_ancho_sugerido(col))

    _configurar_ordenacion_columnas(tree, columnas)
    return tree


def _ancho_sugerido(columna: str) -> int:
    sugeridos = {
        "Fecha": 100,
        "Hora": 100,
        "Mensaje": 200,
        "Estado Mensaje": 140,
        "Nombre": 200,
        "Turno": 120,
        "Codigo": 100,
        "Asiste": 80,
    }
    return sugeridos.get(columna, 140)


def _configurar_ordenacion_columnas(tree: ttk.Treeview, columnas: Sequence[str]) -> None:
    estados: Dict[str, Optional[str]] = {col: None for col in columnas}

    def ordenar(col: str) -> None:
        datos = [(tree.set(iid, col), iid) for iid in tree.get_children("")]
        reverse = estados[col] == "asc"

        def _clave(valor: Any) -> Tuple[int, Any]:
            if valor is None:
                return (4, "")
            texto = str(valor).strip()
            if not texto:
                return (4, "")
            try:
                return (0, float(texto.replace(",", ".")))
            except Exception:
                pass
            for fmt in ("%d-%m-%Y", "%d/%m/%Y"):
                try:
                    return (1, datetime.strptime(texto, fmt))
                except Exception:
                    pass
            for fmt in ("%H:%M", "%H:%M:%S"):
                try:
                    return (2, datetime.strptime(texto, fmt))
                except Exception:
                    pass
            return (3, texto.lower())

        datos.sort(key=lambda par: _clave(par[0]), reverse=reverse)
        for idx, (_, iid) in enumerate(datos):
            tree.move(iid, "", idx)

        estados[col] = "desc" if reverse else "asc"
        for columna in columnas:
            texto = columna
            if estados[columna] == "asc":
                texto += " ▲"
            elif estados[columna] == "desc":
                texto += " ▼"
            tree.heading(columna, text=texto, command=lambda c=columna: ordenar(c))

    for col in columnas:
        tree.heading(col, text=col, command=lambda c=col: ordenar(c))


def _cargar_tipos_async() -> None:
    if _db is None:
        return

    def _worker() -> None:
        try:
            tipos = cargar_tipos_produccion(_db)
        except Exception as exc:
            logger.exception("Error al cargar tipos de producción")
            _programar(lambda: messagebox.showerror("Informe", f"No se pudieron cargar los tipos: {exc}"))
            return

        def _aplicar() -> None:
            if _tipo_combo is None:
                return
            _tipo_combo["values"] = tipos
            if tipos:
                _tipo_var.set(tipos[0])

        _programar(_aplicar)

    run_bg(_worker, _thread_name="tipos_informe_asistencia")


def _programar(fn, *args, **kwargs) -> None:
    if _ventana is not None and _ventana.winfo_exists():
        _ventana.after(0, fn, *args, **kwargs)
    else:
        try:
            tk._default_root.after(0, fn, *args, **kwargs)  # type: ignore[attr-defined]
        except Exception:
            fn(*args, **kwargs)


def _es_error_tabla_inexistente(exc: Exception) -> bool:
    mensaje = str(exc).upper()
    if "FICHAJES001" not in mensaje:
        return False
    return (
        "CANNOT FIND" in mensaje
        or "COULD NOT FIND" in mensaje
        or "NO SE ENCUENTRA" in mensaje
        or "NO SE ENCONTR" in mensaje
    )


def _crear_error_tabla_inexistente(conn: Optional[Any]) -> TablaFichajesNoEncontradaError:
    tablas = list_tables(conn)[:30]
    tablas_texto = "\n".join(tablas) if tablas else "(sin tablas)"
    ruta = _conn_fich_path or "(ruta no configurada)"
    mensaje = (
        "No se encontró la tabla FICHAJES001 en la base de datos de fichajes.\n\n"
        f"Archivo: {ruta}\n\n"
        "Tablas disponibles (primeras 30):\n"
        f"{tablas_texto}"
    )

    def _mostrar() -> None:
        messagebox.showerror("Informe", mensaje)

    _programar(_mostrar)
    return TablaFichajesNoEncontradaError(mensaje)


def _log_firestore_context(db: Optional[firestore.Client]) -> None:
    """Loggea información básica del cliente de Firestore."""

    if db is None:
        logger.warning("Firestore client not provided for informe de asistencia")
        return

    project = getattr(db, "project", None)
    logger.info("Informe asistencia usando proyecto Firestore: %s", project)


def _extraer_url_indice(exc: Exception) -> Optional[str]:
    msg = str(exc)
    for token in msg.split():
        if token.startswith("https://") and "firestore/indexes?create_composite" in token:
            return token
    return None


def _mostrar_error(exc: Exception) -> None:
    logging.exception("Error generando informe de asistencia", exc_info=exc)
    if getattr(exc, "already_notified", False):
        return
    msg = str(exc)
    url = _extraer_url_indice(exc)
    if url:
        if messagebox.askyesno(
            "Informe",
            "La consulta requiere un índice en Firestore.\n¿Abrir página para crearlo?",
        ):
            webbrowser.open(url)
    else:
        messagebox.showerror("Informe", f"No se pudo generar el informe:\n\n{msg}")


def _generar() -> None:
    if _db is None:
        messagebox.showerror("Informe", "No hay conexión a Firestore.")
        return

    fecha = _obtener_fecha()
    if fecha is None:
        return

    tipo = (_tipo_var.get().strip() if _tipo_var else "")
    if not tipo:
        messagebox.showwarning("Informe", "Selecciona un tipo de mensaje.")
        return

    if _btn_generar is not None:
        _btn_generar.configure(state=tk.DISABLED)

    run_bg(
        lambda: _generar_bg(fecha, tipo),
        _thread_name="generar_informe_asistencia",
    )


def _probar_indice() -> None:
    if _db is None:
        messagebox.showerror("Informe", "No hay conexión a Firestore.")
        return

    fecha = _obtener_fecha()
    if fecha is None:
        return

    tipo = (_tipo_var.get().strip() if _tipo_var else "")
    if not tipo:
        messagebox.showwarning("Informe", "Selecciona un tipo de mensaje.")
        return

    if _btn_probar_indice is not None:
        _btn_probar_indice.configure(state=tk.DISABLED)

    def _worker() -> None:
        try:
            _log_firestore_context(_db)
            mensajes = get_mensajes(_db, fecha, tipo)

            def _ok() -> None:
                if _btn_probar_indice is not None:
                    _btn_probar_indice.configure(state=tk.NORMAL)
                messagebox.showinfo(
                    "Informe",
                    f"Consulta exitosa. Mensajes encontrados: {len(mensajes)}",
                )

            _programar(_ok)
        except Exception as exc:
            logger.exception("Error al probar el índice de Firestore", exc_info=exc)

            def _fail() -> None:
                if _btn_probar_indice is not None:
                    _btn_probar_indice.configure(state=tk.NORMAL)
                messagebox.showerror("Informe", f"Error al probar el índice:\n\n{exc}")

            _programar(_fail)

    run_bg(_worker, _thread_name="probar_indice_informe_asistencia")


def _obtener_fecha() -> Optional[date]:
    if DateEntry is not None and isinstance(_date_widget, DateEntry):
        try:
            return _date_widget.get_date()  # type: ignore[return-value]
        except Exception as exc:
            messagebox.showerror("Informe", f"Fecha inválida: {exc}")
            return None

    if _date_widget is None:
        messagebox.showerror("Informe", "Selector de fecha no inicializado.")
        return None

    texto = _date_widget.get().strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(texto, fmt).date()
        except Exception:
            continue
    messagebox.showerror("Informe", "Fecha inválida. Usa el formato DD-MM-YYYY.")
    return None


def _generar_bg(fecha: date, tipo: str) -> None:
    global _datos_llamados, _datos_sin_mensaje, _fecha_actual

    try:
        if pyodbc is None:
            raise RuntimeError("pyodbc no está disponible. Instálalo para consultar Access.")
        conn = _conn_fich
        if conn is None or getattr(conn, "closed", False):
            if _conn_fich_path:
                try:
                    _abrir_conexion_fichajes()
                    conn = _conn_fich
                except Exception as exc:
                    raise RuntimeError(
                        f"No se pudo abrir la base de datos de fichajes en {_conn_fich_path}: {exc}"
                    ) from exc
            else:
                raise RuntimeError("No se configuró la ruta de la base de datos de fichajes.")

        if conn is None:
            raise RuntimeError("No se pudo acceder a la base de datos de fichajes.")

        fecha_texto = fecha.strftime("%d-%m-%Y")
        fecha_yyyymmdd = fecha.strftime("%Y%m%d")

        _log_firestore_context(_db)
        mensajes = get_mensajes(_db, fecha, tipo)
        usuarios_map = get_usuarios_map(_db)
        usuarios_por_codigo = get_usuarios_por_codigo(usuarios_map)

        try:
            presentes = get_codigos_presentes(fecha_yyyymmdd, conn)
        except ProgrammingError as exc:
            if _es_error_tabla_inexistente(exc):
                raise _crear_error_tabla_inexistente(conn) from exc
            raise

        filas_llamados: List[Dict[str, Any]] = []

        for mensaje in mensajes:
            uid = _limpiar_str(mensaje.get("uid"))
            usuario = usuarios_map.get(uid) if uid else None
            codigo_usuario = _limpiar_str((usuario or {}).get("Codigo"))
            codigo_doc = _limpiar_str(mensaje.get("codigo"))
            codigo_valido = codigo_usuario or codigo_doc
            if codigo_valido and codigo_valido.upper() == "N/D":
                codigo_valido = None
            codigo_mostrar = codigo_valido or "N/D"

            nombre = _limpiar_str((usuario or {}).get("Nombre")) or "N/D"
            turno = _limpiar_str((usuario or {}).get("Turno")) or "N/D"

            fecha_mensaje = _limpiar_str(mensaje.get("dia")) or "N/D"
            hora_mensaje = _limpiar_str(mensaje.get("hora")) or "N/D"

            mensaje_tipo = _limpiar_str(mensaje.get("mensaje")) or tipo
            estado = _limpiar_str(mensaje.get("estado")) or "N/D"

            presente = False
            if codigo_valido:
                try:
                    presente = asiste(codigo_valido, fecha_yyyymmdd, conn)
                except ProgrammingError as exc:
                    if _es_error_tabla_inexistente(exc):
                        raise _crear_error_tabla_inexistente(conn) from exc
                    raise
            asiste_texto = "SI" if presente else "NO"

            fila = {
                "Fecha": fecha_mensaje,
                "Hora": hora_mensaje,
                "Mensaje": mensaje_tipo,
                "Estado Mensaje": estado,
                "Nombre": nombre,
                "Turno": turno,
                "Codigo": codigo_mostrar,
                "Asiste": asiste_texto,
            }
            filas_llamados.append(fila)

        codigos_con_mensaje = {
            codigo
            for codigo in (
                str(fila.get("Codigo") or "").strip() for fila in filas_llamados
            )
            if codigo and codigo.upper() != "N/D"
        }

        presentes_codigos = {
            idempleado_a_codigo(str(idemp)) for idemp in presentes
        }
        sin_mensaje_codigos = {
            codigo for codigo in presentes_codigos if codigo and codigo not in codigos_con_mensaje
        }

        filas_sin_mensaje: List[Dict[str, Any]] = []
        for codigo in sorted(sin_mensaje_codigos):
            usuario = usuarios_por_codigo.get(codigo)
            nombre = _limpiar_str((usuario or {}).get("Nombre")) or "N/D"
            turno = _limpiar_str((usuario or {}).get("Turno")) or "N/D"
            fila = {
                "Fecha": fecha_texto,
                "Nombre": nombre,
                "Turno": turno,
                "Codigo": codigo,
            }
            filas_sin_mensaje.append(fila)

        logger.info(
            "Informe asistencia generado fecha=%s tipo=%s llamados=%s sin_mensaje=%s",
            fecha_texto,
            tipo,
            len(filas_llamados),
            len(filas_sin_mensaje),
        )

        def _aplicar() -> None:
            global _datos_llamados, _datos_sin_mensaje, _fecha_actual
            _datos_llamados = filas_llamados
            _datos_sin_mensaje = filas_sin_mensaje
            _fecha_actual = fecha
            _actualizar_treeviews()
            if _btn_generar is not None:
                _btn_generar.configure(state=tk.NORMAL)

        _programar(_aplicar)

    except Exception as exc:
        _programar(_notificar, exc)


def _notificar(exc: Exception) -> None:
    if _btn_generar is not None:
        _btn_generar.configure(state=tk.NORMAL)
    _mostrar_error(exc)


def _actualizar_treeviews() -> None:
    if _tree_llamados is not None:
        _tree_llamados.delete(*_tree_llamados.get_children(""))
        for fila in _datos_llamados:
            tags = ("asiste_si",) if fila.get("Asiste") == "SI" else ("asiste_no",)
            _tree_llamados.insert("", "end", values=[fila.get(c, "") for c in _COLUMNAS_LLAMADOS], tags=tags)
        if _total_llamados_var is not None:
            _total_llamados_var.set(
                f"Total personas llamadas: {len(_datos_llamados)}"
            )

    if _tree_sin_mensaje is not None:
        _tree_sin_mensaje.delete(*_tree_sin_mensaje.get_children(""))
        for fila in _datos_sin_mensaje:
            _tree_sin_mensaje.insert("", "end", values=[fila.get(c, "") for c in _COLUMNAS_SIN_MENSAJE])
        if _total_sin_msg_var is not None:
            _total_sin_msg_var.set(
                f"Total asistieron sin mensaje: {len(_datos_sin_mensaje)}"
            )


def cargar_tipos_produccion(db: firestore.Client) -> List[str]:
    candidatos = [
        ("PlantillasMensaje", "Producción"),
        ("PlantillasMensaje", "Produccion"),
    ]

    mensajes: List[str] = []
    for coleccion, documento in candidatos:
        try:
            doc = db.collection(coleccion).document(documento).get()
        except Exception:
            continue
        if doc.exists:
            datos = doc.to_dict() or {}
            texto = datos.get("Mensaje") or ""
            if isinstance(texto, str) and texto.strip():
                mensajes.extend(texto.split(","))
                break
            # fallback: si el doc contiene subdocumento "Producción"
            produccion = datos.get("Producción")
            if isinstance(produccion, dict) and isinstance(produccion.get("Mensaje"), str):
                mensajes.extend(produccion["Mensaje"].split(","))
                break

    if not mensajes:
        rutas_extra = [
            ("PlantillasMensaje", "Producción", "Producción"),
            ("PlantillasMensaje", "Produccion", "Produccion"),
        ]
        for coll, doc_id, inner in rutas_extra:
            try:
                doc = db.collection(coll).document(doc_id).collection(inner).document(inner).get()
            except Exception:
                continue
            if doc.exists:
                datos = doc.to_dict() or {}
                texto = datos.get("Mensaje")
                if isinstance(texto, str) and texto.strip():
                    mensajes.extend(texto.split(","))
                    break

    if not mensajes:
        raise RuntimeError("No se encontraron tipos en PlantillasMensaje/Producción.")

    tipos = sorted({item.strip() for item in mensajes if item.strip()})
    if not tipos:
        raise RuntimeError("El documento de plantillas no contiene tipos válidos.")
    return tipos


def get_mensajes(
    db: firestore.Client,
    fecha_sel: date,
    tipo: str,
) -> List[Dict[str, Any]]:
    if db is None:
        return []

    coleccion = db.collection("Mensajes")
    dia_str = fecha_sel.strftime("%Y-%m-%d")
    consulta = (
        coleccion.where("mensaje", "==", tipo).where("dia", "==", dia_str)
    )
    try:
        documentos = list(consulta.stream())
    except FailedPrecondition as exc:
        url = _extraer_url_indice(exc)
        if url:
            try:
                webbrowser.open(url)
            except Exception:
                logger.exception("No se pudo abrir el navegador para el índice requerido")
        raise
    resultados: List[Dict[str, Any]] = []
    for doc in documentos:
        datos = doc.to_dict() or {}
        datos.setdefault("doc_id", doc.id)
        resultados.append(datos)
    return resultados


def get_usuarios_map(db: firestore.Client) -> Dict[str, Dict[str, Any]]:
    if db is None:
        return {}

    resultado: Dict[str, Dict[str, Any]] = {}
    for doc in db.collection("UsuariosAutorizados").stream():
        datos = doc.to_dict() or {}
        datos.setdefault("uid", doc.id)
        resultado[doc.id] = datos
    return resultado


def get_usuarios_por_codigo(
    usuarios: Dict[str, Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    indice: Dict[str, Dict[str, Any]] = {}
    for datos in usuarios.values():
        codigo = _limpiar_str(datos.get("Codigo"))
        if codigo:
            indice[codigo] = datos
    return indice


def get_codigos_presentes(fecha: str, conn: Optional[Any]) -> set[str]:
    if pyodbc is None or conn is None:
        return set()

    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT [IdEmpleado] FROM [FICHAJES001] WHERE [Fecha] = ?",
            fecha,
        )
        filas = cursor.fetchall()
    except ProgrammingError:
        raise
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass

    presentes: set[str] = set()
    for fila in filas:
        valor = fila[0]
        if valor is not None:
            codigo = _limpiar_str(valor)
            if codigo:
                presentes.add(codigo)
    return presentes


def idempleado_a_codigo(idemp: str) -> str:
    idemp = (idemp or "").strip()
    if not idemp:
        return ""
    try:
        return str(int(idemp))
    except Exception:
        return idemp


def asiste(codigo: str, fecha: str, conn: Optional[Any]) -> bool:
    if pyodbc is None or conn is None:
        return False

    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT TOP 1 1 FROM [FICHAJES001] WHERE [Fecha] = ? AND [IdEmpleado] = ?",
            fecha,
            codigo,
        )
        return cursor.fetchone() is not None
    except ProgrammingError:
        raise
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass


def exportar_treeview_csv(columnas: Sequence[str], datos: Iterable[Dict[str, Any]], ruta: str) -> None:
    with open(ruta, "w", newline="", encoding="utf-8-sig") as archivo:
        escritor = csv.writer(archivo)
        escritor.writerow(columnas)
        for fila in datos:
            escritor.writerow([fila.get(col, "") for col in columnas])


def exportar_treeview_excel(
    columnas: Sequence[str], datos: Iterable[Dict[str, Any]], ruta: str
) -> None:
    if pd is None:
        raise RuntimeError("pandas no está disponible")
    df = pd.DataFrame([{col: fila.get(col, "") for col in columnas} for fila in datos])
    df.to_excel(ruta, index=False)


def _exportar_llamados(formato: str) -> None:
    if not _datos_llamados:
        messagebox.showinfo("Informe", "No hay datos para exportar.")
        return

    fecha_txt = _fecha_actual.strftime("%Y%m%d") if _fecha_actual else "informe"
    if formato == "csv":
        ruta = filedialog.asksaveasfilename(
            title="Exportar CSV",
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            initialfile=f"InformeAsistencia_llamados_{fecha_txt}.csv",
        )
        if not ruta:
            return
        exportar_treeview_csv(_COLUMNAS_LLAMADOS, _datos_llamados, ruta)
        messagebox.showinfo("Informe", "Datos exportados correctamente.")
        return

    ruta = filedialog.asksaveasfilename(
        title="Exportar Excel",
        defaultextension=".xlsx",
        filetypes=[("Excel", "*.xlsx"), ("CSV", "*.csv")],
        initialfile=f"InformeAsistencia_llamados_{fecha_txt}.xlsx",
    )
    if not ruta:
        return

    try:
        exportar_treeview_excel(_COLUMNAS_LLAMADOS, _datos_llamados, ruta)
    except Exception as exc:
        logger.warning("Fallo exportando a Excel, se intentará CSV", exc_info=True)
        if ruta.lower().endswith(".xlsx"):
            ruta_csv = os.path.splitext(ruta)[0] + ".csv"
        else:
            ruta_csv = ruta
        exportar_treeview_csv(_COLUMNAS_LLAMADOS, _datos_llamados, ruta_csv)
        messagebox.showwarning(
            "Informe",
            f"No fue posible exportar a Excel ({exc}). Se generó un CSV en su lugar.",
        )
    else:
        messagebox.showinfo("Informe", "Datos exportados correctamente.")


def _exportar_sin_mensaje(formato: str) -> None:
    if not _datos_sin_mensaje:
        messagebox.showinfo("Informe", "No hay datos para exportar.")
        return

    fecha_txt = _fecha_actual.strftime("%Y%m%d") if _fecha_actual else "informe"
    if formato == "csv":
        ruta = filedialog.asksaveasfilename(
            title="Exportar CSV",
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            initialfile=f"InformeAsistencia_sin_mensaje_{fecha_txt}.csv",
        )
        if not ruta:
            return
        exportar_treeview_csv(_COLUMNAS_SIN_MENSAJE, _datos_sin_mensaje, ruta)
        messagebox.showinfo("Informe", "Datos exportados correctamente.")
        return

    ruta = filedialog.asksaveasfilename(
        title="Exportar Excel",
        defaultextension=".xlsx",
        filetypes=[("Excel", "*.xlsx"), ("CSV", "*.csv")],
        initialfile=f"InformeAsistencia_sin_mensaje_{fecha_txt}.xlsx",
    )
    if not ruta:
        return

    try:
        exportar_treeview_excel(_COLUMNAS_SIN_MENSAJE, _datos_sin_mensaje, ruta)
    except Exception as exc:
        logger.warning("Fallo exportando Excel, generando CSV", exc_info=True)
        if ruta.lower().endswith(".xlsx"):
            ruta_csv = os.path.splitext(ruta)[0] + ".csv"
        else:
            ruta_csv = ruta
        exportar_treeview_csv(_COLUMNAS_SIN_MENSAJE, _datos_sin_mensaje, ruta_csv)
        messagebox.showwarning(
            "Informe",
            f"No fue posible exportar a Excel ({exc}). Se generó un CSV en su lugar.",
        )
    else:
        messagebox.showinfo("Informe", "Datos exportados correctamente.")


def _limpiar_str(valor: Any) -> Optional[str]:
    if valor is None:
        return None
    if isinstance(valor, str):
        texto = valor.strip()
        return texto if texto else None
    texto = str(valor).strip()
    return texto if texto else None

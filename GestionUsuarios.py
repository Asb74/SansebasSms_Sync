import tkinter as tk
from tkinter import ttk, messagebox
from tkcalendar import Calendar
import pyodbc
import os
from firebase_admin import auth, firestore
import datetime as dt
from datetime import date, timedelta, timezone
import re
from decimal import Decimal
from typing import Optional, Union, Dict, Iterable, Tuple, List
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import time


DATE_RE = re.compile(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})")


def start_of_day_local_to_utc(d: date):
    local_tz = dt.datetime.now().astimezone().tzinfo
    local = dt.datetime(d.year, d.month, d.day, tzinfo=local_tz)
    return local.astimezone(timezone.utc)


def end_of_day_local_to_utc(d: date):
    local_tz = dt.datetime.now().astimezone().tzinfo
    local = dt.datetime(d.year, d.month, d.day, 23, 59, 59, 999000, tzinfo=local_tz)
    return local.astimezone(timezone.utc)


def _timestamp_to_local_date(value):
    if value is None:
        return None
    # Firestore Timestamp → datetime
    if hasattr(value, "to_datetime"):
        try:
            value = value.to_datetime()
        except Exception:
            return None
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.datetime.now().astimezone().tzinfo)
        return value.astimezone().date()
    if isinstance(value, date):
        return value
    return None


def _is_ok(v: str | None) -> bool:
    return (v or "").strip().lower() == "ok"


def parse_baja_texto_largo(raw: str) -> date | None:
    """Devuelve la **última** fecha válida encontrada en el texto (dd/mm/yy|yyyy o dd-mm-yy|yyyy)."""
    if not raw:
        return None
    s = str(raw).strip()
    matches = DATE_RE.findall(s)
    if not matches:
        return None
    for d, m, y in reversed(matches):
        try:
            d, m = int(d), int(m)
            y = int(y)
            if y < 100:
                y = 2000 + y if y <= 50 else 1900 + y
            return date(y, m, d)
        except ValueError:
            continue
    return None


def parse_access_date(raw) -> date | None:
    """Convierte valores Access (datetime, str, número OLE) a date."""
    if raw is None:
        return None
    if isinstance(raw, dt.datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    if hasattr(raw, "date"):
        try:
            return raw.date()
        except Exception:
            pass
    if isinstance(raw, (int, float)):
        return date(1899, 12, 30) + timedelta(days=float(raw))
    if isinstance(raw, str):
        for fmt in ("%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y", "%d-%m-%y"):
            try:
                return dt.datetime.strptime(raw.strip(), fmt).date()
            except Exception:
                pass
        return parse_baja_texto_largo(raw)
    return None


# Ventana principal de gestión de usuarios (singleton)
ventana_usuarios = None

# Funciones auxiliares de normalización
def s(x):
    return "" if x is None else str(x)


def s_trim(x):
    return s(x).strip()


def safe_re_sub(pattern, repl, value):
    return re.sub(pattern, repl, s(value))

def normalizar_dni(dni):
    return safe_re_sub(r"[^0-9A-Za-z]", "", dni).upper()


def map_genero(sexo_raw: str | None) -> str:
    s_val = (sexo_raw or "").strip().upper()
    if s_val == "H":
        return "Hombre"
    if s_val == "M":
        return "Mujer"
    return "Otro"


def _normalizar_genero_existente(valor: Optional[str]) -> Optional[str]:
    texto = s_trim(valor)
    if not texto:
        return None
    minus = texto.lower()
    if minus in ("h", "hombre"):
        return "Hombre"
    if minus in ("m", "mujer"):
        return "Mujer"
    return texto


def to_date(x):
    if x is None or x == "":
        return None
    parsed = parse_access_date(x)
    if parsed:
        return parsed
    try:
        if hasattr(x, "date"):
            return x.date()
    except Exception:
        return None
    if isinstance(x, dt.date):
        return x
    if isinstance(x, dt.datetime):
        return x.date()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return dt.datetime.strptime(s(x), fmt).date()
        except ValueError:
            pass
    return None


def fmt_dmy(d):
    return d.strftime("%d-%m-%Y") if d else None


def _date_to_str_ddmmyyyy(d: Optional[date]) -> Optional[str]:
    return fmt_dmy(d)


def _normalize_optional_str(value: Optional[Union[str, date]]) -> Optional[str]:
    if isinstance(value, date):
        return _date_to_str_ddmmyyyy(value)
    if value is None:
        return None
    texto = str(value).strip()
    return texto or None


def _to_int_safe(value: Union[str, int, float, Decimal, None], default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int,)):
        return int(value)
    try:
        if isinstance(value, (float, Decimal)):
            return int(value)
        texto = str(value).strip()
        if not texto:
            return default
        return int(float(texto.replace(',', '.')))
    except (ValueError, TypeError):
        return default


def _to_float_safe(value: Union[str, int, float, Decimal, None], default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        texto = str(value).strip().replace(',', '.')
        if not texto:
            return default
        return float(texto)
    except (ValueError, TypeError):
        return default


def _chunk_iterable(iterable: Iterable, size: int):
    """Yield successive chunks of given size from iterable."""
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def cargar_trabajadores(dnis: Iterable[str]) -> Dict[str, Dict[str, Optional[Union[str, date]]]]:
    """Lectura única de TRABAJADORES, retornando dict por DNI."""
    ruta = r'X:\ENLACES\Power BI\Campaña\PercecoBi(Campaña).mdb'
    resultado: Dict[str, Dict[str, Optional[Union[str, date]]]] = {}
    if not os.path.exists(ruta):
        print("❌ Ruta MDB no encontrada.")
        return resultado

    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={ruta};'
    )
    columnas = "DNI, CODIGO, FECHAALTA, FECHABAJA, SEXO, APELLIDOS, APELLIDOS2, NOMBRE"
    try:
        conn = pyodbc.connect(str(conn_str))
        cursor = conn.cursor()
        for bloque in _chunk_iterable(list(dnis), 1000):
            placeholders = ','.join('?' for _ in bloque)
            query = f"SELECT {columnas} FROM TRABAJADORES WHERE DNI IN ({placeholders})"
            cursor.execute(query, bloque)
            for row in cursor.fetchall():
                dni = normalizar_dni(getattr(row, 'DNI', None))
                if not dni:
                    continue
                alta_dt = to_date(getattr(row, 'FECHAALTA', None))
                baja_dt = to_date(getattr(row, 'FECHABAJA', None))
                ap1 = s_trim(getattr(row, 'APELLIDOS', None))
                ap2 = s_trim(getattr(row, 'APELLIDOS2', None))
                nom = s_trim(getattr(row, 'NOMBRE', None))
                nombre_compuesto = ' '.join([t for t in (ap1, ap2, nom) if t]).strip() or 'Falta'
                resultado[dni] = {
                    'Nombre': nombre_compuesto,
                    'Alta': _date_to_str_ddmmyyyy(alta_dt),
                    'Baja': _date_to_str_ddmmyyyy(baja_dt),
                    'Codigo': s_trim(getattr(row, 'CODIGO', None)),
                    'AltaDate': alta_dt,
                    'BajaDate': baja_dt,
                    'Genero': map_genero(getattr(row, 'SEXO', None)),
                }
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error cargando TRABAJADORES: {e}")
    return resultado


def cargar_datos_ajustados(
    dnis: Iterable[str],
    min_alta: date,
    altas_por_dni: Optional[Dict[str, date]] = None,
) -> Dict[str, Dict[str, Union[date, int, float, str, None]]]:
    """Lectura única de DATOS_AJUSTADOS con agregados por DNI."""
    ruta = r'X:\\ENLACES\\Power BI\\Campaña\\PercecoBi(Campaña).mdb'
    datos = defaultdict(lambda: {
        'UltimoDia': None,
        '_fechas': set(),
        'TotalHoras': 0.0,
        'Puesto': None,
    })
    if not os.path.exists(ruta):
        return datos

    altas_filtradas: Dict[str, date] = {}
    if altas_por_dni:
        for dni_key, alta_valor in altas_por_dni.items():
            alta_dt = to_date(alta_valor)
            if alta_dt:
                altas_filtradas[normalizar_dni(dni_key)] = alta_dt

    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={ruta};'
    )
    try:
        conn = pyodbc.connect(str(conn_str))
        cursor = conn.cursor()
        for bloque in _chunk_iterable(list(dnis), 1000):
            placeholders = ','.join('?' for _ in bloque)
            params = [min_alta] + list(bloque)
            query = (
                f"SELECT DNI, FECHA, HORAS, HORASEXT, CATEGORIA FROM DATOS_AJUSTADOS "
                f"WHERE FECHA >= ? AND DNI IN ({placeholders})"
            )
            cursor.execute(query, params)
            for row in cursor.fetchall():
                dni = normalizar_dni(getattr(row, 'DNI', None))
                if not dni:
                    continue
                fecha = to_date(getattr(row, 'FECHA', None))
                alta_referencia = altas_filtradas.get(dni)
                if alta_referencia and fecha and fecha < alta_referencia:
                    continue
                horas = float(s(getattr(row, 'HORAS', 0)) or 0) + float(s(getattr(row, 'HORASEXT', 0)) or 0)
                categoria = s_trim(getattr(row, 'CATEGORIA', None))
                info = datos[dni]
                if fecha:
                    info['_fechas'].add(fecha)
                    if not info['UltimoDia'] or fecha > info['UltimoDia']:
                        info['UltimoDia'] = fecha
                info['TotalHoras'] += horas
                if categoria:
                    info['Puesto'] = categoria
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error cargando DATOS_AJUSTADOS: {e}")

    final = {}
    for dni, info in datos.items():
        final[dni] = {
            'UltimoDia': info['UltimoDia'],
            'TotalDia': len(info['_fechas']),
            'TotalHoras': round(info['TotalHoras'], 2),
            'Puesto': info['Puesto'],
        }
    return final


def calcular_baja_y_totales(
    cursor,
    dni: str
) -> Tuple[Optional[str], int, float, Optional[date], Optional[date]]:
    fecha_alta: Optional[date] = None
    fecha_baja: Optional[date] = None
    baja_str: Optional[str] = None
    dni_param = (dni or "").strip().upper()

    if not dni_param:
        return baja_str, 0, 0.0, fecha_alta, fecha_baja

    row = cursor.execute(
        "SELECT FECHAALTA, FECHABAJA, SEXO FROM TRABAJADORES WHERE Trim(UCase(DNI)) = ?",
        (dni_param,)
    ).fetchone()

    raw_baja = getattr(row, 'FECHABAJA', None) if row else None
    fecha_alta = parse_access_date(getattr(row, 'FECHAALTA', None)) if row else None
    fecha_baja = parse_access_date(raw_baja) if row else None
    baja_str = fmt_dmy(fecha_baja)
    print(f"[TRAB] DNI={dni_param} FECHABAJA_raw={raw_baja!r} -> baja={baja_str}")

    if fecha_baja:
        return baja_str, 0, 0.0, fecha_alta, fecha_baja

    if not fecha_alta:
        return baja_str, 0, 0.0, fecha_alta, fecha_baja

    filas = cursor.execute(
        "SELECT FECHA, HORAS, HORASEXT FROM DATOS_AJUSTADOS WHERE Trim(UCase(DNI)) = ? AND FECHA >= ?",
        (dni_param, fecha_alta)
    ).fetchall()

    if not filas:
        return baja_str, 0, 0.0, fecha_alta, fecha_baja

    fechas = set()
    horas_acum = 0.0
    for fila in filas:
        fecha_reg = parse_access_date(getattr(fila, 'FECHA', None))
        if fecha_reg:
            fechas.add(fecha_reg)
        horas_acum += _to_float_safe(getattr(fila, 'HORAS', 0))
        horas_acum += _to_float_safe(getattr(fila, 'HORASEXT', 0))

    total_dia = len(fechas)
    total_horas = round(horas_acum, 2)

    return baja_str, total_dia, total_horas, fecha_alta, fecha_baja


def calcular_totales_y_baja(dni: str) -> Dict[str, Union[int, float, Optional[date], Optional[str]]]:
    resultado: Dict[str, Union[int, float, Optional[date], Optional[str]]] = {
        'total_dia': 0,
        'total_horas': 0.0,
        'fecha_alta': None,
        'fecha_baja': None,
        'baja_str': None,
    }
    dni_normalizado = normalizar_dni(dni)
    if not dni_normalizado:
        return resultado

    ruta = r'X:\ENLACES\Power BI\Campaña\PercecoBi(Campaña).mdb'
    if not os.path.exists(ruta):
        return resultado

    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={ruta};'
    )

    conn = None
    cursor = None
    try:
        conn = pyodbc.connect(str(conn_str))
        cursor = conn.cursor()
        baja_str, total_dia, total_horas, fecha_alta, fecha_baja = calcular_baja_y_totales(cursor, dni_normalizado)
        resultado['baja_str'] = baja_str
        resultado['total_dia'] = int(total_dia)
        resultado['total_horas'] = round(float(total_horas), 2)
        resultado['fecha_alta'] = fecha_alta
        resultado['fecha_baja'] = fecha_baja
    except Exception as e:
        print(f"⚠️ Error calculando totales para {dni_normalizado}: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    return resultado


def migrar_cultivo_a_genero(db, cursor) -> None:
    """Migra documentos antiguos usando el campo Cultivo hacia Género."""
    if db is None:
        print("⚠️ Base de datos Firestore no disponible para migrar Género.")
        return

    try:
        usuarios_docs = list(db.collection("UsuariosAutorizados").stream())
    except Exception as e:
        print(f"❌ No se pudieron obtener usuarios para migrar Género: {e}")
        return

    total = len(usuarios_docs)
    print(f"🔁 Iniciando migración de Género para {total} usuarios...")

    for idx, doc in enumerate(usuarios_docs, start=1):
        data = doc.to_dict() or {}
        uid = doc.id
        dni_raw = data.get("Dni")
        genero_valor: Optional[str] = None

        dni_param = (dni_raw or "").strip().upper()
        if cursor and dni_param:
            try:
                row = cursor.execute(
                    "SELECT FECHAALTA, FECHABAJA, SEXO FROM TRABAJADORES WHERE Trim(UCase(DNI)) = ?",
                    (dni_param,)
                ).fetchone()
                sexo = getattr(row, "SEXO", None) if row else None
                genero_valor = map_genero(sexo)
            except Exception as exc:
                print(f"⚠️ No se pudo obtener SEXO para {dni_param}: {exc}")

        if not genero_valor:
            cultivo = data.get("Cultivo")
            genero_valor = _normalizar_genero_existente(cultivo)
            if not genero_valor:
                genero_existente = _normalizar_genero_existente(data.get("Genero"))
                genero_valor = genero_existente or "Otro"

        try:
            doc_ref = db.collection("UsuariosAutorizados").document(uid)
            doc_ref.update({"Genero": genero_valor})
            if "Cultivo" in data:
                try:
                    doc_ref.update({"Cultivo": firestore.DELETE_FIELD})
                except Exception as exc:
                    print(f"⚠️ No se pudo eliminar Cultivo de {uid}: {exc}")
        except Exception as exc:
            print(f"❌ Error migrando Género de {uid}: {exc}")

        if idx % 100 == 0:
            print(f"   ↪ Migrados {idx}/{total}")

    print("✅ Migración de Género finalizada.")


def abrir_gestion_usuarios(db):
    """Abre la ventana de gestión de usuarios evitando duplicados."""
    global ventana_usuarios
    if ventana_usuarios and ventana_usuarios.winfo_exists():
        ventana_usuarios.lift()
        ventana_usuarios.focus_force()
        return

    ventana_usuarios = tk.Toplevel()
    ventana = ventana_usuarios
    ventana.title("👥 Gestión de Usuarios")
    ventana.geometry("1400x600")

    def on_close():
        global ventana_usuarios
        ventana_usuarios = None
        ventana.destroy()

    ventana.protocol("WM_DELETE_WINDOW", on_close)

    columnas = ["Dni", "Nombre", "Telefono", "correo", "Puesto", "Turno", "Genero",
                "Mensaje", "Seleccionable", "Valor", "Alta", "UltimoDia", "TotalDia", "TotalHoras", "Baja", "Codigo"]

    encabezados = {
        "Dni": "Dni", "Nombre": "Nombre", "Telefono": "Teléfono", "correo": "Correo",
        "Puesto": "Puesto", "Turno": "Turno", "Genero": "Género",
        "Mensaje": "Mensaje", "Seleccionable": "Seleccionable", "Valor": "Valor",
        "Alta": "Alta", "UltimoDia": "Último Día", "TotalDia": "Total Día",
        "TotalHoras": "Total Horas", "Baja": "Baja", "Codigo": "Código"
    }

    datos_originales = []
    entradas_filtro = {}
    rows_by_iid: Dict[str, Dict[str, str]] = {}
    upcoming_by_uid: Dict[str, List[date]] = defaultdict(list)
    cal_popup: Optional[tk.Toplevel] = None
    cal_uid: Optional[str] = None

    ventana.grid_rowconfigure(2, weight=1)
    ventana.grid_columnconfigure(0, weight=1)

    frame_filtros = tk.Frame(ventana)
    frame_filtros.grid(row=0, column=0, sticky="ew")

    frame_labels = tk.Frame(frame_filtros)
    frame_labels.pack(fill="x")

    frame_entries = tk.Frame(frame_filtros)
    frame_entries.pack(fill="x")

    for idx, col in enumerate(columnas):
        head = encabezados.get(col, col)
        tk.Label(frame_labels, text=head).grid(row=0, column=idx, sticky="ew")
        entry = tk.Entry(frame_entries)
        entry.grid(row=0, column=idx, sticky="ew")
        entradas_filtro[col] = entry
        frame_labels.grid_columnconfigure(idx, weight=1)
        frame_entries.grid_columnconfigure(idx, weight=1)

    frame_botones = tk.Frame(ventana)
    frame_botones.grid(row=1, column=0, sticky="ew", pady=5)

    tabla_frame = tk.Frame(ventana)
    tabla_frame.grid(row=2, column=0, sticky="nsew")
    tabla_frame.grid_rowconfigure(0, weight=1)
    tabla_frame.grid_columnconfigure(0, weight=1)

    tree = ttk.Treeview(tabla_frame, columns=columnas, show="headings", selectmode="extended")
    tree.grid(row=0, column=0, sticky="nsew")
    orden_actual = {col: None for col in columnas}

    scrollbar_y = ttk.Scrollbar(tabla_frame, orient="vertical", command=tree.yview)
    scrollbar_y.grid(row=0, column=1, sticky="ns")

    scrollbar_x = ttk.Scrollbar(tabla_frame, orient="horizontal", command=tree.xview)
    scrollbar_x.grid(row=1, column=0, sticky="ew")

    tree.configure(yscrollcommand=scrollbar_y.set, xscrollcommand=scrollbar_x.set)
    tree.tag_configure("has_days_row", foreground="#b00020")

    frame_status = ttk.Frame(ventana)
    frame_status.grid(row=3, column=0, sticky="ew")
    frame_status.grid_columnconfigure(0, weight=1)
    contador_var = tk.StringVar(value="Seleccionados para Mensaje: 0")
    ultima_act_var = tk.StringVar(value="")
    ttk.Label(frame_status, textvariable=ultima_act_var).grid(row=0, column=0, sticky="w", padx=10)
    ttk.Label(frame_status, textvariable=contador_var).grid(row=0, column=1, sticky="e", padx=10, pady=5)

    COL_INDEX = {name: i for i, name in enumerate(tree["columns"])}
    nombre_col_index = COL_INDEX.get("Nombre", 1)
    nombre_col_id = f"#{nombre_col_index + 1}"

    def formatear_nombre(uid: str, nombre: str | None) -> str:
        base = (nombre or "Falta")
        return ("🔴 " if uid in upcoming_by_uid else "") + base

    def row_to_values(row: dict) -> list[str]:
        valores: list[str] = []
        for col in columnas:
            valor = row.get(col, "")
            if col == "Nombre":
                valor = formatear_nombre(row.get("UID", ""), valor)
            valores.append(valor)
        return valores

    def _hide_cal_popup():
        nonlocal cal_popup, cal_uid
        if cal_popup and cal_popup.winfo_exists():
            cal_popup.destroy()
        cal_popup = None
        cal_uid = None

    def _show_cal_for(uid: str):
        nonlocal cal_popup, cal_uid
        fechas = sorted(upcoming_by_uid.get(uid, []))
        if not fechas:
            _hide_cal_popup()
            return
        x = tree.winfo_pointerx() + 10
        y = tree.winfo_pointery() + 10
        _hide_cal_popup()
        cal_popup = tk.Toplevel(ventana)
        cal_popup.overrideredirect(True)
        try:
            cal_popup.attributes("-topmost", True)
        except Exception:
            pass
        cal_popup.geometry(f"+{x}+{y}")

        primera = fechas[0]
        calendario = Calendar(
            cal_popup,
            selectmode="none",
            year=primera.year,
            month=primera.month,
            day=primera.day,
        )
        calendario.pack()
        calendario.tag_config("libre", background="#b00020", foreground="white")
        for dia in fechas:
            calendario.calevent_create(dia, "Día libre", "libre")

        def _cerrar(_event=None):
            _hide_cal_popup()

        cal_popup.bind("<Leave>", _cerrar)
        cal_popup.bind("<FocusOut>", _cerrar)
        cal_uid = uid

    def _hover_calendar(event):
        if not upcoming_by_uid:
            return
        region = tree.identify("region", event.x, event.y)
        if region != "cell":
            _hide_cal_popup()
            return
        row_id = tree.identify_row(event.y)
        col_id = tree.identify_column(event.x)
        if not row_id or col_id != nombre_col_id:
            _hide_cal_popup()
            return
        if row_id not in upcoming_by_uid:
            _hide_cal_popup()
            return
        if cal_uid == row_id and cal_popup and cal_popup.winfo_exists():
            return
        _show_cal_for(row_id)

    def _cell(item, col_name):
        vals = tree.item(item, "values")
        idx = COL_INDEX[col_name]
        return vals[idx] if idx < len(vals) else None

    def _is_true(x):
        return str(x).strip().lower() in ("true", "1", "sí", "si")

    def actualizar_contador(*_):
        seleccion = tree.selection()
        n = 0
        for it in seleccion:
            if _is_true(_cell(it, "Mensaje")):
                n += 1
        contador_var.set(f"Seleccionados con Mensaje=True: {n}")

    style = ttk.Style()

    def _row_height():
        h = style.lookup("Treeview", "rowheight")
        try:
            return int(h)
        except Exception:
            return 20  # fallback

    def ajustar_altura_tree(*_):
        tree.update_idletasks()
        n_rows = len(tree.get_children(""))
        H = tabla_frame.winfo_height() or tree.winfo_height()
        margen = 40
        rh = _row_height()
        max_rows_fit = max(3, (H - margen) // rh)
        altura = min(n_rows, max_rows_fit)
        tree.configure(height=altura)

    def ordenar_columna(col):
        datos = [(tree.set(iid, col), iid) for iid in tree.get_children()]
        reverse = orden_actual[col] == "asc"

        def convertir(valor):
            try:
                return float(valor)
            except:
                try:
                    return dt.datetime.strptime(valor, "%d-%m-%Y")
                except:
                    return valor.lower()

        datos.sort(key=lambda x: convertir(x[0]), reverse=reverse)
        for idx, (val, iid) in enumerate(datos):
            tree.move(iid, '', idx)

        orden_actual[col] = "desc" if reverse else "asc"

        # Actualiza encabezados visualmente con la flecha
        for c in columnas:
            texto = encabezados.get(c, c)
            if c == col:
                texto += " ▲" if not reverse else " ▼"
            tree.heading(c, text=texto, command=lambda c=c: ordenar_columna(c))


    for col in columnas:
        texto_col = encabezados.get(col, col)
        tree.heading(col, text=texto_col, command=lambda c=col: ordenar_columna(c))
        tree.column(col, anchor="center", width=110)

    if "Genero" in columnas:
        tree.heading("Genero", text="Género", command=lambda c="Genero": ordenar_columna(c))
        tree.column("Genero", width=90, anchor="center")

    seleccionar_todos_var = tk.BooleanVar(value=False)

    def toggle_seleccionar_todos():
        if seleccionar_todos_var.get():
            tree.selection_set(tree.get_children())
        else:
            tree.selection_remove(tree.get_children())
        actualizar_contador()

    def aplicar_filtros():
        _hide_cal_popup()
        tree.delete(*tree.get_children())
        criterios = {col: entradas_filtro[col].get().strip().lower() for col in columnas}
        for row in datos_originales:
            visible = True
            for col in columnas:
                valor = str(row.get(col, "")).lower().strip()
                if criterios[col] and criterios[col] not in valor:
                    visible = False
                    break
            if visible:
                uid_row = row["UID"]
                valores = row_to_values(row)
                tags = ("has_days_row",) if uid_row in upcoming_by_uid else ()
                tree.insert("", "end", iid=uid_row, values=valores, tags=tags)
                rows_by_iid[uid_row] = row
        toggle_seleccionar_todos()
        ajustar_altura_tree()

    def limpiar_filtros():
        for entry in entradas_filtro.values():
            entry.delete(0, tk.END)
        aplicar_filtros()

    def guardar_dato(uid, campo, valor):
        try:
            doc_ref = db.collection("UsuariosAutorizados").document(uid)
            if campo in ["Mensaje", "Seleccionable", "Valor"]:
                valor = valor == "True"
            elif campo in ["TotalDia"]:
                valor = int(valor)
            elif campo in ["TotalHoras"]:
                valor = float(valor)
            elif campo in ["Alta", "UltimoDia", "Baja", "Codigo", "Genero"]:
                valor = s_trim(valor)
            doc_ref.update({campo: valor})
        except Exception as e:
            print(f"⚠️ Error al guardar {campo} de {uid}: {e}")

    def editar_celda(event):
        item_id = tree.focus()
        if not item_id:
            return
        col = tree.identify_column(event.x)
        col_index = int(col.replace("#", "")) - 1
        col_nombre = columnas[col_index]

        if col_nombre in ["Mensaje", "Seleccionable", "Valor"]:
            val = tree.set(item_id, col_nombre)
            nuevo = "False" if val == "True" else "True"
            tree.set(item_id, col_nombre, nuevo)
            guardar_dato(item_id, col_nombre, nuevo)
            for fila in datos_originales:
                if fila["UID"] == item_id:
                    fila[col_nombre] = nuevo
                    break
            actualizar_contador()
        else:
            x, y, width, height = tree.bbox(item_id, column=col)
            if col_nombre == "Nombre":
                fila_actual = rows_by_iid.get(item_id, {})
                valor_actual = fila_actual.get("Nombre", "")
            else:
                valor_actual = tree.set(item_id, col_nombre)
            entry = tk.Entry(tree)
            entry.insert(0, valor_actual)
            entry.place(x=x, y=y, width=width, height=height)
            entry.focus()

            def guardar_valor(event=None):
                nuevo_valor = entry.get()
                display_value = (
                    formatear_nombre(item_id, nuevo_valor)
                    if col_nombre == "Nombre"
                    else nuevo_valor
                )
                tree.set(item_id, col_nombre, display_value)
                guardar_dato(item_id, col_nombre, nuevo_valor)
                for fila in datos_originales:
                    if fila["UID"] == item_id:
                        fila[col_nombre] = nuevo_valor
                        break
                if item_id in rows_by_iid:
                    rows_by_iid[item_id][col_nombre] = nuevo_valor
                entry.destroy()

            entry.bind("<Return>", guardar_valor)
            entry.bind("<FocusOut>", guardar_valor)

    def cargar_datos():
        nonlocal datos_originales
        datos_originales = []
        rows_by_iid.clear()
        _hide_cal_popup()
        tree.delete(*tree.get_children())

        t0 = time.time()
        usuarios_docs = list(db.collection("UsuariosAutorizados").stream())
        t1 = time.time()

        hoy = dt.datetime.now().date()
        rango_fin = hoy + timedelta(days=5)

        peticiones_cursor = list(
            db.collection("Peticiones")
            .where("Fecha", ">=", start_of_day_local_to_utc(hoy))
            .where("Fecha", "<=", end_of_day_local_to_utc(rango_fin))
            .stream()
        )

        upcoming_by_uid.clear()
        for pet_doc in peticiones_cursor:
            d = pet_doc.to_dict() or {}
            if not _is_ok(d.get("Admitido")):
                continue
            uid_pet = d.get("uid") or d.get("Uid")
            f = _timestamp_to_local_date(d.get("Fecha"))
            if uid_pet and f and hoy <= f <= rango_fin:
                upcoming_by_uid[uid_pet].append(f)

        for fechas in upcoming_by_uid.values():
            fechas.sort()

        print(
            f"🔎 Peticiones OK próximos 5 días: {sum(len(v) for v in upcoming_by_uid.values())} en {len(upcoming_by_uid)} usuarios"
        )

        dnis = set()
        min_alta = hoy - timedelta(days=365)
        for doc in usuarios_docs:
            data = doc.to_dict()
            dni = normalizar_dni(data.get("Dni"))
            if dni:
                dnis.add(dni)
                alta = to_date(data.get("Alta"))
                if alta and alta < min_alta:
                    min_alta = alta

        trab_by_dni = cargar_trabajadores(dnis)
        altas_por_dni: Dict[str, date] = {}
        for dni_trab, info_trab in trab_by_dni.items():
            alta_dt = info_trab.get('AltaDate') if isinstance(info_trab, dict) else None
            if isinstance(alta_dt, date):
                altas_por_dni[dni_trab] = alta_dt
                if alta_dt < min_alta:
                    min_alta = alta_dt
        t2 = time.time()
        ajust_by_dni = cargar_datos_ajustados(dnis, min_alta, altas_por_dni)
        t3 = time.time()

        total = len(usuarios_docs)

        def procesar_doc(doc):
            uid = doc.id
            data = doc.to_dict() or {}
            try:
                actualiza = {}
                dni_original = data.get("Dni")
                dni_normalizado = normalizar_dni(dni_original)
                data["Dni"] = dni_normalizado or "Falta"
                genero_final = _normalizar_genero_existente(data.get("Genero"))

                total_dia_actual = _to_int_safe(data.get("TotalDia"))
                total_horas_actual = float(round(_to_float_safe(data.get("TotalHoras")), 2))
                baja_actual_norm = _normalize_optional_str(data.get("Baja"))

                if dni_normalizado:
                    trab = trab_by_dni.get(dni_normalizado, {})
                    if trab:
                        for campo in ("Nombre", "Alta", "Codigo"):
                            val = trab.get(campo)
                            if val and val != data.get(campo):
                                actualiza[campo] = val
                                data[campo] = val
                        genero_trab = trab.get("Genero")
                        if genero_trab:
                            genero_final = genero_trab
                    else:
                        actualiza["Mensaje"] = False
                        actualiza["Seleccionable"] = False
                        data["Mensaje"] = False
                        data["Seleccionable"] = False
                else:
                    actualiza["Mensaje"] = False
                    actualiza["Seleccionable"] = False
                    data["Mensaje"] = False
                    data["Seleccionable"] = False

                totales_info = calcular_totales_y_baja(dni_normalizado)
                baja_str = totales_info.get("baja_str")
                total_dia_calculado = _to_int_safe(totales_info.get("total_dia"))
                total_horas_calculado = float(round(_to_float_safe(totales_info.get("total_horas")), 2))
                fecha_alta_calc = totales_info.get("fecha_alta")

                if fecha_alta_calc and not s_trim(data.get("Alta")):
                    alta_calc_str = _date_to_str_ddmmyyyy(fecha_alta_calc)
                    if alta_calc_str:
                        actualiza["Alta"] = alta_calc_str
                        data["Alta"] = alta_calc_str

                updates = {
                    "Baja": baja_str,
                    "TotalDia": int(total_dia_calculado),
                    "TotalHoras": float(round(total_horas_calculado, 2)),
                }

                baja_nueva_norm = _normalize_optional_str(updates["Baja"])
                if baja_nueva_norm != baja_actual_norm:
                    actualiza["Baja"] = updates["Baja"]
                data["Baja"] = updates["Baja"]

                if updates["TotalDia"] != total_dia_actual:
                    actualiza["TotalDia"] = updates["TotalDia"]
                data["TotalDia"] = updates["TotalDia"]

                if updates["TotalHoras"] != total_horas_actual:
                    actualiza["TotalHoras"] = updates["TotalHoras"]
                data["TotalHoras"] = updates["TotalHoras"]

                ajust = ajust_by_dni.get(dni_normalizado, {})
                if ajust:
                    ultima = ajust.get("UltimoDia")
                    if ultima:
                        ultima_str = _date_to_str_ddmmyyyy(ultima)
                        if ultima_str != data.get("UltimoDia"):
                            actualiza["UltimoDia"] = ultima_str
                            data["UltimoDia"] = ultima_str
                    puesto = ajust.get("Puesto")
                    if puesto and puesto != data.get("Puesto"):
                        actualiza["Puesto"] = puesto
                        data["Puesto"] = puesto

                if baja_str:
                    actualiza["Mensaje"] = False
                    actualiza["Seleccionable"] = False
                    data["Mensaje"] = False
                    data["Seleccionable"] = False

                if not genero_final:
                    genero_final = "Otro"
                actualiza["Genero"] = genero_final
                data["Genero"] = genero_final

                data["Nombre"] = data.get("Nombre", "Falta")
                data["Telefono"] = data.get("Telefono", "")
                data["correo"] = data.get("correo", "")
                data["Puesto"] = data.get("Puesto", "Falta")
                data["Turno"] = str(data.get("Turno", "1"))
                data["Mensaje"] = str(data.get("Mensaje", False))
                data["Seleccionable"] = str(data.get("Seleccionable", True))
                data["Valor"] = str(data.get("Valor", False))
                data["Alta"] = data.get("Alta") or _date_to_str_ddmmyyyy(hoy)
                data["UltimoDia"] = data.get("UltimoDia") or _date_to_str_ddmmyyyy(hoy)
                data["TotalDia"] = str(total_dia_calculado)
                data["TotalHoras"] = f"{total_horas_calculado:.2f}"
                data["Baja"] = data.get("Baja") or ""
                data["Codigo"] = s_trim(data.get("Codigo")) or ""

                fila = {"UID": uid, **{col: data.get(col, "") for col in columnas}}
                return uid, fila, actualiza
            except Exception as e:
                print(f"❌ Error procesando {uid}: {e}")
                fila = {"UID": uid, **{col: data.get(col, "") for col in columnas}}
                return uid, fila, {}

        with ThreadPoolExecutor(max_workers=8) as ex:
            resultados = list(ex.map(procesar_doc, usuarios_docs))
        t4 = time.time()

        batch = db.batch()
        ops = 0
        for idx, (uid, fila, actualiza) in enumerate(resultados, start=1):
            if actualiza:
                ref = db.collection("UsuariosAutorizados").document(uid)
                batch.update(ref, actualiza)
                ops += 1
                if ops % 400 == 0:
                    batch.commit()
                    batch = db.batch()
            datos_originales.append(fila)
            rows_by_iid[uid] = fila
            valores = row_to_values(fila)
            tags = ("has_days_row",) if uid in upcoming_by_uid else ()
            tree.insert("", "end", iid=uid, values=valores, tags=tags)
            if idx % 200 == 0:
                print(f"Procesados {idx}/{total}")
        if ops % 400:
            batch.commit()
        t5 = time.time()

        ajustar_altura_tree()
        actualizar_contador()

        print(
            f"⏱️ t0→t1 Firebase {t1 - t0:.2f}s | t1→t2 TRAB {t2 - t1:.2f}s | "
            f"t2→t3 AJUST {t3 - t2:.2f}s | t3→t4 proc {t4 - t3:.2f}s | "
            f"t4→t5 commit {t5 - t4:.2f}s | total {t5 - t0:.2f}s"
        )

    def toggle_mensaje():
        seleccion = tree.selection()
        if not seleccion:
            messagebox.showwarning("⚠️ Selección", "Selecciona uno o más usuarios.")
            return
        for uid in seleccion:
            valor_actual = tree.set(uid, "Mensaje")
            nuevo_valor = "False" if valor_actual == "True" else "True"
            tree.set(uid, "Mensaje", nuevo_valor)
            guardar_dato(uid, "Mensaje", nuevo_valor)
            for fila in datos_originales:
                if fila["UID"] == uid:
                    fila["Mensaje"] = nuevo_valor
                    break
            if uid in rows_by_iid:
                rows_by_iid[uid]["Mensaje"] = nuevo_valor
        actualizar_contador()

    def eliminar_usuario():
        seleccion = tree.focus()
        if not seleccion:
            messagebox.showwarning("⚠️ Selección", "Selecciona un usuario para eliminar.")
            return

        uid = seleccion
        nombre = tree.set(uid, "Nombre")

        if not messagebox.askyesno("Confirmación", f"¿Eliminar al usuario '{nombre}' ({uid})?\nEsta acción no se puede deshacer."):
            return

        try:
            # Eliminar de Firestore
            db.collection("UsuariosAutorizados").document(uid).delete()
            print(f"✅ Documento {uid} eliminado de Firestore.")

            # Eliminar de Firebase Auth si existe
            try:
                auth.delete_user(uid)
                print(f"✅ Usuario {uid} eliminado de Firebase Auth.")
            except auth.UserNotFoundError:
                print(f"⚠️ Usuario {uid} no encontrado en Firebase Auth.")
            except Exception as e:
                print(f"❌ Error al eliminar en Firebase Auth: {e}")

            messagebox.showinfo("✅ Eliminado", f"Usuario '{nombre}' eliminado correctamente.")
            cargar_datos()
        except Exception as e:
            messagebox.showerror("❌ Error", f"No se pudo eliminar el usuario:\n{e}")
    def guardar_todo():
        for item in tree.get_children():
            uid = item
            fila_base = rows_by_iid.get(uid)
            if fila_base:
                datos = {col: fila_base.get(col, "") for col in columnas}
            else:
                valores = tree.item(item, "values")
                datos = {}
                for col, valor in zip(columnas, valores):
                    if col == "Nombre" and isinstance(valor, str) and valor.startswith("🔴 "):
                        valor = valor[2:]
                    datos[col] = valor

            # Conversión de tipos
            for campo in ["Mensaje", "Seleccionable", "Valor"]:
                datos[campo] = datos[campo] == "True"

            for campo in ["TotalDia"]:
                try:
                    datos[campo] = int(datos[campo])
                except:
                    datos[campo] = 0

            for campo in ["TotalHoras"]:
                try:
                    datos[campo] = float(datos[campo])
                except:
                    datos[campo] = 0.0

            for campo in ["Alta", "UltimoDia", "Baja", "Codigo", "Genero"]:
                datos[campo] = s_trim(datos.get(campo))

            try:
                db.collection("UsuariosAutorizados").document(uid).update(datos)
            except Exception as e:
                print(f"⚠️ Error guardando {uid}: {e}")

        messagebox.showinfo("✅ Guardado", "Todos los cambios han sido guardados en Firebase.")

    cargando = False

    def refrescar():
        nonlocal cargando
        if cargando:
            return
        cargando = True
        try:
            _hide_cal_popup()

            hay_filtros = any(entradas_filtro[c].get().strip() for c in columnas)

            y0 = tree.yview()

            cargar_datos()

            if hay_filtros:
                aplicar_filtros()

            try:
                tree.yview_moveto(y0[0])
            except Exception:
                pass

            actualizar_contador()

            from datetime import datetime as _dt

            ultima_act_var.set("Actualizado: " + _dt.now().strftime("%H:%M:%S"))
        finally:
            cargando = False

    btn_actualizar = tk.Button(frame_botones, text="🔄 Actualizar", command=refrescar)
    btn_actualizar.pack(side="left", padx=10)

    tk.Button(frame_botones, text="🔍 Filtrar", command=aplicar_filtros).pack(side="left", padx=10)
    tk.Button(frame_botones, text="🧹 Limpiar", command=limpiar_filtros).pack(side="left", padx=10)
    tk.Checkbutton(frame_botones, text="Seleccionar Todos", variable=seleccionar_todos_var, command=toggle_seleccionar_todos).pack(side="left", padx=10)
    tk.Button(frame_botones, text="Mensaje", command=toggle_mensaje).pack(side="left", padx=10)
    tk.Button(frame_botones, text="🗑 Eliminar seleccionado", bg="salmon", command=eliminar_usuario).pack(side="left", padx=10)
    tk.Button(frame_botones, text="💾 Guardar todo", bg="lightgreen", command=guardar_todo).pack(side="left", padx=10)

    tree.bind("<Double-1>", editar_celda)
    tree.bind("<<TreeviewSelect>>", actualizar_contador)
    tree.bind("<ButtonRelease-1>", lambda e: tree.after(1, actualizar_contador))
    tree.bind("<Motion>", _hover_calendar)
    tree.bind("<Leave>", lambda e: _hide_cal_popup())
    tree.bind("<ButtonPress-1>", lambda e: _hide_cal_popup())
    tree.bind("<MouseWheel>", lambda e: _hide_cal_popup())
    ventana.bind("<Configure>", ajustar_altura_tree)
    ventana.bind("<F5>", lambda e: refrescar())
    ventana.bind("<Control-r>", lambda e: refrescar())

    refrescar()

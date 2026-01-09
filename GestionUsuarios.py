import tkinter as tk
from tkinter import ttk, messagebox
from tkcalendar import Calendar
import pyodbc
import os
from firebase_admin import auth, firestore
from google.api_core.exceptions import AlreadyExists
import datetime as dt
from datetime import date, timedelta, timezone
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Union, Dict, Iterable, Tuple, List, Any, Set
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


# Ruta y utilidades Access
ACCESS_DB_PATH = r"X:\\ENLACES\\Power BI\\Campaña\\PercecoBi(Campaña).mdb"


def _open_access_connection() -> Optional[pyodbc.Connection]:
    if not os.path.exists(ACCESS_DB_PATH):
        print("❌ Ruta MDB no encontrada.")
        return None
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={ACCESS_DB_PATH};'
    )
    try:
        return pyodbc.connect(str(conn_str))
    except Exception as exc:
        print(f"❌ Error abriendo MDB: {exc}")
        return None


def _ensure_cursor(cursor: Optional[pyodbc.Cursor]) -> Tuple[Optional[pyodbc.Cursor], Optional[pyodbc.Connection]]:
    if cursor is not None:
        return cursor, None
    conn = _open_access_connection()
    if not conn:
        return None, None
    return conn.cursor(), conn


def _close_cursor(cursor: Optional[pyodbc.Cursor], conn: Optional[pyodbc.Connection]) -> None:
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


def _range_start(d: date) -> dt.datetime:
    return dt.datetime.combine(d, dt.time.min)


def _range_end(d: date) -> dt.datetime:
    return dt.datetime.combine(d, dt.time(23, 59, 59))


def get_trabajador_por_dni_y_centro(
    dni: str,
    centro: str = "00005",
    cursor: Optional[pyodbc.Cursor] = None,
) -> Optional[Any]:
    dni_param = (dni or "").strip().upper()
    if not dni_param:
        return None
    local_cursor, conn = _ensure_cursor(cursor)
    if local_cursor is None:
        return None
    try:
        query = (
            "SELECT TOP 1 DNI, CODIGO, FECHAALTA, FECHABAJA, SEXO, APELLIDOS, APELLIDOS2, NOMBRE "
            "FROM TRABAJADORES "
            "WHERE Trim(UCase(DNI)) = ? AND CENTRO = ? "
            "ORDER BY FECHAALTA DESC"
        )
        row = local_cursor.execute(query, (dni_param, centro)).fetchone()
        if row is None:
            print(f"[TRAB] DNI={dni_param} centro={centro} no encontrado")
        else:
            alta = parse_access_date(getattr(row, "FECHAALTA", None))
            baja = parse_access_date(getattr(row, "FECHABAJA", None))
            print(
                f"[TRAB] DNI={dni_param} centro={centro} alta={fmt_dmy(alta)} baja={fmt_dmy(baja)}"
            )
        return row
    except Exception as exc:
        print(f"❌ Error obteniendo trabajador {dni_param} centro={centro}: {exc}")
        return None
    finally:
        if cursor is None:
            _close_cursor(local_cursor, conn)


def contar_dias_distintos(
    dni: str,
    desde: date,
    hasta: date,
    cursor: Optional[pyodbc.Cursor] = None,
) -> int:
    if not dni or not desde or not hasta or desde > hasta:
        return 0
    dni_param = dni.strip().upper()
    local_cursor, conn = _ensure_cursor(cursor)
    if local_cursor is None:
        return 0
    try:
        query = (
            "SELECT COUNT(*) AS total_dias FROM ("
            "  SELECT DISTINCT FECHA"
            "  FROM DATOS_AJUSTADOS"
            "  WHERE Trim(UCase(DNI)) = ? AND FECHA >= ? AND FECHA <= ?"
            ") t"
        )
        params = (dni_param, _range_start(desde), _range_end(hasta))
        row = local_cursor.execute(query, params).fetchone()
        return int(row[0] or 0) if row else 0
    except Exception as exc:
        print(
            f"⚠️ Error contando días para {dni_param} entre {desde} y {hasta}: {exc}"
        )
        return 0
    finally:
        if cursor is None:
            _close_cursor(local_cursor, conn)


def sumar_horas(
    dni: str,
    desde: date,
    hasta: date,
    cursor: Optional[pyodbc.Cursor] = None,
) -> float:
    if not dni or not desde or not hasta or desde > hasta:
        return 0.0
    dni_param = dni.strip().upper()
    local_cursor, conn = _ensure_cursor(cursor)
    if local_cursor is None:
        return 0.0
    try:
        query = (
            "SELECT SUM(NZ(HORAS,0) + NZ(HORASEXT,0)) AS total_horas "
            "FROM DATOS_AJUSTADOS "
            "WHERE Trim(UCase(DNI)) = ? AND FECHA >= ? AND FECHA <= ?"
        )
        params = (dni_param, _range_start(desde), _range_end(hasta))
        row = local_cursor.execute(query, params).fetchone()
        if row and row[0] is not None:
            return float(row[0])
    except Exception:
        try:
            fallback_query = (
                "SELECT HORAS, HORASEXT FROM DATOS_AJUSTADOS "
                "WHERE Trim(UCase(DNI)) = ? AND FECHA >= ? AND FECHA <= ?"
            )
            total = 0.0
            for fila in local_cursor.execute(
                fallback_query, (dni_param, _range_start(desde), _range_end(hasta))
            ):
                horas = getattr(fila, "HORAS", 0)
                horas_ext = getattr(fila, "HORASEXT", 0)
                total += float(horas or 0) + float(horas_ext or 0)
            return total
        except Exception as exc_inner:
            print(
                f"⚠️ Error sumando horas para {dni_param} entre {desde} y {hasta}: {exc_inner}"
            )
    finally:
        if cursor is None:
            _close_cursor(local_cursor, conn)
    return 0.0


# Ventana principal de gestión de usuarios (singleton)
# -- Notificación desde otras pantallas (p.ej. Gestión de Mensajes)
ventana_usuarios = None          # ya existe; asegúrate que es global
_notify_reset_cb = None          # callback para actualizar UI abierta


def on_mensajes_generados(uids, db):
    """
    Llamar desde Gestión de Mensajes al finalizar el envío/creación.
    - uids: lista de uid (str) a los que se les generó mensaje
    - db: cliente Firestore
    Efectos:
      1) Actualiza Firestore: UsuariosAutorizados/{uid}.Mensaje = False (bool)
      2) Si Gestión de Usuarios está abierta, actualiza UI en caliente.
    """
    if not uids:
        return

    # 1) Actualiza Firestore en batch
    try:
        batch = db.batch()
        ops = 0
        for uid in uids:
            ref = db.collection("UsuariosAutorizados").document(uid)
            batch.update(ref, {"Mensaje": False})
            ops += 1
            if ops % 400 == 0:
                batch.commit()
                batch = db.batch()
        if ops % 400:
            batch.commit()
    except Exception as e:
        print(f"⚠️ No se pudo actualizar Mensaje=False en Firestore: {e}")

    # 2) Actualiza la UI si está abierta
    if _notify_reset_cb is not None:
        try:
            _notify_reset_cb(list(uids))
        except Exception as e:
            print(f"⚠️ Error notificando a Gestión de Usuarios: {e}")

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

def normalize_genero(value: Any) -> str:
    texto = s_trim(value)
    if not texto:
        return "(Vacías)"
    minus = texto.casefold()
    if minus in {"h", "hombre", "masculino", "male"}:
        return "Hombre"
    if minus in {"m", "f", "mujer", "femenino", "female"}:
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


def dias_sin_trabajar(ultimo_dia: Optional[Union[str, date, dt.datetime]]) -> int:
    """Calcula los días transcurridos desde ``ultimo_dia`` hasta hoy."""

    hoy = date.today()
    if not ultimo_dia:
        return 999

    if isinstance(ultimo_dia, str):
        parsed = to_date(ultimo_dia)
    elif isinstance(ultimo_dia, dt.datetime):
        parsed = ultimo_dia.date()
    elif isinstance(ultimo_dia, date):
        parsed = ultimo_dia
    else:
        parsed = to_date(ultimo_dia)

    if not parsed:
        return 999

    try:
        return (hoy - parsed).days
    except Exception:
        return 999


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
    resultado: Dict[str, Dict[str, Optional[Union[str, date]]]] = {}
    conn = _open_access_connection()
    if not conn:
        return resultado

    try:
        cursor = conn.cursor()
        vistos: set[str] = set()
        for dni_bruto in dnis:
            dni_norm = normalizar_dni(dni_bruto)
            if not dni_norm or dni_norm in vistos:
                continue
            vistos.add(dni_norm)
            row = get_trabajador_por_dni_y_centro(dni_norm, cursor=cursor)
            if not row:
                continue
            alta_dt = parse_access_date(getattr(row, 'FECHAALTA', None))
            baja_dt = parse_access_date(getattr(row, 'FECHABAJA', None))
            ap1 = s_trim(getattr(row, 'APELLIDOS', None))
            ap2 = s_trim(getattr(row, 'APELLIDOS2', None))
            nom = s_trim(getattr(row, 'NOMBRE', None))
            nombre_compuesto = ' '.join([t for t in (ap1, ap2, nom) if t]).strip() or 'Falta'
            resultado[dni_norm] = {
                'Nombre': nombre_compuesto,
                'Alta': _date_to_str_ddmmyyyy(alta_dt),
                'Baja': _date_to_str_ddmmyyyy(baja_dt),
                'Codigo': s_trim(getattr(row, 'CODIGO', None)),
                'AltaDate': alta_dt,
                'BajaDate': baja_dt,
                'Genero': map_genero(getattr(row, 'SEXO', None)),
            }
    except Exception as e:
        print(f"❌ Error cargando TRABAJADORES: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return resultado


def cargar_datos_ajustados(
    dnis: Iterable[str],
    min_alta: date,
    altas_por_dni: Optional[Dict[str, date]] = None,
) -> Dict[str, Dict[str, Union[date, int, float, str, None]]]:
    """Lectura única de DATOS_AJUSTADOS con agregados por DNI."""
    ruta = ACCESS_DB_PATH
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

    conn = _open_access_connection()
    if not conn:
        return datos
    try:
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


def calcular_totales_y_baja(dni: str) -> Dict[str, Union[int, float, Optional[date], Optional[str]]]:
    resultado: Dict[str, Union[int, float, Optional[date], Optional[str]]] = {
        'total_dia': 0,
        'total_horas': 0.0,
        'total_dia_mes_actual': 0,
        'total_dia_semana_actual': 0,
        'fecha_alta': None,
        'fecha_baja': None,
        'baja_str': None,
    }
    dni_normalizado = normalizar_dni(dni)
    if not dni_normalizado:
        return resultado

    conn = _open_access_connection()
    if not conn:
        return resultado

    cursor = None
    try:
        cursor = conn.cursor()
        row = get_trabajador_por_dni_y_centro(dni_normalizado, cursor=cursor)
        if not row:
            return resultado

        fecha_alta = parse_access_date(getattr(row, 'FECHAALTA', None))
        fecha_baja = parse_access_date(getattr(row, 'FECHABAJA', None))
        baja_str = fmt_dmy(fecha_baja)

        resultado['fecha_alta'] = fecha_alta
        resultado['fecha_baja'] = fecha_baja
        resultado['baja_str'] = baja_str

        hoy = date.today()
        if fecha_baja:
            print(f"[AJUST] DNI={dni_normalizado} baja={baja_str} -> Mensaje=False")
            return resultado

        if fecha_alta and fecha_alta <= hoy:
            total_dia = contar_dias_distintos(dni_normalizado, fecha_alta, hoy, cursor=cursor)
            total_horas = sumar_horas(dni_normalizado, fecha_alta, hoy, cursor=cursor)
            resultado['total_dia'] = int(total_dia)
            resultado['total_horas'] = round(float(total_horas), 2)
            print(
                f"[AJUST] DNI={dni_normalizado} rango_global={fmt_dmy(fecha_alta)}→{fmt_dmy(hoy)} "
                f"días={total_dia} horas={resultado['total_horas']:.2f}"
            )
        else:
            print(f"[AJUST] DNI={dni_normalizado} sin fecha de alta válida para totales globales")

        primer_dia_mes = hoy.replace(day=1)
        primer_dia_semana = hoy - timedelta(days=hoy.weekday())

        desde_mes = primer_dia_mes
        if fecha_alta and fecha_alta > desde_mes:
            desde_mes = fecha_alta
        if desde_mes <= hoy:
            total_mes = contar_dias_distintos(dni_normalizado, desde_mes, hoy, cursor=cursor)
            resultado['total_dia_mes_actual'] = int(total_mes)
            print(
                f"[AJUST] DNI={dni_normalizado} rango_mes={fmt_dmy(desde_mes)}→{fmt_dmy(hoy)} "
                f"días={total_mes}"
            )

        desde_semana = primer_dia_semana
        if fecha_alta and fecha_alta > desde_semana:
            desde_semana = fecha_alta
        if desde_semana <= hoy:
            total_semana = contar_dias_distintos(dni_normalizado, desde_semana, hoy, cursor=cursor)
            resultado['total_dia_semana_actual'] = int(total_semana)
            print(
                f"[AJUST] DNI={dni_normalizado} rango_semana={fmt_dmy(desde_semana)}→{fmt_dmy(hoy)} "
                f"días={total_semana}"
            )
    except Exception as e:
        print(f"⚠️ Error calculando totales para {dni_normalizado}: {e}")
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        try:
            conn.close()
        except Exception:
            pass
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
                row = get_trabajador_por_dni_y_centro(dni_param, cursor=cursor)
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
    global ventana_usuarios, _notify_reset_cb
    if ventana_usuarios and ventana_usuarios.winfo_exists():
        ventana_usuarios.lift()
        ventana_usuarios.focus_force()
        return

    ventana_usuarios = tk.Toplevel()
    ventana = ventana_usuarios
    ventana.title("👥 Gestión de Usuarios")
    ventana.geometry("1400x600")

    columnas = ["Dni", "Nombre", "Telefono", "correo", "Puesto", "Turno", "Genero",
                "Mensaje", "Seleccionable", "Valor", "Alta", "UltimoDia", "TotalDia", "TotalHoras",
                "TotalDiaMesActual", "TotalDiaSemanaActual", "Baja", "Codigo"]

    encabezados = {
        "Dni": "Dni", "Nombre": "Nombre", "Telefono": "Teléfono", "correo": "Correo",
        "Puesto": "Puesto", "Turno": "Turno", "Genero": "Género",
        "Mensaje": "Mensaje", "Seleccionable": "Seleccionable", "Valor": "Valor",
        "Alta": "Alta", "UltimoDia": "Último Día", "TotalDia": "Total Día",
        "TotalHoras": "Total Horas", "TotalDiaMesActual": "Total Día Mes Actual",
        "TotalDiaSemanaActual": "Total Día Semana Actual", "Baja": "Baja", "Codigo": "Código"
    }

    datos_originales = []
    @dataclass(frozen=True)
    class FilterState:
        selected_values: Optional[Set[str]] = None
        operator: Optional[str] = None
        value1: Any = None
        value2: Any = None

        def __post_init__(self) -> None:
            if self.selected_values is not None and self.operator is not None:
                raise ValueError("FilterState no puede mezclar selected_values y operator.")
            if self.selected_values is None and self.operator is None:
                raise ValueError("FilterState debe definir selected_values u operator.")

    filtros_activos: Dict[str, FilterState] = {}
    column_types: Dict[str, str] = {}
    rows_by_iid: Dict[str, Dict[str, str]] = {}
    upcoming_by_uid: Dict[str, List[date]] = defaultdict(list)
    cal_popup: Optional[tk.Toplevel] = None
    cal_uid: Optional[str] = None
    dni_dialog_abierto = False
    empty_filter_label = "(Vacías)"
    date_columns = {"Alta", "UltimoDia", "Baja"}
    bool_columns = {"Mensaje", "Seleccionable"}

    def _local_timezone():
        tz = dt.datetime.now().astimezone().tzinfo
        return tz or timezone.utc

    def _to_timestamp_inicio_dia(dia: date) -> dt.datetime:
        tz = _local_timezone()
        return dt.datetime(dia.year, dia.month, dia.day, tzinfo=tz)

    def _yyyymmdd(dia: date) -> str:
        return dia.strftime("%Y%m%d")

    def crear_peticiones_en_firestore(uid: str, dias: Set[date], motivo: str) -> tuple[int, int]:
        nuevas = 0
        duplicadas = 0
        coleccion = db.collection("Peticiones")
        tz = _local_timezone()

        for dia in sorted(dias):
            doc_id = f"{uid}_{_yyyymmdd(dia)}"
            doc_ref = coleccion.document(doc_id)
            payload = {
                "uid": uid,
                "Fecha": _to_timestamp_inicio_dia(dia),
                "Motivo": motivo,
                "Admitido": "Ok",
                "creadoEn": dt.datetime.now(tz=tz),
            }
            try:
                doc_ref.create(payload)
                nuevas += 1
            except AlreadyExists:
                duplicadas += 1
            except Exception:
                raise

        return nuevas, duplicadas

    def abrir_dialogo_peticiones(usuario: Dict[str, Any] | None) -> None:
        nonlocal dni_dialog_abierto
        if not usuario or dni_dialog_abierto:
            return

        uid = usuario.get("UID")
        if not uid:
            messagebox.showwarning(
                "Peticiones",
                "No se pudo determinar el UID del usuario seleccionado.",
            )
            return

        nombre_usuario = usuario.get("Nombre") or ""
        dni_usuario = usuario.get("Dni") or ""

        dni_dialog_abierto = True
        dialogo: tk.Toplevel | None = None

        def cerrar() -> None:
            nonlocal dialogo
            try:
                ventana.attributes("-disabled", False)
            except Exception:
                pass
            if dialogo is None:
                ventana.focus_force()
                return
            try:
                dialogo.grab_release()
            except Exception:
                pass
            if dialogo.winfo_exists():
                dialogo.destroy()
            dialogo = None
            ventana.focus_force()

        try:
            dialogo = tk.Toplevel(ventana)
            dialogo.title(f"Peticiones de días libres - {nombre_usuario} ({dni_usuario})")
            dialogo.transient(ventana)
            dialogo.grab_set()
            dialogo.resizable(False, False)

            try:
                ventana.attributes("-disabled", True)
            except Exception:
                pass

            contenido = ttk.Frame(dialogo, padding=16)
            contenido.grid(row=0, column=0, sticky="nsew")
            dialogo.grid_rowconfigure(0, weight=1)
            dialogo.grid_columnconfigure(0, weight=1)

            cal_kwargs = {
                "selectmode": "day",
                "firstweekday": "monday",
                "showweeknumbers": False,
            }
            try:
                calendario = Calendar(contenido, locale="es_ES", **cal_kwargs)
            except Exception:
                calendario = Calendar(contenido, **cal_kwargs)
            calendario.grid(row=0, column=0, columnspan=2, sticky="nsew")
            calendario.tag_config("sel", background="#1e88e5", foreground="white")

            seleccionados: Set[date] = set()
            eventos_por_dia: Dict[date, int] = {}
            dias_var = tk.StringVar(value="Días seleccionados: 0")

            def _actualizar_label() -> None:
                dias_var.set(f"Días seleccionados: {len(seleccionados)}")

            def _agregar_dia(dia: date) -> None:
                if dia in seleccionados:
                    return
                seleccionados.add(dia)
                eventos_por_dia[dia] = calendario.calevent_create(dia, "seleccionado", "sel")
                _actualizar_label()

            def _quitar_dia(dia: date) -> None:
                if dia not in seleccionados:
                    return
                seleccionados.remove(dia)
                event_id = eventos_por_dia.pop(dia, None)
                if event_id is not None:
                    calendario.calevent_remove(event_id)
                _actualizar_label()

            def _toggle_dia(_event=None) -> None:
                try:
                    dia_sel = calendario.selection_get()
                except Exception:
                    return
                if not isinstance(dia_sel, date):
                    return
                if dia_sel in seleccionados:
                    _quitar_dia(dia_sel)
                else:
                    _agregar_dia(dia_sel)

            calendario.bind("<<CalendarSelected>>", _toggle_dia)

            ttk.Label(contenido, textvariable=dias_var).grid(
                row=1, column=0, columnspan=2, sticky="w", pady=(8, 4)
            )

            botones_cal = ttk.Frame(contenido)
            botones_cal.grid(row=2, column=0, columnspan=2, sticky="w", pady=(0, 12))

            def _dia_actual() -> date | None:
                try:
                    dia_sel = calendario.selection_get()
                except Exception:
                    return None
                return dia_sel if isinstance(dia_sel, date) else None

            ttk.Button(
                botones_cal,
                text="Añadir selección",
                command=lambda: (_agregar_dia(d) if (d := _dia_actual()) else None),
            ).grid(row=0, column=0, padx=(0, 8))
            ttk.Button(
                botones_cal,
                text="Quitar selección",
                command=lambda: (_quitar_dia(d) if (d := _dia_actual()) else None),
            ).grid(row=0, column=1)

            ttk.Label(contenido, text="Motivo:").grid(row=3, column=0, columnspan=2, sticky="w")
            motivo_text = tk.Text(contenido, height=5, width=40)
            motivo_text.grid(row=4, column=0, columnspan=2, sticky="nsew", pady=(4, 0))

            contenido.grid_rowconfigure(4, weight=1)
            contenido.grid_columnconfigure(0, weight=1)
            contenido.grid_columnconfigure(1, weight=1)

            botones_accion = ttk.Frame(contenido)
            botones_accion.grid(row=5, column=0, columnspan=2, sticky="e", pady=(12, 0))

            def cancelar() -> None:
                cerrar()

            def aceptar() -> None:
                if not seleccionados:
                    messagebox.showwarning("Peticiones", "Selecciona al menos un día del calendario.")
                    return
                motivo = motivo_text.get("1.0", tk.END).strip()
                if not motivo:
                    messagebox.showwarning("Peticiones", "El motivo no puede estar vacío.")
                    return

                total_dias = len(seleccionados)
                if not messagebox.askokcancel(
                    "Peticiones",
                    f"Se van a crear {total_dias} peticiones de días libres para {nombre_usuario} ({dni_usuario}). ¿Desea continuar?",
                ):
                    return

                try:
                    nuevas, duplicadas = crear_peticiones_en_firestore(uid, seleccionados, motivo)
                except Exception as exc:
                    messagebox.showerror(
                        "Peticiones",
                        f"No se pudieron crear las peticiones:\n\n{exc}",
                    )
                    return

                cerrar()
                refrescar()
                messagebox.showinfo(
                    "Peticiones",
                    f"Peticiones creadas: {nuevas}\nDuplicadas: {duplicadas}\nUsuario: {nombre_usuario} ({dni_usuario})",
                )

            ttk.Button(botones_accion, text="Aceptar", command=aceptar).grid(row=0, column=0, padx=(0, 8))
            ttk.Button(botones_accion, text="Cancelar", command=cancelar).grid(row=0, column=1)

            dialogo.protocol("WM_DELETE_WINDOW", cancelar)
            motivo_text.focus_set()

            ventana.wait_window(dialogo)
        finally:
            cerrar()
            dni_dialog_abierto = False


    ventana.grid_rowconfigure(1, weight=1)
    ventana.grid_columnconfigure(0, weight=1)

    columnas_widths = {col: 110 for col in columnas}
    columnas_widths["Genero"] = 90

    frame_botones = tk.Frame(ventana)
    frame_botones.grid(row=0, column=0, sticky="ew", pady=5)

    tabla_frame = tk.Frame(ventana)
    tabla_frame.grid(row=1, column=0, sticky="nsew")
    tabla_frame.grid_rowconfigure(0, weight=1)
    tabla_frame.grid_columnconfigure(0, weight=1)

    tree = ttk.Treeview(tabla_frame, columns=columnas, show="headings", selectmode="extended")
    tree.grid(row=0, column=0, sticky="nsew")
    orden_actual = {col: None for col in columnas}

    scrollbar_y = ttk.Scrollbar(tabla_frame, orient="vertical", command=tree.yview)
    scrollbar_y.grid(row=0, column=1, sticky="ns")

    scrollbar_x = ttk.Scrollbar(tabla_frame, orient="horizontal", command=tree.xview)
    scrollbar_x.grid(row=1, column=0, sticky="ew")

    def _on_tree_xscroll(first, last):
        scrollbar_x.set(first, last)

    tree.configure(yscrollcommand=scrollbar_y.set, xscrollcommand=_on_tree_xscroll)
    tree.tag_configure("has_days_row", foreground="#b00020")
    tree.tag_configure("inactivo_3", foreground="#b35900")
    tree.tag_configure("inactivo_7", foreground="#3b0066")

    frame_status = ttk.Frame(ventana)
    frame_status.grid(row=2, column=0, sticky="ew")
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

    def _coerce_bool(value: Any) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and value in (0, 1):
            return bool(value)
        if isinstance(value, str):
            valor = value.strip().lower()
            if valor in {"true", "sí", "si", "1", "ok", "yes"}:
                return True
            if valor in {"false", "no", "0"}:
                return False
        return None

    def _coerce_number(value: Any) -> Optional[float]:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value.replace(",", "."))
            except Exception:
                return None
        return None

    def _coerce_date(value: Any, col: str) -> Optional[date]:
        if isinstance(value, dt.datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            return parse_access_date(value)
        if isinstance(value, (int, float)) and col in date_columns:
            return parse_access_date(value)
        return None

    def _row_filter_values(row: dict, col: str) -> tuple[Any, str]:
        # Critical: filters operate on display value, but we keep raw for type detection.
        raw = row.get(col, "")
        if col == "Nombre":
            display = formatear_nombre(row.get("UID", ""), raw)
        elif col == "Genero":
            display = normalize_genero(raw)
        else:
            display = raw
        if display is None or str(display).strip() == "":
            return raw, empty_filter_label
        return raw, str(display)

    def _infer_column_type(col: str) -> str:
        muestras: list[Any] = []
        for row in datos_originales:
            raw, display = _row_filter_values(row, col)
            if display == empty_filter_label:
                continue
            muestras.append(display if col == "Nombre" else raw)
            if len(muestras) >= 50:
                break
        if not muestras:
            return "text"
        if col in bool_columns and any(_coerce_bool(valor) is not None for valor in muestras):
            return "bool"
        if col in date_columns and any(_coerce_date(valor, col) is not None for valor in muestras):
            return "date"
        if all(_coerce_bool(valor) is not None for valor in muestras):
            return "bool"
        if all(_coerce_date(valor, col) is not None for valor in muestras):
            return "date"
        if all(_coerce_number(valor) is not None for valor in muestras):
            return "number"
        return "text"

    def _actualizar_tipos_columnas() -> None:
        column_types.clear()
        for col in columnas:
            column_types[col] = _infer_column_type(col)

    def _actualizar_tipo_columna(col: str) -> None:
        column_types[col] = _infer_column_type(col)

    def _sort_key_for_value(value: Any, col: str) -> tuple[int, Any]:
        if value is None:
            return (1, "")
        texto = str(value).strip()
        if texto == "" or texto == empty_filter_label:
            return (1, "")
        tipo = column_types.get(col, "text")
        if tipo == "number":
            parsed = _coerce_number(value)
        elif tipo == "date":
            parsed = _coerce_date(value, col)
        elif tipo == "bool":
            bool_val = _coerce_bool(value)
            parsed = None if bool_val is None else (1 if bool_val else 0)
        else:
            parsed = texto.lower()
        if parsed is None:
            parsed = texto.lower()
        return (0, parsed)

    def row_to_values(row: dict) -> list[str]:
        valores: list[str] = []
        for col in columnas:
            valor = row.get(col, "")
            if col == "Nombre":
                valor = formatear_nombre(row.get("UID", ""), valor)
            valores.append(valor)
        return valores

    def _row_tags(uid: str, row: dict) -> tuple[str, ...]:
        tags: list[str] = []
        if uid in upcoming_by_uid:
            tags.append("has_days_row")
        dias = dias_sin_trabajar(row.get("UltimoDia"))
        if dias > 7:
            tags.append("inactivo_7")
        elif dias > 3:
            tags.append("inactivo_3")
        return tuple(tags)

    def _apply_row_tags(uid: str, row: dict | None) -> None:
        if not row or not tree.exists(uid):
            return
        tree.item(uid, tags=_row_tags(uid, row))

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

    def manejar_click_dni(event):
        tree.after(1, actualizar_contador)
        if getattr(event, "num", 1) != 1:
            return
        if tree.identify("region", event.x, event.y) != "cell":
            return
        item_id = tree.identify_row(event.y)
        col_id = tree.identify_column(event.x)
        if not item_id or not col_id:
            return
        try:
            col_index = int(col_id.replace("#", "")) - 1
        except ValueError:
            return
        if col_index < 0 or col_index >= len(columnas):
            return
        if columnas[col_index] != "Dni":
            return
        abrir_dialogo_peticiones(rows_by_iid.get(item_id))

    def __apply_reset_in_ui(uids):
        """Actualiza la columna Mensaje a False en la UI abierta."""

        def _do():
            for uid in uids:
                if tree.exists(uid):
                    tree.set(uid, "Mensaje", "False")
                if uid in rows_by_iid:
                    rows_by_iid[uid]["Mensaje"] = "False"
                for fila in datos_originales:
                    if fila.get("UID") == uid:
                        fila["Mensaje"] = "False"
                        break
            actualizar_contador()

        try:
            ventana.after(0, _do)
        except Exception:
            _do()

    _notify_reset_cb = __apply_reset_in_ui

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

    def _actualizar_encabezados():
        for c in columnas:
            texto = encabezados.get(c, c)
            if orden_actual.get(c) == "asc":
                texto += " ▲"
            elif orden_actual.get(c) == "desc":
                texto += " ▼"
            tree.heading(c, text=texto, command=lambda c=c: ordenar_columna(c))

    def ordenar_columna(col):
        datos = [(tree.set(iid, col), iid) for iid in tree.get_children()]
        reverse = orden_actual[col] == "asc"
        datos.sort(key=lambda x: _sort_key_for_value(x[0], col), reverse=reverse)
        for idx, (val, iid) in enumerate(datos):
            tree.move(iid, '', idx)

        for c in columnas:
            if c != col:
                orden_actual[c] = None
        orden_actual[col] = "desc" if reverse else "asc"
        _actualizar_encabezados()

    def row_filter_value(row: dict, col: str) -> str:
        _, display = _row_filter_values(row, col)
        return display

    def row_matches_filter(row: dict, col: str, filtro: FilterState) -> bool:
        raw, display = _row_filter_values(row, col)
        valor_display = None if display == empty_filter_label else display
        if filtro.operator is None:
            if filtro.selected_values is None:
                return True
            return display in filtro.selected_values

        operator = filtro.operator
        if operator in {"vacías", "no vacías"}:
            esta_vacio = valor_display is None
            return esta_vacio if operator == "vacías" else not esta_vacio

        tipo = column_types.get(col, "text")
        if tipo == "text":
            if valor_display is None:
                return False
            texto = str(valor_display).casefold()
            needle = str(filtro.value1 or "").casefold()
            if operator == "contiene":
                return needle in texto
            if operator == "no contiene":
                return needle not in texto
            if operator == "empieza por":
                return texto.startswith(needle)
            if operator == "termina en":
                return texto.endswith(needle)
            if operator == "igual a":
                return texto == needle
            if operator == "distinto de":
                return texto != needle
            return True

        if tipo == "number":
            numero = _coerce_number(raw)
            if numero is None:
                return False
            valor1 = filtro.value1
            valor2 = filtro.value2
            if operator == "=":
                return numero == valor1
            if operator == "≠":
                return numero != valor1
            if operator == ">":
                return numero > valor1
            if operator == "<":
                return numero < valor1
            if operator == "≥":
                return numero >= valor1
            if operator == "≤":
                return numero <= valor1
            if operator == "entre":
                if valor1 is None or valor2 is None:
                    return False
                minimo, maximo = sorted((valor1, valor2))
                return minimo <= numero <= maximo
            return True

        if tipo == "date":
            fecha = _coerce_date(raw, col)
            if fecha is None:
                return False
            valor1 = filtro.value1
            valor2 = filtro.value2
            hoy = dt.datetime.now().date()
            if operator == "es":
                return fecha == valor1
            if operator == "antes de":
                return fecha < valor1
            if operator == "después de":
                return fecha > valor1
            if operator == "entre":
                if valor1 is None or valor2 is None:
                    return False
                minimo, maximo = sorted((valor1, valor2))
                return minimo <= fecha <= maximo
            if operator == "hoy":
                return fecha == hoy
            if operator == "este mes":
                return fecha.year == hoy.year and fecha.month == hoy.month
            return True

        if tipo == "bool":
            valor_bool = _coerce_bool(raw)
            if valor_bool is None:
                return False
            return valor_bool == filtro.value1

        return True

    def _row_matches_filters(row: dict, skip_col: str | None = None) -> bool:
        for col, estado in filtros_activos.items():
            if col == skip_col:
                continue
            if not row_matches_filter(row, col, estado):
                return False
        return True

    def _set_active_filter(col: str, filtro: FilterState) -> None:
        filtros_activos.pop(col, None)
        filtros_activos[col] = filtro

    def _valores_base(col: str) -> set[str]:
        # Excel-like: recompute values from original data with all filters except this column.
        return {
            row_filter_value(row, col)
            for row in datos_originales
            if _row_matches_filters(row, skip_col=col)
        }

    def _valores_unicos(col: str) -> list[str]:
        valores = _valores_base(col)
        seleccionados = filtros_activos.get(col)
        if seleccionados and seleccionados.selected_values is not None:
            valores.update(seleccionados.selected_values)
        return sorted(valores, key=lambda valor: _sort_key_for_value(valor, col))

    def _aplicar_orden_actual():
        col_orden = next((c for c, orden in orden_actual.items() if orden), None)
        if not col_orden:
            _actualizar_encabezados()
            return
        reverse = orden_actual[col_orden] == "desc"
        datos = [(tree.set(iid, col_orden), iid) for iid in tree.get_children()]
        datos.sort(key=lambda x: _sort_key_for_value(x[0], col_orden), reverse=reverse)
        for idx, (_val, iid) in enumerate(datos):
            tree.move(iid, "", idx)
        _actualizar_encabezados()

    def aplicar_filtros():
        _hide_cal_popup()
        tree.delete(*tree.get_children())
        rows_by_iid.clear()
        for row in datos_originales:
            if not _row_matches_filters(row):
                continue
            uid_row = row["UID"]
            valores = row_to_values(row)
            tags = _row_tags(uid_row, row)
            tree.insert("", "end", iid=uid_row, values=valores, tags=tags)
            rows_by_iid[uid_row] = row
        _aplicar_orden_actual()
        toggle_seleccionar_todos()
        ajustar_altura_tree()

    def limpiar_filtros():
        filtros_activos.clear()
        aplicar_filtros()

    filtro_popup: Optional[tk.Toplevel] = None

    def _cerrar_popup_filtros():
        nonlocal filtro_popup
        if filtro_popup and filtro_popup.winfo_exists():
            filtro_popup.destroy()
        filtro_popup = None

    def _popup_position_for_column(col_id: str) -> tuple[int, int]:
        bbox = tree.bbox("", column=col_id)
        if not bbox and tree.get_children():
            bbox = tree.bbox(tree.get_children()[0], column=col_id)
        if not bbox:
            return tree.winfo_rootx(), tree.winfo_rooty()
        x, y, width, height = bbox
        return tree.winfo_rootx() + x, tree.winfo_rooty() + y + height

    def mostrar_filtro_columna(col: str):
        nonlocal filtro_popup
        _cerrar_popup_filtros()
        popup = tk.Toplevel(ventana)
        filtro_popup = popup
        popup.title(f"Filtro - {encabezados.get(col, col)}")
        popup.transient(ventana)
        popup.resizable(False, False)
        popup.attributes("-topmost", True)

        col_id = f"#{columnas.index(col) + 1}"
        x, y = _popup_position_for_column(col_id)
        popup.geometry(f"+{x}+{y}")

        container = ttk.Frame(popup, padding=8)
        container.grid(row=0, column=0, sticky="nsew")
        popup.grid_columnconfigure(0, weight=1)

        ttk.Button(
            container,
            text="Ordenar de menor a mayor",
            command=lambda: (ordenar_columna(col), _cerrar_popup_filtros()),
        ).grid(row=0, column=0, sticky="ew", pady=(0, 4))
        ttk.Button(
            container,
            text="Ordenar de mayor a menor",
            command=lambda: (ordenar_columna(col), _cerrar_popup_filtros()),
        ).grid(row=1, column=0, sticky="ew", pady=(0, 8))

        ttk.Separator(container, orient="horizontal").grid(row=2, column=0, sticky="ew", pady=(0, 8))

        tipo_col = column_types.get(col, "text")
        estado_actual = filtros_activos.get(col)
        modo_var = tk.StringVar(
            value="condicion" if estado_actual and estado_actual.operator else "lista"
        )

        filtro_avanzado = ttk.LabelFrame(container, text="Filtro avanzado", padding=6)
        filtro_avanzado.grid(row=3, column=0, sticky="ew", pady=(0, 8))
        filtro_avanzado.grid_columnconfigure(1, weight=1)

        ttk.Radiobutton(
            filtro_avanzado,
            text="Usar lista de valores",
            variable=modo_var,
            value="lista",
        ).grid(row=0, column=0, sticky="w", columnspan=2)
        ttk.Radiobutton(
            filtro_avanzado,
            text="Usar condición",
            variable=modo_var,
            value="condicion",
        ).grid(row=1, column=0, sticky="w", columnspan=2)

        operadores_por_tipo = {
            "text": [
                "contiene",
                "no contiene",
                "empieza por",
                "termina en",
                "igual a",
                "distinto de",
                "vacías",
                "no vacías",
            ],
            "number": ["=", "≠", ">", "<", "≥", "≤", "entre", "vacías", "no vacías"],
            "date": [
                "es",
                "antes de",
                "después de",
                "entre",
                "hoy",
                "este mes",
                "vacías",
                "no vacías",
            ],
        }

        operator_var = tk.StringVar()
        operator_combo: Optional[ttk.Combobox] = None
        value1_var = tk.StringVar()
        value2_var = tk.StringVar()
        value1_entry: Optional[ttk.Entry] = None
        value2_entry: Optional[ttk.Entry] = None
        bool_var = tk.StringVar(value="True")
        date_popups: Dict[str, tk.Toplevel] = {}

        if tipo_col == "bool":
            ttk.Label(filtro_avanzado, text="Valor:").grid(row=2, column=0, sticky="w")
            bool_frame = ttk.Frame(filtro_avanzado)
            bool_frame.grid(row=2, column=1, sticky="w")
            ttk.Radiobutton(
                bool_frame, text="True", variable=bool_var, value="True"
            ).pack(side="left")
            ttk.Radiobutton(
                bool_frame, text="False", variable=bool_var, value="False"
            ).pack(side="left", padx=(6, 0))
        else:
            ttk.Label(filtro_avanzado, text="Operador:").grid(row=2, column=0, sticky="w")
            operator_combo = ttk.Combobox(
                filtro_avanzado,
                textvariable=operator_var,
                values=operadores_por_tipo.get(tipo_col, operadores_por_tipo["text"]),
                state="readonly",
            )
            operator_combo.grid(row=2, column=1, sticky="ew")

            ttk.Label(filtro_avanzado, text="Valor:").grid(row=3, column=0, sticky="w")
            if tipo_col == "date":
                def _cerrar_cal_popup(key: str) -> None:
                    popup_cal = date_popups.pop(key, None)
                    if popup_cal and popup_cal.winfo_exists():
                        popup_cal.destroy()

                def _abrir_calendario(entry: ttk.Entry, var: tk.StringVar, key: str) -> None:
                    if entry.cget("state") == "disabled":
                        return
                    _cerrar_cal_popup(key)
                    cal_popup = tk.Toplevel(popup)
                    date_popups[key] = cal_popup
                    cal_popup.transient(popup)
                    cal_popup.resizable(False, False)
                    try:
                        cal_popup.attributes("-topmost", True)
                    except Exception:
                        pass
                    x = entry.winfo_rootx()
                    y = entry.winfo_rooty() + entry.winfo_height()
                    cal_popup.geometry(f"+{x}+{y}")

                    fecha_inicial = parse_access_date(var.get())
                    cal_kwargs = {
                        "selectmode": "day",
                        "firstweekday": "monday",
                        "showweeknumbers": False,
                    }
                    try:
                        if isinstance(fecha_inicial, date):
                            calendario = Calendar(
                                cal_popup,
                                locale="es_ES",
                                year=fecha_inicial.year,
                                month=fecha_inicial.month,
                                day=fecha_inicial.day,
                                **cal_kwargs,
                            )
                        else:
                            calendario = Calendar(cal_popup, locale="es_ES", **cal_kwargs)
                    except Exception:
                        if isinstance(fecha_inicial, date):
                            calendario = Calendar(
                                cal_popup,
                                year=fecha_inicial.year,
                                month=fecha_inicial.month,
                                day=fecha_inicial.day,
                                **cal_kwargs,
                            )
                        else:
                            calendario = Calendar(cal_popup, **cal_kwargs)
                    calendario.pack()

                    def _seleccionar_fecha(_event=None) -> None:
                        try:
                            seleccion = calendario.selection_get()
                        except Exception:
                            return
                        if not isinstance(seleccion, date):
                            return
                        var.set(fmt_dmy(seleccion) or "")
                        _cerrar_cal_popup(key)

                    calendario.bind("<<CalendarSelected>>", _seleccionar_fecha)
                    cal_popup.bind("<Escape>", lambda _e: _cerrar_cal_popup(key))

                    def _cerrar_si_fuera() -> None:
                        if cal_popup.winfo_exists() and cal_popup.focus_displayof() is None:
                            _cerrar_cal_popup(key)

                    cal_popup.bind("<FocusOut>", lambda _e: cal_popup.after(50, _cerrar_si_fuera))

                value1_entry = ttk.Entry(filtro_avanzado, textvariable=value1_var, state="readonly")
                value1_entry.bind(
                    "<Button-1>",
                    lambda _e, entry=value1_entry: _abrir_calendario(entry, value1_var, "value1"),
                )
                value1_entry.bind(
                    "<Return>",
                    lambda _e, entry=value1_entry: _abrir_calendario(entry, value1_var, "value1"),
                )
            else:
                value1_entry = ttk.Entry(filtro_avanzado, textvariable=value1_var)
            value1_entry.grid(row=3, column=1, sticky="ew")

            ttk.Label(filtro_avanzado, text="Hasta:").grid(row=4, column=0, sticky="w")
            if tipo_col == "date":
                value2_entry = ttk.Entry(filtro_avanzado, textvariable=value2_var, state="readonly")
                value2_entry.bind(
                    "<Button-1>",
                    lambda _e, entry=value2_entry: _abrir_calendario(entry, value2_var, "value2"),
                )
                value2_entry.bind(
                    "<Return>",
                    lambda _e, entry=value2_entry: _abrir_calendario(entry, value2_var, "value2"),
                )
            else:
                value2_entry = ttk.Entry(filtro_avanzado, textvariable=value2_var)
            value2_entry.grid(row=4, column=1, sticky="ew")

        def _formatear_valor_filtro(valor: Any) -> str:
            if isinstance(valor, date):
                return fmt_dmy(valor) or ""
            return "" if valor is None else str(valor)

        if estado_actual and estado_actual.operator:
            if tipo_col == "bool":
                bool_var.set("True" if estado_actual.value1 else "False")
            else:
                operator_var.set(estado_actual.operator)
                if estado_actual.value1 is not None:
                    value1_var.set(_formatear_valor_filtro(estado_actual.value1))
                if estado_actual.value2 is not None:
                    value2_var.set(_formatear_valor_filtro(estado_actual.value2))
        else:
            if operator_combo is not None:
                operator_var.set(operadores_por_tipo.get(tipo_col, ["contiene"])[0])

        def _set_entry_state(entry: Optional[ttk.Entry], enabled: bool) -> None:
            if entry is None:
                return
            if tipo_col == "date":
                entry.configure(state="readonly" if enabled else "disabled")
            else:
                entry.configure(state="normal" if enabled else "disabled")

        def _actualizar_visibilidad_operador(*_):
            modo_condicion = modo_var.get() == "condicion"
            widgets = []
            if operator_combo is not None:
                widgets.append(operator_combo)
            if value1_entry is not None and tipo_col != "date":
                widgets.append(value1_entry)
            if value2_entry is not None and tipo_col != "date":
                widgets.append(value2_entry)
            for widget in widgets:
                widget.configure(state="normal" if modo_condicion else "disabled")
            if tipo_col == "bool":
                for child in filtro_avanzado.winfo_children():
                    if isinstance(child, ttk.Frame):
                        state = "normal" if modo_condicion else "disabled"
                        for grand in child.winfo_children():
                            grand.configure(state=state)

            if not modo_condicion:
                if value1_entry is not None:
                    _set_entry_state(value1_entry, False)
                if value2_entry is not None:
                    _set_entry_state(value2_entry, False)
                return

            operator = operator_var.get()
            if operator in {"vacías", "no vacías", "hoy", "este mes"}:
                value1_var.set("")
                value2_var.set("")
            elif operator != "entre":
                value2_var.set("")
            if value1_entry is not None:
                _set_entry_state(
                    value1_entry,
                    operator not in {"vacías", "no vacías", "hoy", "este mes"},
                )
            if value2_entry is not None:
                _set_entry_state(value2_entry, operator == "entre")

        if operator_combo is not None:
            operator_combo.bind("<<ComboboxSelected>>", _actualizar_visibilidad_operador)
        modo_var.trace_add("write", _actualizar_visibilidad_operador)
        _actualizar_visibilidad_operador()

        ttk.Label(container, text="Buscar:").grid(row=4, column=0, sticky="w")
        busqueda_var = tk.StringVar()
        entrada_busqueda = ttk.Entry(container, textvariable=busqueda_var)
        entrada_busqueda.grid(row=5, column=0, sticky="ew", pady=(0, 6))

        valores_vars: Dict[str, tk.BooleanVar] = {}
        seleccion_actual = filtros_activos.get(col)
        seleccionar_todo_var = tk.BooleanVar(value=False)

        def _asegurar_valores_vars(valores: set[str]) -> None:
            for valor in valores:
                if valor in valores_vars:
                    continue
                if seleccion_actual is None or seleccion_actual.selected_values is None:
                    seleccionado = True
                else:
                    seleccionado = valor in seleccion_actual.selected_values
                valores_vars[valor] = tk.BooleanVar(value=seleccionado)

        def _filtro_local() -> Optional[FilterState]:
            if modo_var.get() != "condicion":
                return None
            if tipo_col == "bool":
                return FilterState(selected_values=None, operator="bool", value1=bool_var.get() == "True")
            operador = operator_var.get()
            if not operador:
                return None
            if operador in {"vacías", "no vacías", "hoy", "este mes"}:
                return FilterState(selected_values=None, operator=operador, value1=None, value2=None)
            valor1_raw = value1_var.get().strip()
            if tipo_col == "number":
                valor1 = _coerce_number(valor1_raw)
            elif tipo_col == "date":
                valor1 = parse_access_date(valor1_raw)
            else:
                valor1 = valor1_raw
            if valor1 is None or valor1_raw == "":
                return None
            valor2 = None
            if operador == "entre":
                valor2_raw = value2_var.get().strip()
                if tipo_col == "number":
                    valor2 = _coerce_number(valor2_raw)
                elif tipo_col == "date":
                    valor2 = parse_access_date(valor2_raw)
                else:
                    valor2 = valor2_raw
                if valor2 is None or valor2_raw == "":
                    return None
            return FilterState(selected_values=None, operator=operador, value1=valor1, value2=valor2)

        def _valor_cumple_condicion(valor: str, filtro: FilterState) -> bool:
            if valor == empty_filter_label:
                if filtro.operator == "vacías":
                    return True
                if filtro.operator == "no vacías":
                    return False
                return False
            if filtro.operator in {"vacías", "no vacías"}:
                return filtro.operator == "no vacías"
            tipo = column_types.get(col, "text")
            if tipo == "text":
                texto = str(valor).casefold()
                needle = str(filtro.value1 or "").casefold()
                if filtro.operator == "contiene":
                    return needle in texto
                if filtro.operator == "no contiene":
                    return needle not in texto
                if filtro.operator == "empieza por":
                    return texto.startswith(needle)
                if filtro.operator == "termina en":
                    return texto.endswith(needle)
                if filtro.operator == "igual a":
                    return texto == needle
                if filtro.operator == "distinto de":
                    return texto != needle
                return True
            if tipo == "number":
                numero = _coerce_number(valor)
                if numero is None:
                    return False
                if filtro.operator == "=":
                    return numero == filtro.value1
                if filtro.operator == "≠":
                    return numero != filtro.value1
                if filtro.operator == ">":
                    return numero > filtro.value1
                if filtro.operator == "<":
                    return numero < filtro.value1
                if filtro.operator == "≥":
                    return numero >= filtro.value1
                if filtro.operator == "≤":
                    return numero <= filtro.value1
                if filtro.operator == "entre":
                    if filtro.value1 is None or filtro.value2 is None:
                        return False
                    minimo, maximo = sorted((filtro.value1, filtro.value2))
                    return minimo <= numero <= maximo
                return True
            if tipo == "date":
                fecha = parse_access_date(valor)
                if fecha is None:
                    return False
                hoy = dt.datetime.now().date()
                if filtro.operator == "es":
                    return fecha == filtro.value1
                if filtro.operator == "antes de":
                    return fecha < filtro.value1
                if filtro.operator == "después de":
                    return fecha > filtro.value1
                if filtro.operator == "entre":
                    if filtro.value1 is None or filtro.value2 is None:
                        return False
                    minimo, maximo = sorted((filtro.value1, filtro.value2))
                    return minimo <= fecha <= maximo
                if filtro.operator == "hoy":
                    return fecha == hoy
                if filtro.operator == "este mes":
                    return fecha.year == hoy.year and fecha.month == hoy.month
                return True
            if tipo == "bool":
                valor_bool = _coerce_bool(valor)
                if valor_bool is None:
                    return False
                return valor_bool == filtro.value1
            return True

        def _valores_filtrados():
            termino = busqueda_var.get().strip().lower()
            valores_base = set(_valores_base(col))
            if seleccion_actual and seleccion_actual.selected_values is not None:
                valores_base.update(seleccion_actual.selected_values)
            _asegurar_valores_vars(valores_base)
            filtro = _filtro_local()
            if filtro:
                valores_base = {valor for valor in valores_base if _valor_cumple_condicion(valor, filtro)}
            valores_ordenados = sorted(valores_base, key=lambda valor: _sort_key_for_value(valor, col))
            if not termino:
                return valores_ordenados
            return [valor for valor in valores_ordenados if termino in str(valor).lower()]

        def _actualizar_estado_seleccionar_todo():
            valores_visibles = _valores_filtrados()
            if not valores_visibles:
                seleccionar_todo_var.set(True)
                return
            seleccionar_todo_var.set(all(valores_vars[valor].get() for valor in valores_visibles))

        def _toggle_seleccionar_todo():
            marcado = seleccionar_todo_var.get()
            for valor in _valores_filtrados():
                valores_vars[valor].set(marcado)

        def _seleccionar_resultados_busqueda():
            visibles = set(_valores_filtrados())
            for valor in valores_vars:
                valores_vars[valor].set(valor in visibles)
            _actualizar_estado_seleccionar_todo()

        seleccionar_todo_cb = ttk.Checkbutton(
            container,
            text="Seleccionar todo",
            variable=seleccionar_todo_var,
            command=_toggle_seleccionar_todo,
        )
        seleccionar_todo_cb.grid(row=6, column=0, sticky="w", pady=(0, 4))

        ttk.Button(
            container,
            text="Seleccionar resultados de la búsqueda",
            command=_seleccionar_resultados_busqueda,
        ).grid(row=7, column=0, sticky="w", pady=(0, 6))

        lista_frame = ttk.Frame(container)
        lista_frame.grid(row=8, column=0, sticky="nsew")
        lista_canvas = tk.Canvas(lista_frame, height=200, highlightthickness=0)
        scrollbar_vals = ttk.Scrollbar(lista_frame, orient="vertical", command=lista_canvas.yview)
        lista_canvas.configure(yscrollcommand=scrollbar_vals.set)
        scrollbar_vals.grid(row=0, column=1, sticky="ns")
        lista_canvas.grid(row=0, column=0, sticky="nsew")
        lista_frame.grid_rowconfigure(0, weight=1)
        lista_frame.grid_columnconfigure(0, weight=1)

        valores_inner = ttk.Frame(lista_canvas)
        ventana_vals = lista_canvas.create_window((0, 0), window=valores_inner, anchor="nw")

        def _actualizar_scrollregion(_event=None):
            lista_canvas.configure(scrollregion=lista_canvas.bbox("all"))

        valores_inner.bind("<Configure>", _actualizar_scrollregion)

        def _renderizar_lista():
            for child in valores_inner.winfo_children():
                child.destroy()
            for valor in _valores_filtrados():
                cb = ttk.Checkbutton(
                    valores_inner,
                    text=str(valor),
                    variable=valores_vars[valor],
                    command=_actualizar_estado_seleccionar_todo,
                )
                cb.pack(anchor="w")
            _actualizar_estado_seleccionar_todo()
            lista_canvas.update_idletasks()
            lista_canvas.configure(scrollregion=lista_canvas.bbox("all"))
            lista_canvas.itemconfigure(ventana_vals, width=lista_canvas.winfo_width())

        def _on_canvas_configure(event):
            lista_canvas.itemconfigure(ventana_vals, width=event.width)

        lista_canvas.bind("<Configure>", _on_canvas_configure)
        busqueda_var.trace_add("write", lambda *_: _renderizar_lista())
        modo_var.trace_add("write", lambda *_: _renderizar_lista())
        operator_var.trace_add("write", lambda *_: _renderizar_lista())
        value1_var.trace_add("write", lambda *_: _renderizar_lista())
        value2_var.trace_add("write", lambda *_: _renderizar_lista())
        bool_var.trace_add("write", lambda *_: _renderizar_lista())
        _renderizar_lista()

        botones = ttk.Frame(container)
        botones.grid(row=9, column=0, sticky="e", pady=(8, 0))

        def _aceptar():
            seleccionados = {val for val, var in valores_vars.items() if var.get()}
            _set_active_filter(col, FilterState(selected_values=seleccionados))
            aplicar_filtros()
            _cerrar_popup_filtros()

        def _cancelar():
            _cerrar_popup_filtros()

        ttk.Button(botones, text="Aceptar", command=_aceptar).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(botones, text="Cancelar", command=_cancelar).grid(row=0, column=1)

        def _cerrar_si_fuera():
            if popup.winfo_exists() and popup.focus_displayof() is None:
                _cerrar_popup_filtros()

        popup.bind("<FocusOut>", lambda _e: popup.after(50, _cerrar_si_fuera))
        popup.bind("<Escape>", lambda _e: _cerrar_popup_filtros())
        entrada_busqueda.focus_set()


    for col in columnas:
        texto_col = encabezados.get(col, col)
        tree.heading(col, text=texto_col, command=lambda c=col: ordenar_columna(c))
        tree.column(col, anchor="center", width=columnas_widths.get(col, 110))

    if "Genero" in columnas:
        tree.heading("Genero", text="Género", command=lambda c="Genero": ordenar_columna(c))
        tree.column("Genero", width=columnas_widths.get("Genero", 90), anchor="center")

    def _on_heading_right_click(event):
        region = tree.identify("region", event.x, event.y)
        if region != "heading":
            return
        col_id = tree.identify_column(event.x)
        if not col_id:
            return
        try:
            col_index = int(col_id.replace("#", "")) - 1
        except ValueError:
            return
        if 0 <= col_index < len(columnas):
            mostrar_filtro_columna(columnas[col_index])

    tree.bind("<Button-3>", _on_heading_right_click)

    seleccionar_todos_var = tk.BooleanVar(value=False)

    def toggle_seleccionar_todos():
        if seleccionar_todos_var.get():
            tree.selection_set(tree.get_children())
        else:
            tree.selection_remove(tree.get_children())
        actualizar_contador()

    def guardar_dato(uid, campo, valor):
        try:
            doc_ref = db.collection("UsuariosAutorizados").document(uid)
            if campo in ["Mensaje", "Seleccionable", "Valor"]:
                valor = valor == "True"
            elif campo in ["TotalDia", "TotalDiaMesActual", "TotalDiaSemanaActual"]:
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

        if col_nombre == "Dni":
            abrir_dialogo_peticiones(rows_by_iid.get(item_id))
            return

        if col_nombre in ["Mensaje", "Seleccionable", "Valor"]:
            val = tree.set(item_id, col_nombre)
            nuevo = "False" if val == "True" else "True"
            tree.set(item_id, col_nombre, nuevo)
            guardar_dato(item_id, col_nombre, nuevo)
            for fila in datos_originales:
                if fila["UID"] == item_id:
                    fila[col_nombre] = nuevo
                    break
            _actualizar_tipo_columna(col_nombre)
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
                    if col_nombre == "UltimoDia":
                        _apply_row_tags(item_id, rows_by_iid[item_id])
                _actualizar_tipo_columna(col_nombre)
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
                raw_total_dia_mes_actual = data.get("TotalDiaMesActual")
                raw_total_dia_semana_actual = data.get("TotalDiaSemanaActual")
                raw_total_dia_mes_actual_str = s_trim(raw_total_dia_mes_actual)
                raw_total_dia_semana_actual_str = s_trim(raw_total_dia_semana_actual)
                total_dia_mes_actual_actual = _to_int_safe(raw_total_dia_mes_actual)
                total_dia_semana_actual_actual = _to_int_safe(raw_total_dia_semana_actual)
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
                total_dia_mes_actual_calc = _to_int_safe(totales_info.get("total_dia_mes_actual"))
                total_dia_semana_actual_calc = _to_int_safe(totales_info.get("total_dia_semana_actual"))
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
                    "TotalDiaMesActual": int(total_dia_mes_actual_calc),
                    "TotalDiaSemanaActual": int(total_dia_semana_actual_calc),
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

                if (
                    updates["TotalDiaMesActual"] != total_dia_mes_actual_actual
                    or raw_total_dia_mes_actual is None
                    or raw_total_dia_mes_actual_str == ""
                ):
                    actualiza["TotalDiaMesActual"] = updates["TotalDiaMesActual"]
                data["TotalDiaMesActual"] = updates["TotalDiaMesActual"]

                if (
                    updates["TotalDiaSemanaActual"] != total_dia_semana_actual_actual
                    or raw_total_dia_semana_actual is None
                    or raw_total_dia_semana_actual_str == ""
                ):
                    actualiza["TotalDiaSemanaActual"] = updates["TotalDiaSemanaActual"]
                data["TotalDiaSemanaActual"] = updates["TotalDiaSemanaActual"]

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
                data["TotalDiaMesActual"] = str(total_dia_mes_actual_calc)
                data["TotalDiaSemanaActual"] = str(total_dia_semana_actual_calc)
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
            tags = _row_tags(uid, fila)
            tree.insert("", "end", iid=uid, values=valores, tags=tags)
            if idx % 200 == 0:
                print(f"Procesados {idx}/{total}")
        if ops % 400:
            batch.commit()
        t5 = time.time()

        _actualizar_tipos_columnas()
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

            for campo in ["TotalDia", "TotalDiaMesActual", "TotalDiaSemanaActual"]:
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

            hay_filtros = bool(filtros_activos)

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

    tk.Button(frame_botones, text="🧹 Limpiar filtros", command=limpiar_filtros).pack(side="left", padx=10)
    tk.Checkbutton(frame_botones, text="Seleccionar Todos", variable=seleccionar_todos_var, command=toggle_seleccionar_todos).pack(side="left", padx=10)
    tk.Button(frame_botones, text="Mensaje", command=toggle_mensaje).pack(side="left", padx=10)
    tk.Button(frame_botones, text="🗑 Eliminar seleccionado", bg="salmon", command=eliminar_usuario).pack(side="left", padx=10)
    tk.Button(frame_botones, text="💾 Guardar todo", bg="lightgreen", command=guardar_todo).pack(side="left", padx=10)

    def on_close():
        global ventana_usuarios, _notify_reset_cb
        _notify_reset_cb = None
        ventana_usuarios = None
        ventana.destroy()

    ventana.protocol("WM_DELETE_WINDOW", on_close)

    tree.bind("<Double-1>", editar_celda)
    tree.bind("<<TreeviewSelect>>", actualizar_contador)
    tree.bind("<ButtonRelease-1>", manejar_click_dni)
    tree.bind("<Motion>", _hover_calendar)
    tree.bind("<Leave>", lambda e: _hide_cal_popup())
    tree.bind("<ButtonPress-1>", lambda e: _hide_cal_popup())
    tree.bind("<MouseWheel>", lambda e: _hide_cal_popup())
    ventana.bind("<Configure>", ajustar_altura_tree)
    ventana.bind("<F5>", lambda e: refrescar())
    ventana.bind("<Control-r>", lambda e: refrescar())

    refrescar()

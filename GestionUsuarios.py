import tkinter as tk
from tkinter import ttk, messagebox
import pyodbc
import os
from firebase_admin import auth
import datetime as dt
from datetime import datetime, date, timedelta
import datetime
import re
from decimal import Decimal
from typing import Optional, Union, Dict, Iterable, Tuple
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import time


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


def to_date(x):
    if x is None or x == "":
        return None
    try:
        if hasattr(x, "date"):
            return x.date()
    except Exception:
        return None
    if isinstance(x, datetime.date):
        return x
    if isinstance(x, datetime.datetime):
        return x.date()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.datetime.strptime(s(x), fmt).date()
        except ValueError:
            pass
    return None


def fmt_dmy(d):
    return d.strftime("%d-%m-%Y") if isinstance(d, date) else None


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
    columnas = "DNI, CODIGO, FECHAALTA, FECHABAJA, APELLIDOS, APELLIDOS2, NOMBRE"
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
    total_dia = 0
    total_horas = 0.0

    row = cursor.execute(
        "SELECT FECHAALTA, FECHABAJA FROM TRABAJADORES WHERE DNI = ?",
        (dni,)
    ).fetchone()
    if row:
        fecha_alta = to_date(getattr(row, 'FECHAALTA', None))
        fecha_baja = to_date(getattr(row, 'FECHABAJA', None))
        baja_str = fmt_dmy(fecha_baja)

    if fecha_baja:
        return baja_str, 0, 0.0, fecha_alta, fecha_baja

    if not fecha_alta:
        return baja_str, 0, 0.0, fecha_alta, fecha_baja

    filas = cursor.execute(
        "SELECT FECHA, HORAS, HORASEXT FROM DATOS_AJUSTADOS WHERE DNI = ? AND FECHA >= ?",
        (dni, fecha_alta)
    ).fetchall()

    fechas = set()
    horas_acum = 0.0
    for fila in filas:
        fecha_reg = to_date(getattr(fila, 'FECHA', None))
        if fecha_reg:
            fechas.add(fecha_reg)
        horas_acum += _to_float_safe(getattr(fila, 'HORAS', 0))
        horas_acum += _to_float_safe(getattr(fila, 'HORASEXT', 0))

    total_dia = len(fechas)
    total_horas = round(horas_acum, 2)

    if not filas:
        total_dia = 0
        total_horas = 0.0

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

    columnas = ["Dni", "Nombre", "Telefono", "correo", "Puesto", "Turno", "Cultivo",
                "Mensaje", "Seleccionable", "Valor", "Alta", "UltimoDia", "TotalDia", "TotalHoras", "Baja", "Codigo"]

    encabezados = {
        "Dni": "Dni", "Nombre": "Nombre", "Telefono": "Teléfono", "correo": "Correo",
        "Puesto": "Puesto", "Turno": "Turno", "Cultivo": "Cultivo",
        "Mensaje": "Mensaje", "Seleccionable": "Seleccionable", "Valor": "Valor",
        "Alta": "Alta", "UltimoDia": "Último Día", "TotalDia": "Total Día",
        "TotalHoras": "Total Horas", "Baja": "Baja", "Codigo": "Código"
    }

    datos_originales = []
    entradas_filtro = {}

    frame_filtros = tk.Frame(ventana)
    frame_filtros.pack(fill="x")

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
    frame_botones.pack(pady=5)

    tabla_frame = tk.Frame(ventana)
    tabla_frame.pack(fill="both", expand=True)

    canvas = tk.Canvas(tabla_frame)
    canvas.pack(side="left", fill="both", expand=True)

    scrollbar_y = ttk.Scrollbar(tabla_frame, orient="vertical", command=canvas.yview)
    scrollbar_y.pack(side="right", fill="y")

    tabla_canvas_frame = tk.Frame(canvas)
    canvas.create_window((0, 0), window=tabla_canvas_frame, anchor="nw")

    tabla = ttk.Treeview(tabla_canvas_frame, columns=columnas, show="headings", selectmode="extended")
    tabla.grid(row=0, column=0, sticky="nsew")
    orden_actual = {col: None for col in columnas}

    scrollbar_x = ttk.Scrollbar(ventana, orient="horizontal", command=canvas.xview)
    scrollbar_x.pack(side="bottom", fill="x")
    canvas.configure(yscrollcommand=scrollbar_y.set, xscrollcommand=scrollbar_x.set)

    tabla_canvas_frame.grid_rowconfigure(0, weight=1)
    tabla_canvas_frame.grid_columnconfigure(0, weight=1)
    def ordenar_columna(col):
        datos = [(tabla.set(iid, col), iid) for iid in tabla.get_children()]
        reverse = orden_actual[col] == "asc"

        def convertir(valor):
            try:
                return float(valor)
            except:
                try:
                    return datetime.strptime(valor, "%d-%m-%Y")
                except:
                    return valor.lower()

        datos.sort(key=lambda x: convertir(x[0]), reverse=reverse)
        for idx, (val, iid) in enumerate(datos):
            tabla.move(iid, '', idx)

        orden_actual[col] = "desc" if reverse else "asc"

        # Actualiza encabezados visualmente con la flecha
        for c in columnas:
            texto = encabezados.get(c, c)
            if c == col:
                texto += " ▲" if not reverse else " ▼"
            tabla.heading(c, text=texto, command=lambda c=c: ordenar_columna(c))

    
    def actualizar_scroll(event=None):
        canvas.configure(scrollregion=canvas.bbox("all"))

    canvas.bind("<Configure>", actualizar_scroll)

    # Y reemplaza el bucle:
    for col in columnas:
        texto_col = encabezados.get(col, col)
        tabla.heading(col, text=texto_col, command=lambda c=col: ordenar_columna(c))
        tabla.column(col, anchor="center", width=110)

    seleccionar_todos_var = tk.BooleanVar(value=False)

    def toggle_seleccionar_todos():
        if seleccionar_todos_var.get():
            tabla.selection_set(tabla.get_children())
        else:
            tabla.selection_remove(tabla.get_children())

    def aplicar_filtros():
        tabla.delete(*tabla.get_children())
        criterios = {col: entradas_filtro[col].get().strip().lower() for col in columnas}
        for row in datos_originales:
            visible = True
            for col in columnas:
                valor = str(row.get(col, "")).lower().strip()
                if criterios[col] and criterios[col] not in valor:
                    visible = False
                    break
            if visible:
                tabla.insert("", "end", iid=row["UID"], values=[row.get(c, "") for c in columnas])
        toggle_seleccionar_todos()

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
            elif campo in ["Alta", "UltimoDia", "Baja", "Codigo"]:
                valor = s_trim(valor)
            doc_ref.update({campo: valor})
        except Exception as e:
            print(f"⚠️ Error al guardar {campo} de {uid}: {e}")

    def editar_celda(event):
        item_id = tabla.focus()
        if not item_id:
            return
        col = tabla.identify_column(event.x)
        col_index = int(col.replace("#", "")) - 1
        col_nombre = columnas[col_index]

        if col_nombre in ["Mensaje", "Seleccionable", "Valor"]:
            val = tabla.set(item_id, col_nombre)
            nuevo = "False" if val == "True" else "True"
            tabla.set(item_id, col_nombre, nuevo)
            guardar_dato(item_id, col_nombre, nuevo)
        else:
            x, y, width, height = tabla.bbox(item_id, column=col)
            valor_actual = tabla.set(item_id, col_nombre)
            entry = tk.Entry(tabla)
            entry.insert(0, valor_actual)
            entry.place(x=x, y=y, width=width, height=height)
            entry.focus()

            def guardar_valor(event=None):
                nuevo_valor = entry.get()
                tabla.set(item_id, col_nombre, nuevo_valor)
                guardar_dato(item_id, col_nombre, nuevo_valor)
                entry.destroy()

            entry.bind("<Return>", guardar_valor)
            entry.bind("<FocusOut>", guardar_valor)

    def cargar_datos():
        nonlocal datos_originales
        datos_originales = []
        tabla.delete(*tabla.get_children())

        t0 = time.time()
        usuarios_docs = list(db.collection("UsuariosAutorizados").stream())
        t1 = time.time()

        hoy = dt.datetime.now().date()
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

                total_dia_actual = _to_int_safe(data.get("TotalDia"))
                total_horas_actual = round(_to_float_safe(data.get("TotalHoras")), 2)
                baja_actual_norm = _normalize_optional_str(data.get("Baja"))

                if dni_normalizado:
                    trab = trab_by_dni.get(dni_normalizado, {})
                    if trab:
                        for campo in ("Nombre", "Alta", "Codigo"):
                            val = trab.get(campo)
                            if val and val != data.get(campo):
                                actualiza[campo] = val
                                data[campo] = val
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
                total_horas_calculado = round(_to_float_safe(totales_info.get("total_horas")), 2)
                fecha_alta_calc = totales_info.get("fecha_alta")

                if fecha_alta_calc and not s_trim(data.get("Alta")):
                    alta_calc_str = _date_to_str_ddmmyyyy(fecha_alta_calc)
                    if alta_calc_str:
                        actualiza["Alta"] = alta_calc_str
                        data["Alta"] = alta_calc_str

                if baja_str != baja_actual_norm:
                    actualiza["Baja"] = baja_str
                data["Baja"] = baja_str

                if total_dia_calculado != total_dia_actual:
                    actualiza["TotalDia"] = total_dia_calculado
                data["TotalDia"] = total_dia_calculado

                if total_horas_calculado != total_horas_actual:
                    actualiza["TotalHoras"] = total_horas_calculado
                data["TotalHoras"] = total_horas_calculado

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

                data["Nombre"] = data.get("Nombre", "Falta")
                data["Telefono"] = data.get("Telefono", "")
                data["correo"] = data.get("correo", "")
                data["Puesto"] = data.get("Puesto", "Falta")
                data["Turno"] = str(data.get("Turno", "1"))
                data["Cultivo"] = data.get("Cultivo", "Falta")
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
            tabla.insert("", "end", iid=uid, values=[fila[col] for col in columnas])
            if idx % 200 == 0:
                print(f"Procesados {idx}/{total}")
        if ops % 400:
            batch.commit()
        t5 = time.time()

        print(
            f"⏱️ t0→t1 Firebase {t1 - t0:.2f}s | t1→t2 TRAB {t2 - t1:.2f}s | "
            f"t2→t3 AJUST {t3 - t2:.2f}s | t3→t4 proc {t4 - t3:.2f}s | "
            f"t4→t5 commit {t5 - t4:.2f}s | total {t5 - t0:.2f}s"
        )

    def toggle_mensaje():
        seleccion = tabla.selection()
        if not seleccion:
            messagebox.showwarning("⚠️ Selección", "Selecciona uno o más usuarios.")
            return
        for uid in seleccion:
            valor_actual = tabla.set(uid, "Mensaje")
            nuevo_valor = "False" if valor_actual == "True" else "True"
            tabla.set(uid, "Mensaje", nuevo_valor)
            guardar_dato(uid, "Mensaje", nuevo_valor)
            for fila in datos_originales:
                if fila["UID"] == uid:
                    fila["Mensaje"] = nuevo_valor
                    break

    def eliminar_usuario():
        seleccion = tabla.focus()
        if not seleccion:
            messagebox.showwarning("⚠️ Selección", "Selecciona un usuario para eliminar.")
            return

        uid = seleccion
        nombre = tabla.set(uid, "Nombre")

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
        for item in tabla.get_children():
            valores = tabla.item(item, "values")
            uid = item
            datos = dict(zip(columnas, valores))

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

            for campo in ["Alta", "UltimoDia", "Baja", "Codigo"]:
                datos[campo] = s_trim(datos.get(campo))

            try:
                db.collection("UsuariosAutorizados").document(uid).update(datos)
            except Exception as e:
                print(f"⚠️ Error guardando {uid}: {e}")

        messagebox.showinfo("✅ Guardado", "Todos los cambios han sido guardados en Firebase.")

    tk.Button(frame_botones, text="🔍 Filtrar", command=aplicar_filtros).pack(side="left", padx=10)
    tk.Button(frame_botones, text="🧹 Limpiar", command=limpiar_filtros).pack(side="left", padx=10)
    tk.Checkbutton(frame_botones, text="Seleccionar Todos", variable=seleccionar_todos_var, command=toggle_seleccionar_todos).pack(side="left", padx=10)
    tk.Button(frame_botones, text="Mensaje", command=toggle_mensaje).pack(side="left", padx=10)
    tk.Button(frame_botones, text="🗑 Eliminar seleccionado", bg="salmon", command=eliminar_usuario).pack(side="left", padx=10)
    tk.Button(frame_botones, text="💾 Guardar todo", bg="lightgreen", command=guardar_todo).pack(side="left", padx=10)

    tabla.bind("<Double-1>", editar_celda)
    cargar_datos()

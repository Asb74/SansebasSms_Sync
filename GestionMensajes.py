import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import csv
from datetime import datetime, date, timedelta
import time
from typing import Dict, Tuple
import re

try:
    from tkcalendar import DateEntry
except Exception:  # tkcalendar no disponible
    DateEntry = None  # type: ignore

from firebase_admin import firestore

nombre_cache: Dict[str, str] = {}

# Ventana principal de gestión de mensajes (singleton)
ventana_mensajes = None


def sanitize_filename(s: str) -> str:
    return re.sub(r'[\\/*?:"<>|]+', " ", s).strip()


def start_end_of_day(d: date) -> Tuple[datetime, datetime]:
    inicio = datetime(d.year, d.month, d.day)
    fin = inicio + timedelta(days=1)
    return inicio, fin


def formatea_fecha(valor) -> str:
    try:
        if hasattr(valor, "to_datetime"):
            valor = valor.to_datetime()
        if isinstance(valor, datetime):
            if valor.tzinfo is not None:
                valor = valor.astimezone().replace(tzinfo=None)
            return valor.strftime("%d-%m-%Y %H:%M")
    except Exception:
        pass
    return ""


def fetch_nombre(uid: str, db: firestore.Client) -> str:
    if not uid:
        return "Falta"
    if uid in nombre_cache:
        return nombre_cache[uid]
    nombre = "Falta"
    try:
        doc = db.collection("UsuariosAutorizados").document(uid).get()
        if doc.exists:
            nombre = doc.to_dict().get("Nombre") or "Falta"
    except Exception as e:
        print(f"❌ Error obteniendo nombre para {uid}: {e}")
    nombre_cache[uid] = nombre
    return nombre


def abrir_gestion_mensajes(db: firestore.Client) -> None:
    """Abre la ventana de gestión de mensajes evitando duplicados."""
    global ventana_mensajes
    if ventana_mensajes and ventana_mensajes.winfo_exists():
        ventana_mensajes.lift()
        ventana_mensajes.focus_force()
        return

    ventana_mensajes = tk.Toplevel()
    ventana = ventana_mensajes
    ventana.title("Gestión de Mensajes")
    ventana.geometry("900x500")

    def on_close():
        global ventana_mensajes
        ventana_mensajes = None
        ventana.destroy()

    ventana.protocol("WM_DELETE_WINDOW", on_close)

    datos_tabla = []

    frame_top = tk.Frame(ventana)
    frame_top.pack(fill="x", padx=5, pady=5)

    if DateEntry:
        selector_fecha = DateEntry(frame_top, width=12, date_pattern="dd-mm-yyyy")
        selector_fecha.set_date(date.today())
    else:
        selector_fecha = tk.Entry(frame_top)
        selector_fecha.insert(0, date.today().strftime("%d-%m-%Y"))
        # TODO: reemplazar con DateEntry si se instala tkcalendar
    selector_fecha.pack(side="left")

    btn_filtrar = tk.Button(frame_top, text="Filtrar", command=lambda: cargar_mensajes())
    btn_filtrar.pack(side="left", padx=5)

    tk.Label(frame_top, text="Mensaje (agrupado):").pack(side="left", padx=(20, 5))
    combo_var = tk.StringVar()
    combo_mensajes = ttk.Combobox(frame_top, textvariable=combo_var, state="readonly")
    combo_mensajes.pack(side="left")
    combo_mensajes['values'] = ["(Todos)"]
    combo_mensajes.current(0)

    tk.Label(frame_top, text="Nombre:").pack(side="left", padx=(20, 5))
    entry_nombre = tk.Entry(frame_top)
    entry_nombre.pack(side="left")

    tk.Label(frame_top, text="Estado:").pack(side="left", padx=(20, 5))
    combo_estado = ttk.Combobox(frame_top, state="readonly")
    combo_estado.pack(side="left")
    combo_estado['values'] = ["(Todos)"]
    combo_estado.current(0)

    btn_limpiar = tk.Button(frame_top, text="Limpiar", command=lambda: limpiar())
    btn_limpiar.pack(side="left", padx=5)

    frame_tree = tk.Frame(ventana)
    frame_tree.pack(fill="both", expand=True)

    columnas = [
        "Tipo",
        "Día",
        "Hora",
        "Mensaje",
        "Cuerpo",
        "Fecha/Hora",
        "UID",
        "Teléfono",
        "Estado",
        "Motivo",
        "Nombre",
    ]
    tree = ttk.Treeview(frame_tree, columns=columnas, show="headings")
    for col in columnas:
        tree.heading(col, text=col)
    tree.column("Tipo", width=80, anchor="w")
    tree.column("Día", width=80, anchor="w")
    tree.column("Hora", width=60, anchor="w")
    tree.column("Mensaje", width=150, anchor="w")
    tree.column("Cuerpo", width=200, anchor="w")
    tree.column("Fecha/Hora", width=130, anchor="w")
    tree.column("UID", width=120, anchor="w")
    tree.column("Teléfono", width=100, anchor="w")
    tree.column("Estado", width=80, anchor="w")
    tree.column("Motivo", width=120, anchor="w")
    tree.column("Nombre", width=150, anchor="w")

    vsb = ttk.Scrollbar(frame_tree, orient="vertical", command=tree.yview)
    hsb = ttk.Scrollbar(frame_tree, orient="horizontal", command=tree.xview)
    tree.configure(yscroll=vsb.set, xscroll=hsb.set)
    tree.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    hsb.grid(row=1, column=0, sticky="ew")
    frame_tree.grid_rowconfigure(0, weight=1)
    frame_tree.grid_columnconfigure(0, weight=1)

    frame_bottom = tk.Frame(ventana)
    frame_bottom.pack(fill="x", padx=5, pady=5)

    btn_exportar = tk.Button(frame_bottom, text="Exportar CSV", command=lambda: exportar_csv())
    btn_exportar.pack(side="left")

    estado_var = tk.StringVar(value="")
    tk.Label(frame_bottom, textvariable=estado_var).pack(side="right")

    def obtener_fecha():
        if DateEntry:
            return selector_fecha.get_date()
        texto = selector_fecha.get().strip()
        try:
            return datetime.strptime(texto, "%d-%m-%Y").date()
        except Exception:
            messagebox.showerror("Error", "Fecha inválida. Use DD-MM-YYYY.")
            return None

    def aplicar_filtros():
        tree.delete(*tree.get_children())
        filtro_mensaje = combo_mensajes.get()
        filtro_estado = combo_estado.get()
        filtro_nombre = entry_nombre.get().strip().lower()
        filtrados = datos_tabla
        if filtro_mensaje != "(Todos)":
            filtrados = [d for d in filtrados if d.get("mensaje") == filtro_mensaje]
        if filtro_estado != "(Todos)":
            filtrados = [d for d in filtrados if d.get("estado", "") == filtro_estado]
        if filtro_nombre:
            filtrados = [d for d in filtrados if filtro_nombre in d.get("Nombre", "").lower()]
        for d in filtrados:
            tree.insert("", "end", values=(
                d.get("tipo", ""),
                d.get("dia", ""),
                d.get("hora", ""),
                d.get("mensaje", ""),
                d.get("cuerpo", ""),
                formatea_fecha(d.get("fechaHora")),
                d.get("uid", ""),
                d.get("telefono", ""),
                d.get("estado", ""),
                d.get("motivo", ""),
                d.get("Nombre", ""),
            ))

    def cargar_mensajes():
        d = obtener_fecha()
        if not d:
            return
        nonlocal datos_tabla
        inicio, fin = start_end_of_day(d)
        t0 = time.perf_counter()
        try:
            docs = db.collection("Mensajes").where("fechaHora", ">=", inicio).where("fechaHora", "<", fin).stream()
            datos = []
            mensajes_unicos = set()
            estados_unicos = set()
            for doc in docs:
                item = doc.to_dict()
                fecha = item.get("fechaHora")
                mensaje = item.get("mensaje", "")
                telefono = item.get("telefono", "")
                uid = item.get("uid")
                estado_msg = item.get("estado", "")
                motivo = item.get("motivo")
                if not motivo:
                    motivo = estado_msg or ""
                nombre = fetch_nombre(uid, db)
                datos.append({
                    "tipo": item.get("tipo", ""),
                    "dia": item.get("dia", ""),
                    "hora": item.get("hora", ""),
                    "mensaje": mensaje,
                    "cuerpo": item.get("cuerpo", ""),
                    "fechaHora": fecha,
                    "uid": uid or "",
                    "telefono": telefono,
                    "estado": estado_msg,
                    "motivo": motivo,
                    "Nombre": nombre,
                })
                mensajes_unicos.add(mensaje or "")
                estados_unicos.add(estado_msg or "")
            datos.sort(key=lambda x: x.get("fechaHora"), reverse=True)
            datos_tabla = datos

            sel_mensaje = combo_mensajes.get()
            sel_estado = combo_estado.get()
            valores_msj = ["(Todos)"] + sorted([m for m in mensajes_unicos if m])
            valores_est = ["(Todos)"] + sorted([e for e in estados_unicos if e])
            combo_mensajes['values'] = valores_msj
            combo_estado['values'] = valores_est
            combo_mensajes.set(sel_mensaje if sel_mensaje in valores_msj else "(Todos)")
            combo_estado.set(sel_estado if sel_estado in valores_est else "(Todos)")

            aplicar_filtros()
            ms = int((time.perf_counter() - t0) * 1000)
            estado_var.set(f"{len(datos_tabla)} resultados · {d.strftime('%d-%m-%Y')} · {ms} ms")
            print(f"⏱️ cargar_mensajes: {ms} ms ({len(datos_tabla)} registros)")
        except Exception as e:
            messagebox.showerror("Error", f"No se pudieron cargar los mensajes: {e}")
            print(f"❌ Error cargando mensajes: {e}")

    combo_mensajes.bind("<<ComboboxSelected>>", lambda e: aplicar_filtros())
    combo_estado.bind("<<ComboboxSelected>>", lambda e: aplicar_filtros())

    def exportar_csv():
        filtro_mensaje = combo_mensajes.get()
        filtro_estado = combo_estado.get()
        filtro_nombre = entry_nombre.get().strip().lower()
        filtrados = datos_tabla
        if filtro_mensaje != "(Todos)":
            filtrados = [d for d in filtrados if d.get("mensaje") == filtro_mensaje]
        if filtro_estado != "(Todos)":
            filtrados = [d for d in filtrados if d.get("estado", "") == filtro_estado]
        if filtro_nombre:
            filtrados = [d for d in filtrados if filtro_nombre in d.get("Nombre", "").lower()]

        if not filtrados:
            messagebox.showinfo("Sin datos", "No hay datos para exportar.")
            return

        fecha_seleccionada = obtener_fecha()
        if not fecha_seleccionada:
            return
        date_str = fecha_seleccionada.strftime("%Y%m%d")
        time_str = datetime.now().strftime("%H%M")
        sel = combo_mensajes.get()
        base = "Mensajes" if not sel or sel == "(Todos)" else sanitize_filename(sel)[:60]
        default_name = f"{date_str}_{time_str} {base}.csv"
        path = filedialog.asksaveasfilename(
            initialfile=default_name,
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
        )
        if not path:
            return

        try:
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
                writer.writerow(["uid", "telefono", "tipo", "mensaje", "cuerpo", "estado", "motivo", "dia", "hora", "fechaHora"])
                for item in filtrados:
                    writer.writerow([
                        item.get("uid", ""),
                        item.get("telefono", ""),
                        item.get("tipo", ""),
                        item.get("mensaje", ""),
                        item.get("cuerpo", ""),
                        item.get("estado", ""),
                        item.get("motivo", ""),
                        item.get("dia", ""),
                        item.get("hora", ""),
                        formatea_fecha(item.get("fechaHora")),
                    ])
            messagebox.showinfo("Éxito", "CSV exportado correctamente.")
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo exportar CSV: {e}")
            print(f"❌ Error exportando CSV: {e}")

    def limpiar():
        if DateEntry:
            selector_fecha.set_date(date.today())
        else:
            selector_fecha.delete(0, tk.END)
            selector_fecha.insert(0, date.today().strftime("%d-%m-%Y"))
        combo_mensajes['values'] = ["(Todos)"]
        combo_mensajes.current(0)
        combo_estado['values'] = ["(Todos)"]
        combo_estado.current(0)
        entry_nombre.delete(0, tk.END)
        cargar_mensajes()

    cargar_mensajes()

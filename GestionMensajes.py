import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import csv
from datetime import datetime, date, timedelta
import time
from typing import Dict, Tuple, List, Set
import re

try:
    from tkcalendar import DateEntry
except Exception:  # tkcalendar no disponible
    DateEntry = None  # type: ignore

from firebase_admin import firestore

# Gestión de reenvíos de mensajes
def reset_all_usuarios_mensaje(db: firestore.Client) -> None:
    """Establece Mensaje=False a todos los documentos de UsuariosAutorizados."""
    batch = db.batch()
    n = 0
    for doc in db.collection("UsuariosAutorizados").stream():
        batch.set(doc.reference, {"Mensaje": False}, merge=True)
        n += 1
        if n % 450 == 0:
            batch.commit()
            batch = db.batch()
    if n % 450 != 0:
        batch.commit()


def set_usuarios_mensaje_true(db: firestore.Client, uid_set: Set[str]) -> None:
    """Establece Mensaje=True sólo para los UIDs indicados."""
    batch = db.batch()
    n = 0
    for uid in uid_set:
        ref = db.collection("UsuariosAutorizados").document(uid)
        batch.set(ref, {"Mensaje": True}, merge=True)
        n += 1
        if n % 450 == 0:
            batch.commit()
            batch = db.batch()
    if n % 450 != 0:
        batch.commit()

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

    rows: List[dict] = []
    filtered_rows: List[dict] = []
    row_by_doc: Dict[str, dict] = {}
    seleccionados: Set[str] = set()

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

    chk_select_all_var = tk.BooleanVar(value=False)

    def on_select_all():
        if chk_select_all_var.get():
            for r in filtered_rows:
                seleccionados.add(r["doc_id"])
        else:
            for r in filtered_rows:
                seleccionados.discard(r["doc_id"])
        refrescar_checks()

    chk_select_all = tk.Checkbutton(
        frame_top,
        text="Seleccionar todos (filtrados)",
        variable=chk_select_all_var,
        command=on_select_all,
    )
    chk_select_all.pack(side="left", padx=20)

    frame_tree = tk.Frame(ventana)
    frame_tree.pack(fill="both", expand=True)

    columnas = [
        "✓",
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
        "doc_id",
    ]
    tree = ttk.Treeview(frame_tree, columns=columnas, show="headings")
    for col in columnas:
        tree.heading(col, text=col if col != "doc_id" else "")
    tree.column("✓", width=40, anchor="center", stretch=False)
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
    tree.column("doc_id", width=0, stretch=False)

    vsb = ttk.Scrollbar(frame_tree, orient="vertical", command=tree.yview)
    hsb = ttk.Scrollbar(frame_tree, orient="horizontal", command=tree.xview)
    tree.configure(yscroll=vsb.set, xscroll=hsb.set)
    tree.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    hsb.grid(row=1, column=0, sticky="ew")
    frame_tree.grid_rowconfigure(0, weight=1)
    frame_tree.grid_columnconfigure(0, weight=1)

    def on_tree_click(event):
        region = tree.identify("region", event.x, event.y)
        if region != "cell":
            return
        col = tree.identify_column(event.x)
        if col != "#1":
            return
        item = tree.identify_row(event.y)
        if not item:
            return
        doc_id = tree.set(item, "doc_id")
        if doc_id in seleccionados:
            seleccionados.remove(doc_id)
        else:
            seleccionados.add(doc_id)
        refrescar_checks()

    tree.bind("<Button-1>", on_tree_click)

    frame_bottom = tk.Frame(ventana)
    frame_bottom.pack(fill="x", padx=5, pady=5)

    btn_reenviar = tk.Button(frame_bottom, text="Reenviar Mensajes", state="disabled", command=lambda: on_reenviar())
    btn_reenviar.pack(side="left")

    btn_exportar = tk.Button(frame_bottom, text="Exportar CSV", command=lambda: exportar_csv())
    btn_exportar.pack(side="left", padx=5)

    lbl_sel = ttk.Label(frame_bottom, text="0 seleccionados")
    lbl_sel.pack(side="left", padx=20)

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
        nonlocal filtered_rows
        tree.delete(*tree.get_children())
        filtro_mensaje = combo_mensajes.get()
        filtro_estado = combo_estado.get()
        filtro_nombre = entry_nombre.get().strip().lower()
        filtrados = rows
        if filtro_mensaje != "(Todos)":
            filtrados = [d for d in filtrados if d.get("mensaje") == filtro_mensaje]
        if filtro_estado != "(Todos)":
            filtrados = [d for d in filtrados if d.get("estado", "") == filtro_estado]
        if filtro_nombre:
            filtrados = [d for d in filtrados if filtro_nombre in d.get("Nombre", "").lower()]
        filtered_rows = filtrados
        for d in filtrados:
            tree.insert(
                "",
                "end",
                values=(
                    "✔" if d["doc_id"] in seleccionados else "",
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
                    d.get("doc_id", ""),
                ),
            )
        refrescar_checks()

    def refrescar_checks():
        """Refresca los símbolos de selección, contador y estado del maestro."""
        for item in tree.get_children():
            doc_id = tree.set(item, "doc_id")
            tree.set(item, "✓", "✔" if doc_id in seleccionados else "")
        lbl_sel.config(text=f"{len(seleccionados)} seleccionados")
        btn_reenviar.config(state="normal" if seleccionados else "disabled")
        if filtered_rows:
            all_sel = all(r["doc_id"] in seleccionados for r in filtered_rows)
            chk_select_all_var.set(all_sel)
        else:
            chk_select_all_var.set(False)

    def cargar_mensajes():
        d = obtener_fecha()
        if not d:
            return
        nonlocal rows, row_by_doc
        inicio, fin = start_end_of_day(d)
        t0 = time.perf_counter()
        try:
            docs = db.collection("Mensajes").where("fechaHora", ">=", inicio).where("fechaHora", "<", fin).stream()
            datos = []
            row_by_doc = {}
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
                row = {
                    "doc_id": doc.id,
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
                }
                datos.append(row)
                row_by_doc[doc.id] = row
                mensajes_unicos.add(mensaje or "")
                estados_unicos.add(estado_msg or "")
            datos.sort(key=lambda x: x.get("fechaHora"), reverse=True)
            rows = datos
            seleccionados.intersection_update(row_by_doc.keys())

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
            estado_var.set(f"{len(rows)} resultados · {d.strftime('%d-%m-%Y')} · {ms} ms")
            print(f"⏱️ cargar_mensajes: {ms} ms ({len(rows)} registros)")
        except Exception as e:
            messagebox.showerror("Error", f"No se pudieron cargar los mensajes: {e}")
            print(f"❌ Error cargando mensajes: {e}")

    def on_reenviar():
        if not seleccionados:
            return
        uid_set = {row_by_doc[doc_id]["uid"] for doc_id in seleccionados if doc_id in row_by_doc}
        if not uid_set:
            return
        if not messagebox.askyesno("Confirmar", f"¿Reenviar mensajes a {len(uid_set)} usuarios?"):
            return
        btn_reenviar.config(state="disabled")
        ventana.update_idletasks()
        try:
            reset_all_usuarios_mensaje(db)
            set_usuarios_mensaje_true(db, uid_set)
            first_id = next(iter(seleccionados))
            first_row = row_by_doc[first_id]
            preset = {k: first_row.get(k, "") for k in ("tipo", "mensaje", "cuerpo", "dia", "hora")}
            for k in ("tipo", "mensaje", "cuerpo", "dia", "hora"):
                v = first_row.get(k)
                if not all(row_by_doc[s].get(k) == v for s in seleccionados):
                    preset[k] = v
            preset["selected_count"] = len(uid_set)
            from GenerarMensajes import abrir_generar_mensajes
            abrir_generar_mensajes(db, preset=preset)
        finally:
            btn_reenviar.config(state="normal")

    combo_mensajes.bind("<<ComboboxSelected>>", lambda e: aplicar_filtros())
    combo_estado.bind("<<ComboboxSelected>>", lambda e: aplicar_filtros())

    def exportar_csv():
        filtro_mensaje = combo_mensajes.get()
        filtro_estado = combo_estado.get()
        filtro_nombre = entry_nombre.get().strip().lower()
        filtrados = rows
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

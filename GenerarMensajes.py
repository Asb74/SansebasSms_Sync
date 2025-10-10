import tkinter as tk
from tkinter import ttk, messagebox
import datetime as dt
from datetime import datetime, date, timezone
from google.cloud.firestore_v1.base_query import FieldFilter

try:
    from tkcalendar import DateEntry
except Exception:  # pragma: no cover - tkcalendar opcional
    DateEntry = None  # type: ignore

from GestionUsuarios import on_mensajes_generados


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


def _resolver_nombres(db, uids: list[str]) -> dict[str, str]:
    """Devuelve {uid: Nombre} usando UsuariosAutorizados."""
    out: dict[str, str] = {}
    for uid in uids:
        try:
            doc = db.collection("UsuariosAutorizados").document(uid).get()
            data = doc.to_dict() or {}
            out[uid] = data.get("Nombre") or uid
        except Exception:
            out[uid] = uid
    return out


def _prechequeo_dias_libres(db, fecha_msg: date, uids_sel: list[str]) -> tuple[set[str], list[str]]:
    if not uids_sel:
        return set(), []

    inicio = start_of_day_local_to_utc(fecha_msg)
    fin = end_of_day_local_to_utc(fecha_msg)

    try:
        peticiones = list(
            db.collection("Peticiones")
            .where(filter=FieldFilter("Fecha", ">=", inicio))
            .where(filter=FieldFilter("Fecha", "<=", fin))
            .stream()
        )
    except Exception:
        peticiones = []

    uids_sel_set = set(uids_sel)
    conflict_uids: set[str] = set()

    for peticion in peticiones:
        data = peticion.to_dict() or {}
        if not _is_ok(data.get("Admitido")):
            continue
        uid = data.get("uid") or data.get("Uid")
        fecha = _timestamp_to_local_date(data.get("Fecha"))
        if not uid or uid not in uids_sel_set or fecha != fecha_msg:
            continue
        conflict_uids.add(uid)

    nombres_map = _resolver_nombres(db, list(conflict_uids))
    nombres_conf = sorted(nombres_map.get(uid, uid) for uid in conflict_uids)
    return conflict_uids, nombres_conf


def _dialogo_conflictos(root, fecha_msg: date, nombres_conf: list[str]) -> bool:
    if not nombres_conf:
        return True

    top = tk.Toplevel(root)
    top.title("Días libres detectados")
    top.transient(root)
    top.grab_set()
    top.geometry("+{}+{}".format(root.winfo_rootx() + 60, root.winfo_rooty() + 60))

    ttk.Label(
        top,
        text=(
            f"Para la fecha {fecha_msg.strftime('%d-%m-%Y')} se han encontrado usuarios "
            "con día libre concedido:"
        ),
    ).pack(padx=12, pady=(12, 6), anchor="w")

    frame = ttk.Frame(top)
    frame.pack(fill="both", expand=True, padx=12, pady=(0, 12))

    lst = tk.Listbox(frame, height=min(10, max(3, len(nombres_conf))))
    for nombre in nombres_conf:
        lst.insert("end", nombre)
    lst.pack(fill="both", expand=True)

    respuesta = {"send": False}

    def _cancelar():
        respuesta["send"] = False
        top.destroy()

    def _enviar():
        respuesta["send"] = True
        top.destroy()

    botones = ttk.Frame(top)
    botones.pack(fill="x", padx=12, pady=(0, 12))
    ttk.Button(botones, text="Cancelar", command=_cancelar).pack(side="left")
    ttk.Button(
        botones,
        text="Enviar mensajes (excluyendo listados)",
        command=_enviar,
    ).pack(side="right")

    top.wait_window()
    return respuesta["send"]

ventana_generar = None


def abrir_generar_mensajes(db, preset=None):
    """Abre la ventana para generar mensajes masivos."""
    global ventana_generar
    if ventana_generar and ventana_generar.winfo_exists():
        ventana_generar.lift()
        ventana_generar.focus_force()
        return

    ventana_generar = tk.Toplevel()
    ventana_generar.title("Generar Mensajes")
    ventana_generar.resizable(False, False)

    frm = ttk.Frame(ventana_generar, padding=10)
    row_index = 1 if preset and preset.get("selected_count") else 0
    frm.grid(row=row_index, column=0, sticky="nsew")
    frm.columnconfigure(1, weight=1)
    pad = {"padx": 10, "pady": 6}

    if preset and preset.get("selected_count"):
        ttk.Label(ventana_generar, text=f"Reenviar a {preset['selected_count']} usuarios").grid(row=0, column=0, columnspan=2, pady=(0, 6))

    # --- Tipo ---
    ttk.Label(frm, text="Tipo:").grid(row=0, column=0, sticky="w", **pad)
    cmb_tipo = ttk.Combobox(frm, state="readonly")
    cmb_tipo.grid(row=0, column=1, sticky="ew", **pad)

    # --- Día ---
    ttk.Label(frm, text="Día:").grid(row=1, column=0, sticky="w", **pad)
    if DateEntry:
        date_entry = DateEntry(frm, date_pattern="yyyy-mm-dd")
    else:
        date_entry = ttk.Entry(frm)
        date_entry.insert(0, datetime.now().strftime("%Y-%m-%d"))
    date_entry.grid(row=1, column=1, sticky="ew", **pad)

    # --- Hora ---
    ttk.Label(frm, text="Hora:").grid(row=2, column=0, sticky="w", **pad)
    frm_time = ttk.Frame(frm)
    sp_hora = ttk.Spinbox(frm_time, from_=0, to=23, width=3, state="readonly", wrap=True, format="%02.0f")
    ttk.Label(frm_time, text=":").grid(row=0, column=1, padx=2)
    sp_min = ttk.Spinbox(frm_time, from_=0, to=59, width=3, state="readonly", wrap=True, format="%02.0f")
    sp_hora.grid(row=0, column=0)
    sp_min.grid(row=0, column=2)
    sp_hora.set("07")
    sp_min.set("00")
    frm_time.grid(row=2, column=1, sticky="w", **pad)

    # --- Mensaje ---
    ttk.Label(frm, text="Mensaje:").grid(row=3, column=0, sticky="w", **pad)
    cmb_mensaje = ttk.Combobox(frm, state="readonly")
    cmb_mensaje.grid(row=3, column=1, sticky="ew", **pad)

    # --- Cuerpo ---
    ttk.Label(frm, text="Cuerpo:").grid(row=4, column=0, sticky="nw", **pad)
    txt_cuerpo = tk.Text(frm, width=40, height=5)
    txt_cuerpo.grid(row=4, column=1, sticky="ew", **pad)
    lbl_contador = ttk.Label(frm, text="0/200")
    lbl_contador.grid(row=5, column=1, sticky="e", **pad)

    def limitar_cuerpo(event=None):
        texto = txt_cuerpo.get("1.0", "end-1c")
        if len(texto) > 200:
            txt_cuerpo.delete("1.0", tk.END)
            txt_cuerpo.insert("1.0", texto[:200])
            texto = texto[:200]
        lbl_contador.config(text=f"{len(texto)}/200")

    txt_cuerpo.bind("<KeyRelease>", limitar_cuerpo)

    # --- Botón guardar ---
    btn_guardar = ttk.Button(frm, text="Guardar")
    btn_guardar.grid(row=6, column=0, columnspan=2, pady=10)

    # --- Carga de datos Firestore ---
    def cargar_tipos():
        try:
            doc = db.collection("PlantillasTipoMensaje").document("TipoMensaje").get()
            if doc.exists:
                tipos = [s.strip() for s in doc.to_dict().get("Tipo", "").split(",") if s.strip()]
                cmb_tipo["values"] = tipos
        except Exception as e:
            messagebox.showerror("Error", f"No se pudieron cargar los tipos: {e}")

    def cargar_mensajes_por_tipo(event=None):
        tipo = cmb_tipo.get().strip()
        if not tipo:
            cmb_mensaje["values"] = []
            cmb_mensaje.set("")
            return
        try:
            doc = db.collection("PlantillasMensaje").document(tipo).get()
            mensajes = []
            if doc.exists:
                mensajes = [s.strip() for s in doc.to_dict().get("Mensaje", "").split(",") if s.strip()]
            cmb_mensaje["values"] = mensajes
            cmb_mensaje.set(mensajes[0] if mensajes else "")
        except Exception as e:
            messagebox.showerror("Error", f"No se pudieron cargar los mensajes: {e}")

    cmb_tipo.bind("<<ComboboxSelected>>", cargar_mensajes_por_tipo)

    # --- Guardar ---
    def guardar():
        tipo = cmb_tipo.get().strip()
        mensaje = cmb_mensaje.get().strip()
        cuerpo = txt_cuerpo.get("1.0", "end-1c").strip()
        if not tipo or not mensaje:
            messagebox.showerror("Error", "Debe seleccionar tipo y mensaje")
            return
        if len(cuerpo) > 200:
            messagebox.showerror("Error", "El cuerpo supera 200 caracteres")
            return

        try:
            if DateEntry and isinstance(date_entry, DateEntry):
                dia = date_entry.get_date()
            else:
                dia = datetime.strptime(date_entry.get().strip(), "%Y-%m-%d").date()
        except Exception:
            messagebox.showerror("Error", "Fecha inválida")
            return
        try:
            h = int(sp_hora.get())
            m = int(sp_min.get())
        except ValueError:
            messagebox.showerror("Error", "Hora inválida")
            return

        fechaHora = datetime(dia.year, dia.month, dia.day, h, m, 0)
        dia_str = dia.strftime("%Y-%m-%d")
        hora_str = f"{h:02d}:{m:02d}"

        btn_guardar.config(state="disabled")
        ventana_generar.update_idletasks()
        uids_afectados: list[str] = []

        try:
            usuarios_stream = db.collection("UsuariosAutorizados").where(
                filter=FieldFilter("Mensaje", "==", True)
            ).stream()
            usuarios_list = []
            uids_seleccionados: list[str] = []
            for doc_user in usuarios_stream:
                data_u = doc_user.to_dict() or {}
                uid = doc_user.id
                usuarios_list.append((uid, data_u))
                uids_seleccionados.append(uid)
        except Exception as e:
            messagebox.showerror("Error", f"No se pudieron obtener los usuarios seleccionados: {e}")
            btn_guardar.config(state="normal")
            return

        conflictos, nombres_conf = _prechequeo_dias_libres(db, dia, uids_seleccionados)

        if conflictos:
            seguir = _dialogo_conflictos(ventana_generar, dia, nombres_conf)
            if not seguir:
                btn_guardar.config(state="normal")
                return
            uids_permitidos = [uid for uid in uids_seleccionados if uid not in conflictos]
            if not uids_permitidos:
                messagebox.showinfo("Sin envíos", "Todos los usuarios seleccionados tienen día libre para esa fecha.")
                btn_guardar.config(state="normal")
                return
        else:
            uids_permitidos = uids_seleccionados

        uids_permitidos_set = set(uids_permitidos)
        usuarios_filtrados = [item for item in usuarios_list if item[0] in uids_permitidos_set]

        if not usuarios_filtrados:
            messagebox.showinfo("Sin usuarios", "No hay usuarios seleccionados para enviar mensajes.")
            btn_guardar.config(state="normal")
            return

        try:
            count = 0
            for uid, data_u in usuarios_filtrados:
                telefono = data_u.get("Telefono") or data_u.get("telefono") or ""
                doc_id = f"{uid}_{fechaHora.strftime('%Y%m%d%H%M')}"
                payload = {
                    "uid": uid,
                    "telefono": telefono,
                    "estado": "Pendiente",
                    "motivo": "Pendiente",
                    "tipo": tipo,
                    "mensaje": mensaje,
                    "cuerpo": cuerpo,
                    "dia": dia_str,
                    "hora": hora_str,
                    "fechaHora": fechaHora,
                }
                db.collection("Mensajes").document(doc_id).set(payload, merge=True)
                count += 1
                uids_afectados.append(uid)
                ventana_generar.update_idletasks()
        except Exception as e:
            messagebox.showerror("Error", f"No se pudieron crear los mensajes: {e}")
            btn_guardar.config(state="normal")
            return

        on_mensajes_generados(uids_afectados, db)

        messagebox.showinfo("OK", f"Mensajes creados para {count} usuarios")
        ventana_generar.destroy()

    btn_guardar.config(command=guardar)

    def aplicar_preset(preset):
        if not preset:
            return
        cmb_tipo.set(preset.get("tipo", ""))
        cargar_mensajes_por_tipo()
        cmb_mensaje.set(preset.get("mensaje", ""))
        txt_cuerpo.delete("1.0", "end")
        txt_cuerpo.insert("1.0", preset.get("cuerpo", ""))
        dia_str = preset.get("dia")
        if dia_str:
            from datetime import datetime as _dt
            date_entry.set_date(_dt.strptime(dia_str, "%Y-%m-%d").date())
        hora_str = preset.get("hora") or ""
        if ":" in hora_str:
            h, m = hora_str.split(":", 1)
            sp_hora.set(h)
            sp_min.set(m)

    cargar_tipos()
    aplicar_preset(preset)

import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime

try:
    from tkcalendar import DateEntry
except Exception:  # pragma: no cover - tkcalendar opcional
    DateEntry = None  # type: ignore

ventana_generar = None


def abrir_generar_mensajes(db, preset=None):
    """Abre la ventana para generar mensajes masivos.

    Parameters
    ----------
    db: firestore.Client
        Conexión a la base de datos.
    preset: dict | None
        Valores iniciales opcionales para precargar la interfaz.
    """
    global ventana_generar
    if ventana_generar and ventana_generar.winfo_exists():
        ventana_generar.lift()
        ventana_generar.focus_force()
        return

    preset = preset or {}

    ventana_generar = tk.Toplevel()
    ventana_generar.title("Generar Mensajes")
    ventana_generar.resizable(False, False)

    frm = ttk.Frame(ventana_generar, padding=10)
    frm.grid(sticky="nsew")
    frm.columnconfigure(1, weight=1)
    pad = {"padx": 10, "pady": 6}

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
    frm_time.grid(row=2, column=1, sticky="w", **pad)
    hora_var = tk.StringVar(value="07")
    min_var = tk.StringVar(value="00")
    sp_hora = tk.Spinbox(frm_time, from_=0, to=23, width=3, textvariable=hora_var,
                         state="readonly", wrap=True, format="%02.0f")
    sp_hora.pack(side="left")
    ttk.Label(frm_time, text=":").pack(side="left")
    sp_min = tk.Spinbox(frm_time, from_=0, to=59, width=3, textvariable=min_var,
                        state="readonly", wrap=True, format="%02.0f")
    sp_min.pack(side="left")

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
    lbl_destinatarios = ttk.Label(frm, text="")
    lbl_destinatarios.grid(row=6, column=0, columnspan=2, sticky="w", **pad)

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
    btn_guardar.grid(row=7, column=0, columnspan=2, pady=10)

    # --- Carga de datos Firestore ---
    def cargar_tipos():
        try:
            doc = db.collection("PlantillasTipoMensaje").document("TipoMensaje").get()
            if doc.exists:
                tipos = [s.strip() for s in doc.to_dict().get("Tipo", "").split(",") if s.strip()]
                cmb_tipo["values"] = tipos
                if preset.get("tipo") in tipos:
                    cmb_tipo.set(preset["tipo"])
                    cargar_mensajes()
                elif tipos:
                    cmb_tipo.set(tipos[0])
                    cargar_mensajes()
        except Exception as e:
            messagebox.showerror("Error", f"No se pudieron cargar los tipos: {e}")

    def cargar_mensajes(event=None):
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
            if preset.get("mensaje") in mensajes:
                cmb_mensaje.set(preset["mensaje"])
            elif mensajes:
                cmb_mensaje.set(mensajes[0])
            else:
                cmb_mensaje.set("")
        except Exception as e:
            messagebox.showerror("Error", f"No se pudieron cargar los mensajes: {e}")

    cmb_tipo.bind("<<ComboboxSelected>>", cargar_mensajes)

    def aplicar_preset_misc():
        if preset.get("dia"):
            try:
                if DateEntry and isinstance(date_entry, DateEntry):
                    date_entry.set_date(datetime.strptime(preset["dia"], "%Y-%m-%d").date())
                else:
                    date_entry.delete(0, tk.END)
                    date_entry.insert(0, preset["dia"])
            except Exception:
                pass
        if preset.get("hora"):
            try:
                h, m = preset["hora"].split(":")
                hora_var.set(h)
                min_var.set(m)
            except Exception:
                pass
        if preset.get("cuerpo"):
            txt_cuerpo.delete("1.0", tk.END)
            txt_cuerpo.insert("1.0", preset["cuerpo"])
            limitar_cuerpo()
        if preset.get("selected_count"):
            lbl_destinatarios.config(text=f"Reenviar a {preset['selected_count']} usuarios")

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
        try:
            usuarios = db.collection("UsuariosAutorizados").where("Mensaje", "==", True).stream()
            count = 0
            for doc_user in usuarios:
                data_u = doc_user.to_dict() or {}
                uid = doc_user.id
                telefono = data_u.get("Telefono") or data_u.get("telefono") or ""
                base_id = f"{uid}_{fechaHora.strftime('%Y%m%d%H%M')}"
                doc_id = base_id
                suf = 1
                while db.collection("Mensajes").document(doc_id).get().exists:
                    doc_id = f"{base_id}_{suf}"
                    suf += 1
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
                ventana_generar.update_idletasks()
        except Exception as e:
            messagebox.showerror("Error", f"No se pudieron crear los mensajes: {e}")
            btn_guardar.config(state="normal")
            return

        messagebox.showinfo("OK", f"Mensajes creados para {count} usuarios")
        ventana_generar.destroy()

    btn_guardar.config(command=guardar)

    cargar_tipos()
    aplicar_preset_misc()

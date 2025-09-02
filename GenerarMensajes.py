import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime

try:
    from tkcalendar import DateEntry
except Exception:  # Si tkcalendar no está disponible
    DateEntry = None  # type: ignore

ventana_generar = None


def abrir_generar_mensajes(db):
    global ventana_generar
    if ventana_generar and ventana_generar.winfo_exists():
        ventana_generar.lift()
        ventana_generar.focus_force()
        return

    ventana_generar = tk.Toplevel()
    ventana_generar.title("Generar Mensaje")
    ventana_generar.geometry("360x400")
    ventana_generar.resizable(False, False)

    frm = ttk.Frame(ventana_generar, padding=10)
    frm.grid(sticky="nsew")
    frm.columnconfigure(1, weight=1)

    ttk.Label(frm, text="Tipo:").grid(row=0, column=0, sticky="w", pady=5)
    cmb_tipo = ttk.Combobox(frm, state="readonly")
    cmb_tipo.grid(row=0, column=1, sticky="ew", pady=5)

    ttk.Label(frm, text="Día:").grid(row=1, column=0, sticky="w", pady=5)
    if DateEntry:
        dt_dia = DateEntry(frm, date_pattern="yyyy-mm-dd")
    else:
        dt_dia = ttk.Entry(frm)
        dt_dia.insert(0, datetime.now().strftime("%Y-%m-%d"))
    dt_dia.grid(row=1, column=1, sticky="ew", pady=5)

    ttk.Label(frm, text="Hora:").grid(row=2, column=0, sticky="w", pady=5)
    sp_hora = tk.Spinbox(frm, from_=0, to=23, width=5, format="%02.0f")
    sp_hora.delete(0, tk.END)
    sp_hora.insert(0, datetime.now().strftime("%H"))
    sp_hora.grid(row=2, column=1, sticky="w", pady=5)
    sp_minuto = tk.Spinbox(frm, from_=0, to=59, width=5, format="%02.0f")
    sp_minuto.delete(0, tk.END)
    sp_minuto.insert(0, datetime.now().strftime("%M"))
    sp_minuto.grid(row=2, column=1, sticky="e", pady=5)

    ttk.Label(frm, text="Mensaje:").grid(row=3, column=0, sticky="w", pady=5)
    cmb_mensaje = ttk.Combobox(frm, state="readonly")
    cmb_mensaje.grid(row=3, column=1, sticky="ew", pady=5)

    ttk.Label(frm, text="Cuerpo:").grid(row=4, column=0, sticky="nw", pady=5)
    txt_cuerpo = tk.Text(frm, height=5, width=30)
    txt_cuerpo.grid(row=4, column=1, sticky="ew", pady=5)
    lbl_contador = ttk.Label(frm, text="0/200")
    lbl_contador.grid(row=5, column=1, sticky="e")

    def on_key(event=None):
        contenido = txt_cuerpo.get("1.0", "end-1c")
        if len(contenido) > 200:
            txt_cuerpo.delete("1.0", tk.END)
            txt_cuerpo.insert("1.0", contenido[:200])
            contenido = contenido[:200]
        lbl_contador.config(text=f"{len(contenido)}/200")
    txt_cuerpo.bind("<KeyRelease>", on_key)

    btn_guardar = ttk.Button(frm, text="Guardar")
    btn_guardar.grid(row=6, column=0, columnspan=2, pady=10)

    def cargar_tipos():
        try:
            doc = db.collection("PlantillasTipoMensaje").document("TipoMensaje").get()
            if doc.exists:
                tipos = [x.strip() for x in doc.to_dict().get("Tipo", "").split(",") if x.strip()]
                cmb_tipo["values"] = tipos
        except Exception as e:
            messagebox.showerror("Error", f"No se pudieron cargar los tipos: {e}")
            print(f"❌ Error cargando tipos: {e}")

    def cargar_mensajes_por_tipo(event=None):
        tipo = cmb_tipo.get().strip()
        try:
            doc = db.collection("PlantillasMensaje").document(tipo).get()
            if doc.exists:
                mensajes = [x.strip() for x in doc.to_dict().get("Mensaje", "").split(",") if x.strip()]
            else:
                mensajes = []
            cmb_mensaje["values"] = mensajes
            cmb_mensaje.set(mensajes[0] if mensajes else "")
        except Exception as e:
            messagebox.showerror("Error", f"No se pudieron cargar los mensajes: {e}")
            print(f"❌ Error cargando mensajes del tipo {tipo}: {e}")

    cmb_tipo.bind("<<ComboboxSelected>>", cargar_mensajes_por_tipo)

    def guardar_mensaje():
        tipo = cmb_tipo.get().strip()
        mensaje = cmb_mensaje.get().strip()
        cuerpo = txt_cuerpo.get("1.0", "end-1c").strip()
        if not tipo or not mensaje:
            messagebox.showerror("Error", "Debe seleccionar tipo y mensaje")
            return
        if len(cuerpo) > 200:
            messagebox.showerror("Error", "El cuerpo supera 200 caracteres")
            return

        if DateEntry:
            dia = dt_dia.get_date().strftime("%Y-%m-%d")
        else:
            dia = dt_dia.get().strip()
        try:
            dia_date = datetime.strptime(dia, "%Y-%m-%d").date()
        except Exception:
            messagebox.showerror("Error", "Fecha inválida")
            return
        try:
            h = int(sp_hora.get())
            m = int(sp_minuto.get())
        except ValueError:
            messagebox.showerror("Error", "Hora inválida")
            return
        hora = f"{h:02d}:{m:02d}"
        fechaHora = datetime(dia_date.year, dia_date.month, dia_date.day, h, m, 0)
        if fechaHora < datetime.now():
            messagebox.showerror("Error", "La fecha y hora no pueden estar en el pasado")
            return

        payload = {
            "estado": "Pendiente",
            "tipo": tipo,
            "dia": dia,
            "hora": hora,
            "mensaje": mensaje,
            "cuerpo": cuerpo,
            "fechaHora": fechaHora,
        }

        btn_guardar.config(state="disabled")
        try:
            db.collection("Mensajes").document().set(payload, merge=True)
            messagebox.showinfo("OK", "Mensaje guardado")
            print(f"✅ Mensaje guardado con fechaHora {fechaHora}")
            ventana_generar.destroy()
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo guardar el mensaje: {e}")
            print(f"❌ Error guardando mensaje: {e}")
        finally:
            btn_guardar.config(state="normal")

    btn_guardar.config(command=guardar_mensaje)

    cargar_tipos()


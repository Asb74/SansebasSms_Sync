import tkinter as tk
from tkinter import ttk, messagebox
import datetime
import pyodbc
import os
from firebase_admin import auth

def buscar_trabajador_access(dni):
    ruta = r'X:\ENLACES\Power BI\Campaña\PercecoBi(Campaña).mdb'
    if not os.path.exists(ruta):
        print("❌ Ruta MDB no encontrada.")
        return {}

    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};',
        f'DBQ={ruta};',
    )

    def parse_fecha(valor):
        if not valor:
            return None
        if isinstance(valor, datetime.datetime):
            return valor.date()
        if isinstance(valor, datetime.date):
            return valor
        if isinstance(valor, str):
            for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
                try:
                    return datetime.datetime.strptime(valor, fmt).date()
                except ValueError:
                    continue
        return None

    datos = {"Nombre": None, "Alta": None, "Baja": None, "Codigo": None}

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        query = f"""
            SELECT
                APELLIDOS & ' ' & APELLIDOS2 & ', ' & NOMBRE AS NombreCompuesto,
                FECHAALTA,
                FECHABAJA,
                CODIGO
            FROM TRABAJADORES
            WHERE DNI = ?
        """
        cursor.execute(query, (dni,))
        row = cursor.fetchone()

        if row:
            datos["Nombre"] = row.NombreCompuesto
            datos["Alta"] = parse_fecha(row.FECHAALTA)
            datos["Baja"] = parse_fecha(row.FECHABAJA)
            datos["Codigo"] = row.CODIGO

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error TRABAJADORES MDB para {dni}: {e}")

    return datos

def calcular_total_dias_horas(dni, desde_fecha):
    ruta = r'X:\\ENLACES\\Power BI\\Campaña\\PercecoBi(Campaña).mdb'
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={ruta};'
    )
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT FECHA, HORAS, HORASEXT, CATEGORIA
            FROM DATOS_AJUSTADOS
            WHERE DNI = ?
              AND FECHA >= ?
        """, (dni, desde_fecha))
        dias = 0
        total_horas = 0
        categoria = None
        ultima_fecha = None
        for row in cursor.fetchall():
            dias += 1
            total_horas += float(row.HORAS or 0) + float(row.HORASEXT or 0)
            if not categoria and row.CATEGORIA:
                categoria = row.CATEGORIA.strip()
            if row.FECHA:
                if not ultima_fecha or row.FECHA > ultima_fecha:
                    ultima_fecha = row.FECHA
        conn.close()
        return dias, round(total_horas, 2), categoria, ultima_fecha
    except Exception as e:
        print(f"❌ Error DATOS_AJUSTADOS para {dni}: {e}")
        return 0, 0.0, None, None
def abrir_gestion_usuarios(db):
    ventana = tk.Toplevel()
    ventana.title("👥 Gestión de Usuarios")
    ventana.geometry("1400x600")

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

    tabla = ttk.Treeview(tabla_canvas_frame, columns=columnas, show="headings", selectmode="browse")
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
                    return datetime.datetime.strptime(valor, "%d-%m-%Y")
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

    def limpiar_filtros():
        for entry in entradas_filtro.values():
            entry.delete(0, tk.END)
        aplicar_filtros()

    def guardar_dato(uid, campo, valor):
        try:
            doc_ref = db.collection("UsuariosAutorizados").document(uid)
            if campo in ["Mensaje", "Seleccionable", "Valor"]:
                valor = valor == "True"
            elif campo in ["TotalDia", "Codigo"]:
                valor = int(valor)
            elif campo in ["TotalHoras"]:
                valor = float(valor)
            elif campo in ["Alta", "UltimoDia", "Baja"]:
                valor = datetime.datetime.strptime(valor, "%d-%m-%Y")
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

        usuarios = db.collection("UsuariosAutorizados").stream()
        hoy = datetime.datetime.now().date()
        hoy_dt = datetime.datetime.combine(hoy, datetime.time.min)

        for doc in usuarios:
            uid = doc.id
            data = doc.to_dict()
            actualiza = {}

            dni = data.get("Dni", "").strip() or "Falta"
            data["Dni"] = dni

            # TRABAJADORES
            if dni != "Falta":
                datos_trab = buscar_trabajador_access(dni)
                if datos_trab:
                    cambios = {}
                    nombre = datos_trab.get("Nombre")
                    if nombre and nombre != data.get("Nombre", ""):
                        cambios["Nombre"] = nombre
                        data["Nombre"] = nombre
                    alta_fecha = datos_trab.get("Alta")
                    if alta_fecha:
                        try:
                            nueva_alta = datetime.datetime.combine(alta_fecha, datetime.time.min)
                            if nueva_alta != data.get("Alta"):
                                cambios["Alta"] = nueva_alta
                                data["Alta"] = nueva_alta
                        except Exception as e:
                            print(f"⚠️ Error procesando Alta para {dni}: {e}")

                    baja_fecha = datos_trab.get("Baja")
                    if baja_fecha:
                        try:
                            nueva_baja = datetime.datetime.combine(baja_fecha, datetime.time.min)
                            if nueva_baja != data.get("Baja"):
                                cambios["Baja"] = nueva_baja
                                data["Baja"] = nueva_baja
                        except Exception as e:
                            print(f"⚠️ Error procesando Baja para {dni}: {e}")

                    codigo = datos_trab.get("Codigo")
                    if codigo and codigo != data.get("Codigo"):
                        cambios["Codigo"] = codigo
                        data["Codigo"] = codigo
                    if cambios:
                        db.collection("UsuariosAutorizados").document(uid).update(cambios)
                else:
                    actualiza["Mensaje"] = False
                    actualiza["Seleccionable"] = False
                    data["Mensaje"] = "False"
                    data["Seleccionable"] = "False"
            else:
                actualiza["Mensaje"] = False
                actualiza["Seleccionable"] = False
                data["Mensaje"] = "False"
                data["Seleccionable"] = "False"

            # DATOS_AJUSTADOS
            desde_fecha = data.get("Alta", hoy_dt).date()
            total_dias, total_horas, categoria, ultima_fecha = calcular_total_dias_horas(dni, desde_fecha)
            if total_dias != data.get("TotalDia"):
                actualiza["TotalDia"] = total_dias
                data["TotalDia"] = total_dias
            if round(total_horas, 2) != round(float(data.get("TotalHoras", 0.0)), 2):
                actualiza["TotalHoras"] = round(total_horas, 2)
                data["TotalHoras"] = total_horas
            if categoria and categoria != data.get("Puesto"):
                actualiza["Puesto"] = categoria
                data["Puesto"] = categoria
            if ultima_fecha:
                ultima_dt = datetime.datetime.combine(ultima_fecha, datetime.time.min)
                if ultima_dt != data.get("UltimoDia"):
                    actualiza["UltimoDia"] = ultima_dt
                    data["UltimoDia"] = ultima_dt
            if actualiza:
                db.collection("UsuariosAutorizados").document(uid).update(actualiza)


            # Default values
            data["Nombre"] = data.get("Nombre", "Falta")
            data["Telefono"] = data.get("Telefono", "")
            data["correo"] = data.get("correo", "")
            data["Puesto"] = data.get("Puesto", "Falta")
            data["Turno"] = str(data.get("Turno", "1"))
            data["Cultivo"] = data.get("Cultivo", "Falta")
            data["Mensaje"] = str(data.get("Mensaje", False))
            data["Seleccionable"] = str(data.get("Seleccionable", True))
            data["Valor"] = str(data.get("Valor", False))
            data["Alta"] = data.get("Alta", hoy_dt)
            data["UltimoDia"] = data.get("UltimoDia", hoy_dt)
            data["TotalDia"] = str(data.get("TotalDia", "0"))
            data["TotalHoras"] = str(data.get("TotalHoras", "0.0"))
            data["Baja"] = data.get("Baja", "")
            data["Codigo"] = str(data.get("Codigo", ""))

            fila = {
                "UID": uid,
                **{col: data.get(col, "") if col not in ["Alta", "UltimoDia", "Baja"] else data[col].strftime("%d-%m-%Y") if data[col] else "" for col in columnas}
            }

            datos_originales.append(fila)
            tabla.insert("", "end", iid=uid, values=[fila[col] for col in columnas])
    

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

            for campo in ["TotalDia", "Codigo"]:
                try:
                    datos[campo] = int(datos[campo])
                except:
                    datos[campo] = 0

            for campo in ["TotalHoras"]:
                try:
                    datos[campo] = float(datos[campo])
                except:
                    datos[campo] = 0.0

            for campo in ["Alta", "UltimoDia", "Baja"]:
                try:
                    datos[campo] = datetime.datetime.strptime(datos[campo], "%d-%m-%Y")
                except:
                    datos[campo] = None

            try:
                db.collection("UsuariosAutorizados").document(uid).update(datos)
            except Exception as e:
                print(f"⚠️ Error guardando {uid}: {e}")

        messagebox.showinfo("✅ Guardado", "Todos los cambios han sido guardados en Firebase.")

    tk.Button(frame_botones, text="🔍 Filtrar", command=aplicar_filtros).pack(side="left", padx=10)
    tk.Button(frame_botones, text="🧹 Limpiar", command=limpiar_filtros).pack(side="left", padx=10)
    tk.Button(frame_botones, text="🗑 Eliminar seleccionado", bg="salmon", command=eliminar_usuario).pack(side="left", padx=10)
    tk.Button(frame_botones, text="💾 Guardar todo", bg="lightgreen", command=guardar_todo).pack(side="left", padx=10)

    tabla.bind("<Double-1>", editar_celda)
    cargar_datos()

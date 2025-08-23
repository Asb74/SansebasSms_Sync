import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog, ttk
import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
import datetime
import os
import json
import requests
from dateutil import parser
from PIL import Image, ImageTk
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from GestionUsuarios import abrir_gestion_usuarios
import re
from decimal import Decimal
from typing import Optional
from google.cloud.firestore_v1.base_query import FieldFilter


def _safe_str(v) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s else None
    if isinstance(v, (int, float, Decimal)):
        return str(v)
    if isinstance(v, (bytes, bytearray)):
        for enc in ("utf-8", "latin-1", "cp1252"):
            try:
                s = v.decode(enc, errors="ignore").strip()
                return s if s else None
            except Exception:
                continue
        return None
    s = str(v).strip()
    return s if s else None


_FCM_TOKEN_RE = re.compile(r"^[A-Za-z0-9_\-:.]{100,}$")


def _is_valid_fcm_token(token: Optional[str]) -> bool:
    if not token:
        return False
    if not isinstance(token, str):
        token = _safe_str(token)
        if not token:
            return False
    return bool(_FCM_TOKEN_RE.match(token))


# 🔧 Configuración inicial
credenciales_dinamicas = {"ruta": "sansebassms.json"}
project_info = {"id": None}
carpeta_excel = {"ruta": None}
archivo_notificados = "notificados.json"

# Inicializar Firebase
try:
    with open(credenciales_dinamicas["ruta"], "r") as f:
        data = json.load(f)
        project_info["id"] = data.get("project_id")

    cred = credentials.Certificate(credenciales_dinamicas["ruta"])
    firebase_admin.initialize_app(cred)
    db = firestore.client()
except Exception as e:
    print(f"❌ Error al inicializar Firebase: {e}")
    exit()

def obtener_token_oauth():
    try:
        creds = service_account.Credentials.from_service_account_file(
            credenciales_dinamicas["ruta"],
            scopes=["https://www.googleapis.com/auth/firebase.messaging"]
        )
        creds.refresh(Request())
        return creds.token
    except Exception as e:
        raise Exception(f"No se pudo obtener el token de acceso: {e}")

def enviar_notificaciones_push():
    try:
        snapshot = db.collection("Mensajes").where(filter=FieldFilter("estado", "==", "Pendiente")).get()
        if not snapshot:
            messagebox.showinfo("Notificaciones", "No hay mensajes pendientes.")
            return

        token_oauth = obtener_token_oauth()
        notificados = []
        if os.path.exists(archivo_notificados):
            with open(archivo_notificados, "r") as f:
                notificados = json.load(f)

        nuevos = []

        for doc in snapshot:
            if doc.id in notificados:
                continue

            datos = doc.to_dict()
            uid = datos.get("uid")
            mensaje = datos.get("mensaje", "Tienes un mensaje pendiente")

            if not uid:
                continue

            usuario_doc = db.collection("UsuariosAutorizados").document(uid).get()
            if not usuario_doc.exists:
                continue

            usuario_data = usuario_doc.to_dict()
            token = usuario_data.get("fcmToken")
            if not _is_valid_fcm_token(token):
                print(f"⚠️ Token FCM inválido para {uid}, se omite.")
                continue

            payload = {
                "message": {
                    "token": token,
                    "notification": {
                        "title": "📩 Nuevo mensaje pendiente",
                        "body": mensaje
                    },
                    "data": {
                        "accion": "abrir_usuario_screen"
                    }
                }
            }

            headers = {
                "Authorization": f"Bearer {token_oauth}",
                "Content-Type": "application/json"
            }

            url = f"https://fcm.googleapis.com/v1/projects/{project_info['id']}/messages:send"
            response = requests.post(url, headers=headers, json=payload)

            if response.status_code == 200:
                nuevos.append(doc.id)
            else:
                print(f"❌ Error al enviar a {uid}: {response.text}")
                if response.status_code == 400 and "INVALID_ARGUMENT" in response.text:
                    db.collection("UsuariosAutorizados").document(uid).update({"fcmToken": None})
                    print(f"🧹 fcmToken inválido limpiado para {uid}")

        if nuevos:
            notificados.extend(nuevos)
            with open(archivo_notificados, "w") as f:
                json.dump(notificados, f)

        messagebox.showinfo("Resultado", f"✅ Notificaciones enviadas: {len(nuevos)}")
    except Exception as e:
        messagebox.showerror("Error", f"No se pudieron enviar notificaciones: {e}")

def crear_mensajes_para_todos():
    mensaje = simpledialog.askstring("Nuevo mensaje", "Escribe el mensaje que deseas enviar a los usuarios:", parent=ventana)
    if not mensaje:
        return

    try:
        usuarios = db.collection("UsuariosAutorizados").where(filter=FieldFilter("Mensaje", "==", True)).get()
        if not usuarios:
            messagebox.showinfo("Sin usuarios", "No hay usuarios con 'Mensaje = true'.")
            return

        for usuario in usuarios:
            uid = usuario.id
            data = usuario.to_dict()
            telefono = data.get("Telefono", "")

            ahora = datetime.datetime.now()
            doc_id = f"{uid}_{ahora.strftime('%Y-%m-%dT%H:%M:%S.%f')}"
            doc = {
                "estado": "Pendiente",
                "fechaHora": ahora,
                "mensaje": mensaje,
                "telefono": telefono,
                "uid": uid
            }
            db.collection("Mensajes").document(doc_id).set(doc)

        messagebox.showinfo("Éxito", f"✅ Se han creado mensajes para {len(usuarios)} usuarios.")
    except Exception as e:
        messagebox.showerror("Error", f"No se pudieron crear los mensajes: {e}")

# Funciones de sincronización (descargar, subir, etc.)
def seleccionar_carpeta_destino():
    carpeta = filedialog.askdirectory()
    if carpeta:
        carpeta_excel["ruta"] = carpeta
        estado.set(f"📁 Carpeta de destino seleccionada:\n{carpeta}")

def limpiar_fechas(doc):
    limpio = {}
    for k, v in doc.items():
        if isinstance(v, datetime.datetime):
            v = v.astimezone(datetime.timezone.utc).replace(tzinfo=None).isoformat()
        limpio[k] = v
    return limpio

def tipo_de_valor(valor):
    if isinstance(valor, bool): return "bool"
    if isinstance(valor, int): return "int"
    if isinstance(valor, float): return "float"
    if isinstance(valor, datetime.datetime): return "datetime"
    if isinstance(valor, list): return "list"
    if isinstance(valor, dict): return "dict"
    return "str"

def convertir_desde_tipos(dic, tipos):
    resultado = {}
    for k, v in dic.items():
        tipo = tipos.get(k, "str")
        try:
            if tipo == "datetime" and isinstance(v, str):
                resultado[k] = parser.isoparse(v)
            elif tipo == "int":
                resultado[k] = int(v)
            elif tipo == "float":
                resultado[k] = float(v)
            elif tipo == "bool":
                resultado[k] = str(v).strip().lower() in ["true", "sí"]
            elif tipo in ["list", "dict"]:
                import ast
                resultado[k] = ast.literal_eval(v) if isinstance(v, str) else v
            else:
                resultado[k] = str(v)
        except:
            resultado[k] = v
    return resultado

def descargar_todo():
    if not carpeta_excel["ruta"]:
        messagebox.showerror("Carpeta no seleccionada", "Debes seleccionar una carpeta de destino primero.")
        return
    try:
        colecciones = db.collections()
        for coleccion in colecciones:
            nombre = coleccion.id
            estado.set(f"⏳ Descargando: {nombre}...")
            ventana.update_idletasks()

            docs = coleccion.stream()
            datos = []
            tipos = {}

            for doc in docs:
                raw = doc.to_dict()
                limpio = limpiar_fechas(raw)
                limpio["_id"] = doc.id
                datos.append(limpio)
                for k, v in raw.items():
                    tipos[k] = tipo_de_valor(v)

            if datos:
                ruta_archivo = os.path.join(carpeta_excel["ruta"], f"{nombre}.xlsx")
                with pd.ExcelWriter(ruta_archivo, engine='openpyxl') as writer:
                    pd.DataFrame(datos).to_excel(writer, sheet_name="datos", index=False)
                    pd.DataFrame([{"campo": k, "tipo": v} for k, v in tipos.items()]).to_excel(writer, sheet_name="tipos", index=False)

        messagebox.showinfo("Éxito", "Todas las colecciones fueron exportadas.")
        estado.set("✅ Descarga completada.")
    except Exception as e:
        messagebox.showerror("Error", str(e))
        estado.set("❌ Error al descargar.")

def subir_archivo():
    archivo = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
    if not archivo:
        return
    try:
        nombre_coleccion = os.path.splitext(os.path.basename(archivo))[0]
        df = pd.read_excel(archivo, sheet_name="datos")
        df_tipos = pd.read_excel(archivo, sheet_name="tipos")
        tipos_dict = dict(zip(df_tipos["campo"], df_tipos["tipo"]))

        if "_id" not in df.columns:
            messagebox.showerror("Error", "El archivo no contiene una columna '_id'")
            return

        estado.set(f"⬆️ Subiendo: {nombre_coleccion}...")
        ventana.update_idletasks()

        ids_excel = set()
        for _, fila in df.iterrows():
            doc_id = str(fila["_id"])
            datos_limpios = fila.drop("_id").dropna().to_dict()
            data = convertir_desde_tipos(datos_limpios, tipos_dict)
            db.collection(nombre_coleccion).document(doc_id).set(data)
            ids_excel.add(doc_id)

        if eliminar_var.get():
            docs_firestore = db.collection(nombre_coleccion).stream()
            for doc in docs_firestore:
                if doc.id not in ids_excel:
                    db.collection(nombre_coleccion).document(doc.id).delete()

        messagebox.showinfo("Éxito", f"Archivo '{nombre_coleccion}.xlsx' sincronizado.")
        estado.set("✅ Subida completada.")
    except Exception as e:
        messagebox.showerror("Error", str(e))
        estado.set("❌ Error al subir archivo.")

def revisar_mensajes():
    try:
        notificados = []
        if os.path.exists(archivo_notificados):
            with open(archivo_notificados, "r") as f:
                notificados = json.load(f)

        nuevos = []
        snapshot = db.collection("Mensajes").where(filter=FieldFilter("estado", "==", "Pendiente")).get()
        for doc in snapshot:
            if doc.id not in notificados:
                mensaje = doc.to_dict().get("mensaje", "(sin mensaje)")
                nuevos.append((doc.id, mensaje))
                messagebox.showinfo("📨 Mensaje nuevo", f"{mensaje}")
                notificados.append(doc.id)

        if nuevos:
            with open(archivo_notificados, "w") as f:
                json.dump(notificados, f)

        if not nuevos:
            messagebox.showinfo("Mensajes", "No hay mensajes pendientes nuevos.")
    except Exception as e:
        messagebox.showerror("Error", f"Error al revisar mensajes: {e}")

# Interfaz
ventana = tk.Tk()
ventana.title("Sansebassms Sync")
ventana.geometry("500x580")
ventana.resizable(False, False)

try:
    ventana.iconphoto(False, tk.PhotoImage(file="icono_app.png"))
except:
    pass

frame = tk.Frame(ventana, padx=20, pady=20)
frame.pack(fill="both", expand=True)

try:
    img = Image.open("icono_app.png")
    img = img.resize((64, 64), Image.Resampling.LANCZOS)
    img_tk = ImageTk.PhotoImage(img)
    tk.Label(frame, image=img_tk).pack(pady=(0, 10))
except:
    pass

tk.Button(frame, text="📁 Seleccionar carpeta de destino", command=seleccionar_carpeta_destino, height=2, width=40).pack(pady=5)
tk.Button(frame, text="📥 Descargar todas las colecciones", command=descargar_todo, height=2, width=40).pack(pady=5)
tk.Button(frame, text="📤 Subir archivo Excel a Firebase", command=subir_archivo, height=2, width=40).pack(pady=5)
tk.Button(frame, text="📨 Revisar mensajes pendientes", command=revisar_mensajes, height=2, width=40).pack(pady=5)
tk.Button(frame, text="📝 Crear mensajes automáticos", command=crear_mensajes_para_todos, height=2, width=40, bg="lightblue").pack(pady=5)
tk.Button(frame, text="📲 Enviar notificaciones push", command=enviar_notificaciones_push, height=2, width=40, bg="lightgreen").pack(pady=5)
tk.Button(frame, text="👥 Gestionar Usuarios", command=lambda: abrir_gestion_usuarios(db), height=2, width=40, bg="lightyellow").pack(pady=5)


eliminar_var = tk.BooleanVar(value=True)
tk.Checkbutton(frame, text="Eliminar documentos no presentes en el Excel", variable=eliminar_var).pack(pady=5)

estado = tk.StringVar(value="Estado: Esperando acción...")
tk.Label(frame, textvariable=estado, fg="blue").pack(pady=10)

ventana.mainloop()

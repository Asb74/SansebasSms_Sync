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
from GestionMensajes import abrir_gestion_mensajes
from GenerarMensajes import abrir_generar_mensajes
import re
from decimal import Decimal
from typing import Optional
from google.cloud.firestore_v1.base_query import FieldFilter
import logging
import threading
import time

from logging_setup import install_global_excepthook
from ui_safety import info, warn, error


install_global_excepthook()
logging.info("SansebasSms Sync iniciado")


try:
    import tkcalendar  # noqa: F401
except Exception:
    logging.warning("Instale tkcalendar para habilitar selectores de fecha: pip install tkcalendar")


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


ventana: Optional[tk.Tk] = None
estado: Optional[tk.StringVar] = None


def run_bg(fn, *args, **kwargs) -> None:
    def _runner():
        thread_name = getattr(fn, "__name__", "worker")
        threading.current_thread().name = thread_name
        try:
            fn(*args, **kwargs)
        except Exception:
            logging.exception("Error inesperado en tarea de fondo '%s'", thread_name)
            if ventana is not None:
                error(ventana, "Error", "Ocurrió un error inesperado. Revisa los logs para más detalles.")

    threading.Thread(target=_runner, daemon=True).start()


def set_estado_async(texto: str) -> None:
    if ventana is not None and estado is not None:
        ventana.after(0, lambda: estado.set(texto))


# 🔧 Configuración inicial
credenciales_dinamicas = {"ruta": "sansebassms.json"}
project_info = {"id": None}
carpeta_excel = {"ruta": None}
archivo_notificados = "notificados.json"

# Inicializar Firebase
try:
    if not os.path.exists(credenciales_dinamicas["ruta"]):
        logging.warning("Archivo de credenciales no encontrado, solicitando al usuario.")
        root = tk.Tk()
        root.withdraw()
        nueva_ruta = filedialog.askopenfilename(
            title="Selecciona archivo de credenciales",
            filetypes=[("Archivos JSON", "*.json")]
        )
        if not nueva_ruta:
            messagebox.showinfo(
                "Credenciales",
                "No se seleccionó archivo de credenciales. La aplicación se cerrará."
            )
            root.destroy()
            raise SystemExit("Credenciales no proporcionadas")
        credenciales_dinamicas["ruta"] = nueva_ruta
        root.destroy()

    with open(credenciales_dinamicas["ruta"], "r", encoding="utf-8") as f:
        data = json.load(f)
        project_info["id"] = data.get("project_id")

    cred = credentials.Certificate(credenciales_dinamicas["ruta"])
    firebase_admin.initialize_app(cred)
    db = firestore.client()
except SystemExit:
    raise
except Exception:
    logging.exception("Error al inicializar Firebase")
    raise SystemExit(1)


def abrir_gestion_peticiones(db):
    from GestionPeticiones import abrir_gestion_peticiones as abrir

    sa_path = credenciales_dinamicas.get("ruta")
    project_id = project_info.get("id")
    abrir(db, sa_path, project_id)


def with_retry(fn, *, tries: int = 3, base: float = 0.5, cap: float = 5.0):
    delay = base
    last_exception = None
    for intento in range(1, tries + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - capturamos cualquier excepción
            last_exception = exc
            logging.warning("Intento %s/%s fallido: %s", intento, tries, exc, exc_info=True)
            if intento >= tries:
                break
            time.sleep(delay)
            delay = min(cap, delay * 2)
    if last_exception is not None:
        raise last_exception


def obtener_token_oauth():
    try:
        creds = service_account.Credentials.from_service_account_file(
            credenciales_dinamicas["ruta"],
            scopes=["https://www.googleapis.com/auth/firebase.messaging"]
        )
        creds.refresh(Request())
        return creds.token
    except Exception as e:
        raise RuntimeError(f"No se pudo obtener el token de acceso: {e}") from e


def get_doc_safe(doc_ref):
    try:
        doc = with_retry(doc_ref.get)
        if doc and getattr(doc, "exists", False):
            return doc
    except Exception:
        logging.exception("Error al obtener documento %s", getattr(doc_ref, "id", doc_ref))
    return None


def enviar_notificaciones_push():
    logging.info("Iniciando envío de notificaciones push")
    try:
        snapshot = with_retry(
            lambda: db.collection("Mensajes").where(filter=FieldFilter("estado", "==", "Pendiente")).get()
        )
        if not snapshot:
            info(ventana, "Notificaciones", "No hay mensajes pendientes.")
            return

        token_oauth = obtener_token_oauth()
        notificados: list[str] = []
        if os.path.exists(archivo_notificados):
            with open(archivo_notificados, "r", encoding="utf-8") as f:
                notificados = json.load(f)

        nuevos: list[str] = []

        for doc in snapshot:
            if doc.id in notificados:
                continue

            datos = doc.to_dict() or {}
            uid = datos.get("uid")
            mensaje = datos.get("mensaje", "Tienes un mensaje pendiente")

            if not uid:
                logging.debug("Documento %s sin uid, se omite", doc.id)
                continue

            usuario_doc = get_doc_safe(db.collection("UsuariosAutorizados").document(uid))
            if usuario_doc is None:
                logging.warning("Usuario %s no encontrado al enviar push", uid)
                continue

            usuario_data = usuario_doc.to_dict() or {}
            token = usuario_data.get("fcmToken")
            if not _is_valid_fcm_token(token):
                logging.warning("Token FCM inválido para %s, se omite.", uid)
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
                "Content-Type": "application/json",
            }

            url = f"https://fcm.googleapis.com/v1/projects/{project_info['id']}/messages:send"

            def _send():
                response = requests.post(url, headers=headers, json=payload, timeout=10)
                response.raise_for_status()
                return response

            try:
                with_retry(_send)
                nuevos.append(doc.id)
            except requests.HTTPError as http_err:
                logging.error("Error HTTP al enviar a %s: %s", uid, http_err, exc_info=True)
                if http_err.response is not None and http_err.response.status_code == 400 and "INVALID_ARGUMENT" in http_err.response.text:
                    with_retry(lambda: db.collection("UsuariosAutorizados").document(uid).update({"fcmToken": None}))
                    logging.info("fcmToken inválido limpiado para %s", uid)
            except Exception:
                logging.exception("Error al enviar notificación a %s", uid)

        if nuevos:
            notificados.extend(nuevos)
            with open(archivo_notificados, "w", encoding="utf-8") as f:
                json.dump(notificados, f)
            logging.info("Notificaciones marcadas como enviadas: %s", len(nuevos))

        info(ventana, "Resultado", f"✅ Notificaciones enviadas: {len(nuevos)}")
    except Exception as e:  # noqa: BLE001
        logging.exception("No se pudieron enviar notificaciones")
        error(ventana, "Error", f"No se pudieron enviar notificaciones: {e}")

def crear_mensajes_para_todos():
    mensaje = simpledialog.askstring(
        "Nuevo mensaje",
        "Escribe el mensaje que deseas enviar a los usuarios:",
        parent=ventana,
    )
    if not mensaje:
        return

    run_bg(_crear_mensajes_para_todos_bg, mensaje)


def _crear_mensajes_para_todos_bg(mensaje: str) -> None:
    logging.info("Creando mensajes para usuarios con Mensaje=True")
    try:
        usuarios = with_retry(
            lambda: db.collection("UsuariosAutorizados").where(filter=FieldFilter("Mensaje", "==", True)).get()
        )
        if not usuarios:
            info(ventana, "Sin usuarios", "No hay usuarios con 'Mensaje = true'.")
            return

        ahora = datetime.datetime.now()
        count = 0
        for usuario in usuarios:
            uid = usuario.id
            data = usuario.to_dict() or {}
            telefono = data.get("Telefono", "")
            doc_id = f"{uid}_{ahora.strftime('%Y-%m-%dT%H:%M:%S.%f')}"
            doc = {
                "estado": "Pendiente",
                "fechaHora": ahora,
                "mensaje": mensaje,
                "telefono": telefono,
                "uid": uid,
            }
            with_retry(lambda doc=doc, doc_id=doc_id: db.collection("Mensajes").document(doc_id).set(doc))
            count += 1

        info(ventana, "Éxito", f"✅ Se han creado mensajes para {count} usuarios.")
    except Exception as e:  # noqa: BLE001
        logging.exception("No se pudieron crear los mensajes")
        error(ventana, "Error", f"No se pudieron crear los mensajes: {e}")

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
        error(ventana, "Carpeta no seleccionada", "Debes seleccionar una carpeta de destino primero.")
        return

    run_bg(_descargar_todo_bg)


def _descargar_todo_bg() -> None:
    logging.info("Descargando colecciones de Firestore")
    try:
        colecciones = with_retry(lambda: list(db.collections()))
        for coleccion in colecciones:
            nombre = coleccion.id
            set_estado_async(f"⏳ Descargando: {nombre}...")

            docs = with_retry(lambda: list(coleccion.stream()))
            datos = []
            tipos = {}

            for doc in docs:
                raw = doc.to_dict() or {}
                limpio = limpiar_fechas(raw)
                limpio["_id"] = doc.id
                datos.append(limpio)
                for k, v in raw.items():
                    tipos[k] = tipo_de_valor(v)

            if datos:
                ruta_archivo = os.path.join(carpeta_excel["ruta"], f"{nombre}.xlsx")
                with pd.ExcelWriter(ruta_archivo, engine="openpyxl") as writer:
                    pd.DataFrame(datos).to_excel(writer, sheet_name="datos", index=False)
                    pd.DataFrame([{"campo": k, "tipo": v} for k, v in tipos.items()]).to_excel(
                        writer, sheet_name="tipos", index=False
                    )

        info(ventana, "Éxito", "Todas las colecciones fueron exportadas.")
        set_estado_async("✅ Descarga completada.")
    except Exception as e:  # noqa: BLE001
        logging.exception("Error al descargar colecciones")
        error(ventana, "Error", str(e))
        set_estado_async("❌ Error al descargar.")

def subir_archivo():
    archivo = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
    if not archivo:
        return
    eliminar_faltantes = bool(eliminar_var.get())
    run_bg(_subir_archivo_bg, archivo, eliminar_faltantes)


def _subir_archivo_bg(archivo: str, eliminar_faltantes: bool) -> None:
    nombre_coleccion = os.path.splitext(os.path.basename(archivo))[0]
    logging.info("Subiendo archivo %s a colección %s", archivo, nombre_coleccion)
    try:
        df = pd.read_excel(archivo, sheet_name="datos")
        df_tipos = pd.read_excel(archivo, sheet_name="tipos")
        tipos_dict = dict(zip(df_tipos["campo"], df_tipos["tipo"]))

        if "_id" not in df.columns:
            error(ventana, "Error", "El archivo no contiene una columna '_id'")
            return

        set_estado_async(f"⬆️ Subiendo: {nombre_coleccion}...")

        ids_excel = set()
        coleccion_ref = db.collection(nombre_coleccion)
        for _, fila in df.iterrows():
            doc_id = str(fila["_id"])
            datos_limpios = fila.drop("_id").dropna().to_dict()
            data = convertir_desde_tipos(datos_limpios, tipos_dict)
            with_retry(lambda data=data, doc_id=doc_id: coleccion_ref.document(doc_id).set(data))
            ids_excel.add(doc_id)

        if eliminar_faltantes:
            docs_firestore = with_retry(lambda: list(coleccion_ref.stream()))
            for doc in docs_firestore:
                if doc.id not in ids_excel:
                    with_retry(lambda doc_id=doc.id: coleccion_ref.document(doc_id).delete())

        info(ventana, "Éxito", f"Archivo '{nombre_coleccion}.xlsx' sincronizado.")
        set_estado_async("✅ Subida completada.")
    except Exception as e:  # noqa: BLE001
        logging.exception("Error al subir archivo %s", archivo)
        error(ventana, "Error", str(e))
        set_estado_async("❌ Error al subir archivo.")

def revisar_mensajes():
    run_bg(_revisar_mensajes_bg)


def _revisar_mensajes_bg() -> None:
    logging.info("Revisando mensajes pendientes")
    try:
        notificados: list[str] = []
        if os.path.exists(archivo_notificados):
            with open(archivo_notificados, "r", encoding="utf-8") as f:
                notificados = json.load(f)

        nuevos: list[tuple[str, str]] = []
        snapshot = with_retry(
            lambda: db.collection("Mensajes").where(filter=FieldFilter("estado", "==", "Pendiente")).get()
        )
        for doc in snapshot:
            if doc.id not in notificados:
                mensaje = (doc.to_dict() or {}).get("mensaje", "(sin mensaje)")
                nuevos.append((doc.id, mensaje))
                info(ventana, "📨 Mensaje nuevo", f"{mensaje}")
                notificados.append(doc.id)

        if nuevos:
            with open(archivo_notificados, "w", encoding="utf-8") as f:
                json.dump(notificados, f)
        else:
            info(ventana, "Mensajes", "No hay mensajes pendientes nuevos.")
    except Exception as e:  # noqa: BLE001
        logging.exception("Error al revisar mensajes")
        error(ventana, "Error", f"Error al revisar mensajes: {e}")

# Interfaz
ventana = tk.Tk()
ventana.title("Sansebassms Sync")
ventana.geometry("500x580")
ventana.resizable(False, False)

try:
    # Establecer el icono como predeterminado para todas las ventanas
    ventana.iconphoto(True, tk.PhotoImage(file="icono_app.png"))
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
SHOW_REVISAR_BTN = False
# Botón ocultado a petición: "Revisar mensajes pendientes"
btn_revisar = tk.Button(frame, text="📨 Revisar mensajes pendientes", command=revisar_mensajes, height=2, width=40)
btn_revisar.pack(pady=5)
if not SHOW_REVISAR_BTN:
    btn_revisar.pack_forget()

btn_crear_auto = tk.Button(
    frame,
    text="📝 Crear mensajes automáticos",
    command=crear_mensajes_para_todos,
    height=2,
    width=40,
    bg="lightblue",
)
btn_crear_auto.pack(pady=5)
btn_crear_auto.pack_forget()  # Botón ocultado a petición: "Crear mensajes automáticos"
tk.Button(
    frame,
    text="📲 Enviar notificaciones push",
    command=lambda: run_bg(enviar_notificaciones_push),
    height=2,
    width=40,
    bg="lightgreen",
).pack(pady=5)
tk.Button(frame, text="👥 Gestionar Usuarios", command=lambda: abrir_gestion_usuarios(db), height=2, width=40, bg="lightyellow").pack(pady=5)
tk.Button(frame, text="📜 Gestionar Mensajes", command=lambda: abrir_gestion_mensajes(db), height=2, width=40).pack(pady=5)
tk.Button(frame, text="Peticiones de Días Libres", command=lambda: abrir_gestion_peticiones(db), height=2, width=40).pack(pady=5)
tk.Button(frame, text="🆕 Generar mensajes", command=lambda: abrir_generar_mensajes(db), height=2, width=40).pack(pady=5)


eliminar_var = tk.BooleanVar(value=True)
tk.Checkbutton(frame, text="Eliminar documentos no presentes en el Excel", variable=eliminar_var).pack(pady=5)

estado = tk.StringVar(value="Estado: Esperando acción...")
tk.Label(frame, textvariable=estado, fg="blue").pack(pady=10)

ventana.mainloop()

import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog, ttk
import firebase_admin
from firebase_admin import credentials, firestore, messaging
import pandas as pd
import datetime
import os
import json
from dateutil import parser
from PIL import Image, ImageTk
from GestionUsuarios import abrir_gestion_usuarios
from GestionMensajes import abrir_gestion_mensajes
from GenerarMensajes import abrir_generar_mensajes
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


def _is_valid_fcm_token(token: Optional[str]) -> bool:
    token = _safe_str(token)
    return bool(token)


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
PROJECT_ID_JSON: Optional[str] = None

try:
    if not os.path.exists(credenciales_dinamicas["ruta"]):
        root = tk.Tk(); root.withdraw()
        nueva_ruta = filedialog.askopenfilename(
            title="Selecciona archivo de credenciales",
            filetypes=[("Archivos JSON", "*.json")]
        )
        if not nueva_ruta:
            messagebox.showinfo("Credenciales", "No se seleccionó archivo de credenciales. La aplicación se cerrará.")
            root.destroy()
            raise SystemExit(1)
        credenciales_dinamicas["ruta"] = nueva_ruta
        root.destroy()

    with open(credenciales_dinamicas["ruta"], "r", encoding="utf-8") as f:
        data = json.load(f)

    PROJECT_ID_JSON = data.get("project_id")      # <-- usar este SIEMPRE para inicializar
    project_info["id"] = PROJECT_ID_JSON

    cred = credentials.Certificate(credenciales_dinamicas["ruta"])
    # No forzar projectId al número: rompe Firestore
    firebase_admin.initialize_app(cred)
    db = firestore.client()

    logging.info("Firebase project id (JSON/efectivo): %s", PROJECT_ID_JSON)
except SystemExit:
    raise
except Exception as e:
    logging.exception("Error al inicializar Firebase: %s", e)
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


def get_doc_safe(doc_ref):
    try:
        doc = with_retry(doc_ref.get)
        if doc and getattr(doc, "exists", False):
            return doc
    except Exception:
        logging.exception("Error al obtener documento %s", getattr(doc_ref, "id", doc_ref))
    return None


# Server key del proyecto (LEGACY). Cárgala por variable de entorno.
LEGACY_KEY = os.environ.get("FCM_LEGACY_SERVER_KEY")


def send_legacy(token: str, title: str, body: str, data: dict) -> bool:
    if not LEGACY_KEY:
        return False
    try:
        import requests
        r = requests.post(
            "https://fcm.googleapis.com/fcm/send",
            headers={
                "Authorization": f"key={LEGACY_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "to": token,
                "notification": {"title": title, "body": body},
                "data": {k: str(v) for k, v in (data or {}).items()},
            },
            timeout=10,
        )
        r.raise_for_status()
        j = r.json()
        return j.get("success", 0) == 1
    except Exception:
        logging.exception("Legacy FCM fallo")
        return False


def _is_v1_404_text(txt: str) -> bool:
    t = txt.upper()
    return ("404" in t and "V1/PROJECTS" in t) or "REQUESTED ENTITY WAS NOT FOUND" in t or "NOT FOUND" in t


def _firestore_api_hint() -> str:
    # enlace directo a habilitar la API v1 del proyecto actual
    return f"https://console.cloud.google.com/apis/library/fcm.googleapis.com?project={project_info['id']}"


def diagnosticar_fcm():
    try:
        app = firebase_admin.get_app()
        enlace = f"https://console.cloud.google.com/apis/library/fcm.googleapis.com?project={project_info['id']}"
        info(
            ventana,
            "Diagnóstico FCM",
            f"Project JSON: {PROJECT_ID_JSON}\n"
            f"Project efectivo: {project_info['id']}\n"
            f"Admin app: {app.name}\n"
            f"Legacy KEY presente: {'sí' if LEGACY_KEY else 'no'}\n\n"
            f"Habilitar FCM v1 aquí:\n{enlace}"
        )
    except Exception as e:
        error(ventana, "Diagnóstico FCM", str(e))


def enviar_notificaciones_push():
    logging.info("Proyecto activo (JSON): %s", project_info["id"])
    logging.info("Enviando notificaciones una a una (Admin SDK)")
    try:
        pendientes = with_retry(
            lambda: db.collection("Mensajes").where(
                filter=FieldFilter("estado", "==", "Pendiente")
            ).get()
        )
        if not pendientes:
            info(ventana, "Notificaciones", "No hay mensajes pendientes.")
            return

        # notificados para no repetir
        notificados: list[str] = []
        if os.path.exists(archivo_notificados):
            with open(archivo_notificados, "r", encoding="utf-8") as f:
                notificados = json.load(f)

        objetivos = []  # (doc_id, uid, token, cuerpo)
        for doc in pendientes:
            if doc.id in notificados:
                continue
            d = doc.to_dict() or {}
            uid = d.get("uid")
            cuerpo = d.get("mensaje", "Tienes un mensaje pendiente")
            if not uid:
                logging.warning("Documento %s sin uid; se omite", doc.id)
                continue
            snap = get_doc_safe(db.collection("UsuariosAutorizados").document(uid))
            if snap is None:
                logging.warning("Usuario %s no encontrado al enviar push", uid)
                continue
            token = (snap.to_dict() or {}).get("fcmToken")
            if not token or not isinstance(token, str):
                logging.warning("Token FCM vacío/invalid para %s; se omite", uid)
                continue
            objetivos.append((doc.id, uid, token, cuerpo))

        if not objetivos:
            info(ventana, "Notificaciones", "No hay destinatarios válidos.")
            return

        ok, total = 0, len(objetivos)
        logging.info("Destinatarios a enviar: %d", total)

        for doc_id, uid, token, cuerpo in objetivos:

            def _send_one():
                msg = messaging.Message(
                    token=token,
                    notification=messaging.Notification(
                        title="📩 Nuevo mensaje pendiente",
                        body=cuerpo
                    ),
                    data={"accion":"abrir_usuario_screen"}
                )
                return messaging.send(msg, dry_run=False)  # devuelve message_id

            try:
                _ = with_retry(_send_one)
                ok += 1
                notificados.append(doc_id)

            except Exception as err:
                txt = str(err)
                logging.error("Error enviando a %s: %s", uid, txt, exc_info=True)

                # ❶ Si es 404/Not Found del endpoint v1 → instrucción clara para habilitar API
                if _is_v1_404_text(txt):
                    msg = ("No se pudieron enviar notificaciones (FCM v1 404). "
                           "Habilita 'Firebase Cloud Messaging API (V1)' para el proyecto "
                           f"'{project_info['id']}'. Abre este enlace y pulsa 'Enable':\n{_firestore_api_hint()}")
                    error(ventana, "FCM v1 no habilitado", msg)

                    # ❷ Fallback legacy si hay server key
                    if LEGACY_KEY and send_legacy(
                        token,
                        "📩 Nuevo mensaje pendiente",
                        cuerpo,
                        {"accion": "abrir_usuario_screen"},
                    ):
                        ok += 1
                        notificados.append(doc_id)
                        logging.info("Envío legacy OK para %s", uid)
                        time.sleep(0.05)
                        continue

                # Limpieza de tokens inválidos
                if "UNREGISTERED" in txt or "INVALID_ARGUMENT" in txt:
                    with_retry(lambda: db.collection("UsuariosAutorizados").document(uid).update({"fcmToken": None}))
                    logging.info("fcmToken inválido limpiado para %s", uid)

            finally:
                time.sleep(0.05)  # 50 ms entre envíos para no saturar

        with open(archivo_notificados, "w", encoding="utf-8") as f:
            json.dump(notificados, f, ensure_ascii=False, indent=2)

        info(ventana, "Resultado", f"✅ Notificaciones enviadas: {ok}/{total}")

    except Exception as e:
        logging.exception("Fallo al enviar notificaciones")
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
    """
    Crea documentos en Mensajes para todos los usuarios con Mensaje=True
    usando escrituras en lote (batch) para evitar 504 Deadline Exceeded.
    """
    logging.info("Creando mensajes (batch) para usuarios con Mensaje=True")
    try:
        usuarios = with_retry(
            lambda: db.collection("UsuariosAutorizados").where(
                filter=FieldFilter("Mensaje", "==", True)
            ).get()
        )

        if not usuarios:
            info(ventana, "Sin usuarios", "No hay usuarios con 'Mensaje = true'.")
            return

        ahora = datetime.datetime.now()
        total = 0
        batch_size = 400  # seguro por debajo del límite (500)

        batch = db.batch()
        en_batch = 0

        logging.info("Usuarios a procesar: %d", len(usuarios))

        for usuario in usuarios:
            uid = usuario.id
            udata = usuario.to_dict() or {}
            telefono = udata.get("Telefono", "")

            # id legible y único
            doc_id = f"{uid}_{ahora.strftime('%Y-%m-%dT%H-%M-%S-%f')}"
            doc_ref = db.collection("Mensajes").document(doc_id)
            payload = {
                "estado": "Pendiente",
                "fechaHora": ahora,
                "mensaje": mensaje,
                "telefono": telefono,
                "uid": uid,
            }

            batch.set(doc_ref, payload)
            en_batch += 1
            total += 1

            if en_batch >= batch_size:
                with_retry(batch.commit)
                logging.info("Batch de %d mensajes confirmado (acumulado=%d)", en_batch, total)
                # nuevo batch y pequeña pausa para aliviar presión de red
                batch = db.batch()
                en_batch = 0
                time.sleep(0.8)

        # commit final si queda algo pendiente
        if en_batch:
            with_retry(batch.commit)
            logging.info("Batch final de %d mensajes confirmado (total=%d)", en_batch, total)

        info(ventana, "Éxito", f"✅ Se han creado mensajes para {total} usuarios.")
    except Exception as e:
        logging.exception("Error creando mensajes en lote")
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
tk.Button(frame, text="🧪 Diagnosticar FCM", command=lambda: run_bg(diagnosticar_fcm), height=2, width=40).pack(pady=5)
tk.Button(frame, text="👥 Gestionar Usuarios", command=lambda: abrir_gestion_usuarios(db), height=2, width=40, bg="lightyellow").pack(pady=5)
tk.Button(frame, text="📜 Gestionar Mensajes", command=lambda: abrir_gestion_mensajes(db), height=2, width=40).pack(pady=5)
tk.Button(frame, text="Peticiones de Días Libres", command=lambda: abrir_gestion_peticiones(db), height=2, width=40).pack(pady=5)
tk.Button(frame, text="🆕 Generar mensajes", command=lambda: abrir_generar_mensajes(db), height=2, width=40).pack(pady=5)


eliminar_var = tk.BooleanVar(value=True)
tk.Checkbutton(frame, text="Eliminar documentos no presentes en el Excel", variable=eliminar_var).pack(pady=5)

estado = tk.StringVar(value="Estado: Esperando acción...")
tk.Label(frame, textvariable=estado, fg="blue").pack(pady=10)

ventana.mainloop()

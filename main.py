import tkinter as tk
from tkinter import filedialog, simpledialog, ttk, messagebox
import logging
from logging_setup import install_global_excepthook
install_global_excepthook()
logging.info("SansebasSms Sync iniciado")
from ui_safety import info, error
from thread_utils import run_bg
logger = logging.getLogger(__name__)
import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
import datetime
import os
import json
import requests
import time
from dateutil import parser
from PIL import Image, ImageTk
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from GestionUsuarios import abrir_gestion_usuarios
from GestionMensajes import abrir_gestion_mensajes
from GenerarMensajes import abrir_generar_mensajes
from notificaciones_push import NOTI_DB, enviar_push_por_mensaje
import re
from decimal import Decimal
from typing import List, Optional, Tuple
from google.cloud.firestore_v1.base_query import FieldFilter

try:
    import tkcalendar  # noqa: F401
except Exception:
    logger.warning(
        "Instale tkcalendar para habilitar selectores de fecha: pip install tkcalendar"
    )


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
archivo_notificados = NOTI_DB

ventana: Optional[tk.Misc] = None
estado: Optional[tk.StringVar] = None


def _get_root() -> Optional[tk.Misc]:
    if ventana is None:
        return None
    try:
        return ventana.winfo_toplevel()
    except Exception:
        return ventana


def _set_estado_async(texto: str) -> None:
    root_ref = _get_root()
    if root_ref is None or estado is None:
        return
    root_ref.after(0, lambda: estado.set(texto))


def _leer_notificados_local() -> list[str]:
    if not os.path.exists(archivo_notificados):
        return []
    try:
        with open(archivo_notificados, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            ids = data.get("ids", [])
        elif isinstance(data, list):
            ids = data
        else:
            ids = []
        return [str(i) for i in ids if i]
    except Exception:
        logger.exception("No se pudo leer %s", archivo_notificados)
        return []


def _guardar_notificados_local(ids: list[str]) -> None:
    try:
        with open(archivo_notificados, "w", encoding="utf-8") as f:
            json.dump({"ids": sorted(set(ids))}, f, ensure_ascii=False, indent=2)
    except Exception:
        logger.exception("No se pudo guardar %s", archivo_notificados)


def with_retry(fn, tries: int = 3, base: float = 0.5, cap: float = 5.0):
    """Ejecuta `fn` con reintentos exponenciales."""

    last_exc: Optional[Exception] = None
    for intento in range(1, tries + 1):
        try:
            return fn()
        except Exception as exc:  # pragma: no cover - logging side effect
            last_exc = exc
            if intento >= tries:
                logger.exception("Operación falló tras %s intentos", tries)
                raise
            delay = min(cap, base * (2 ** (intento - 1)))
            logger.warning(
                "Intento %s/%s fallido, reintentando en %.2f s", intento, tries, delay, exc_info=exc
            )
            time.sleep(delay)
    if last_exc is not None:
        raise last_exc


def _commit_with_retry(batch, max_tries: int = 4, base_sleep: float = 0.7):
    """Confirma un batch con reintentos y backoff exponencial."""

    for intento in range(1, max_tries + 1):
        try:
            return batch.commit(timeout=60.0)
        except Exception as exc:  # pragma: no cover - logging side effect
            if intento >= max_tries:
                logger.exception(
                    "No se pudo confirmar el lote tras %s intentos", max_tries
                )
                raise
            delay = min(6.0, base_sleep * (2 ** (intento - 1)))
            logger.warning(
                "Error al confirmar lote (intento %s/%s). Reintentando en %.2f s",
                intento,
                max_tries,
                delay,
                exc_info=exc,
            )
            time.sleep(delay)


def _paged_query(
    collection_ref,
    where_tuple: Tuple[str, str, object],
    order_field: str = "__name__",
    page_size: int = 200,
    start_after=None,
    timeout: float = 30.0,
) -> List:
    """Obtiene una página de documentos ordenados y filtrados."""

    field, op, value = where_tuple
    query = collection_ref.where(filter=FieldFilter(field, op, value)).order_by(order_field)
    if start_after is not None:
        query = query.start_after(start_after)
    return list(query.limit(page_size).stream(timeout=timeout))


def get_doc_safe(doc_ref):
    try:
        doc = doc_ref.get()
    except Exception as exc:
        logger.exception("Error al obtener documento %s", getattr(doc_ref, "id", doc_ref))
        return None
    if not getattr(doc, "exists", False):
        return None
    try:
        return doc.to_dict() or {}
    except Exception as exc:
        logger.exception("Error al convertir documento %s a dict", getattr(doc, "id", doc_ref))
        return None


def iter_collection_safe(col_ref):
    try:
        for doc in col_ref.stream():
            yield doc
    except Exception:
        logger.exception("Error al iterar colección %s", getattr(col_ref, "id", col_ref))


def enviar_fcm(uid: str, token: Optional[str], token_oauth: str, *, notification: dict, data: Optional[dict] = None) -> bool:
    if not _is_valid_fcm_token(token):
        logger.warning("Token FCM inválido para %s, se omite", uid)
        return False

    payload = {"message": {"token": token, "notification": notification}}
    if data:
        payload["message"]["data"] = data

    headers = {
        "Authorization": f"Bearer {token_oauth}",
        "Content-Type": "application/json",
    }

    url = f"https://fcm.googleapis.com/v1/projects/{project_info['id']}/messages:send"
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
    except Exception:
        logger.exception("Error enviando notificación a %s", uid)
        return False

    if response.status_code == 200:
        logger.info("Notificación enviada a %s", uid)
        return True

    logger.error("Error al enviar a %s: %s", uid, response.text)
    return False

# Inicializar Firebase
try:
    if not os.path.exists(credenciales_dinamicas["ruta"]):
        root_dialog = tk.Tk()
        root_dialog.withdraw()
        nueva_ruta = filedialog.askopenfilename(
            title="Selecciona archivo de credenciales",
            filetypes=[("Archivos JSON", "*.json")]
        )
        if not nueva_ruta:
            messagebox.showinfo(
                "Credenciales",
                "No se seleccionó archivo de credenciales. La aplicación se cerrará."
            )
            root_dialog.destroy()
            raise SystemExit(1)
        credenciales_dinamicas["ruta"] = nueva_ruta
        root_dialog.destroy()

    with open(credenciales_dinamicas["ruta"], "r", encoding="utf-8") as f:
        data = json.load(f)
        project_info["id"] = data.get("project_id")

    cred = credentials.Certificate(credenciales_dinamicas["ruta"])
    firebase_admin.initialize_app(cred)
    db = firestore.client()
except Exception as exc:
    logger.exception("Error al inicializar Firebase")
    root_dialog = tk.Tk()
    root_dialog.withdraw()
    messagebox.showerror(
        "Firebase",
        f"No se pudo inicializar Firebase: {exc}"
    )
    root_dialog.destroy()
    raise SystemExit(1)


def abrir_gestion_peticiones(db):
    from GestionPeticiones import abrir_gestion_peticiones as abrir

    sa_path = credenciales_dinamicas.get("ruta")
    project_id = project_info.get("id")
    abrir(db, sa_path, project_id)


def abrir_informes():
    from Informes import abrir_informes as abrir

    sa_path = credenciales_dinamicas.get("ruta")
    project_id = project_info.get("id")
    abrir(db, sa_path, project_id)


def obtener_token_oauth():
    try:
        creds = service_account.Credentials.from_service_account_file(
            credenciales_dinamicas["ruta"],
            scopes=["https://www.googleapis.com/auth/firebase.messaging"]
        )
        creds.refresh(Request())
        return creds.token
    except Exception as exc:
        logger.exception("No se pudo obtener el token de acceso")
        raise RuntimeError(f"No se pudo obtener el token de acceso: {exc}") from exc

def enviar_notificaciones_push():
    root = _get_root()

    def worker():
        try:
            snapshot = with_retry(
                lambda: db.collection("Mensajes").where(
                    filter=FieldFilter("estado", "==", "Pendiente")
                ).get()
            )
            if not snapshot:
                info(root, "Notificaciones", "No hay mensajes pendientes.")
                return

            total_enviados = 0
            total_fallidos = 0
            dedupe = 0

            for doc in snapshot:
                datos = doc.to_dict() or {}
                uid = datos.get("uid")
                if not uid:
                    logger.warning("Documento %s sin uid para envío push", doc.id)
                usuario_data = (
                    get_doc_safe(db.collection("UsuariosAutorizados").document(uid))
                    if uid
                    else {}
                ) or {}
                resultado = enviar_push_por_mensaje(
                    db,
                    doc.id,
                    datos,
                    usuario_data,
                    actualizar_estado=True,
                )
                enviados = int(resultado.get("enviados", 0))
                fallidos = int(resultado.get("fallidos", 0))
                if enviados == 0 and fallidos == 0:
                    dedupe += 1
                total_enviados += enviados
                total_fallidos += fallidos

            if total_enviados > 0 and total_fallidos == 0:
                info(root, "Resultado", f"✅ Notificaciones enviadas: {total_enviados}")
            elif total_enviados > 0:
                info(
                    root,
                    "Resultado",
                    f"Notificaciones enviadas: {total_enviados}. Fallidas: {total_fallidos}",
                )
            elif dedupe > 0:
                info(
                    root,
                    "Resultado",
                    "No se enviaron notificaciones nuevas (ya estaban enviadas).",
                )
            else:
                info(root, "Resultado", "No se enviaron notificaciones.")
        except Exception as exc:
            logger.exception("No se pudieron enviar notificaciones")
            error(root, "Error", f"No se pudieron enviar notificaciones: {exc}")

    run_bg(worker, _thread_name="enviar_notificaciones_push")

def crear_mensajes_para_todos():
    mensaje = simpledialog.askstring(
        "Nuevo mensaje",
        "Escribe el mensaje que deseas enviar a los usuarios:",
        parent=ventana,
    )
    if not mensaje:
        return

    run_bg(
        lambda: _crear_mensajes_para_todos_bg(mensaje),
        _thread_name="crear_mensajes_para_todos",
    )


def _crear_mensajes_para_todos_bg(mensaje: str) -> None:
    root = _get_root()
    try:
        usuarios_col = db.collection("UsuariosAutorizados")
        mensajes_col = db.collection("Mensajes")

        total_creados = 0
        pagina_actual = 0
        ultimo_doc = None

        while True:
            pagina = _paged_query(
                usuarios_col,
                ("Mensaje", "==", True),
                page_size=200,
                start_after=ultimo_doc,
            )

            if not pagina:
                if pagina_actual == 0 and total_creados == 0:
                    info(root, "Sin usuarios", "No hay usuarios con 'Mensaje = true'.")
                    return
                break

            pagina_actual += 1
            _set_estado_async(
                f"📄 Procesando página {pagina_actual} (mensajes creados: {total_creados})"
            )

            batch = db.batch()
            batch_count = 0

            for usuario in pagina:
                uid = usuario.id
                data = usuario.to_dict() or {}
                telefono = data.get("Telefono", "")

                ahora = datetime.datetime.now()
                doc_id = f"{uid}_{ahora.strftime('%Y-%m-%dT%H-%M-%S-%f')}"
                doc = {
                    "estado": "Pendiente",
                    "fechaHora": ahora,
                    "mensaje": mensaje,
                    "telefono": telefono,
                    "uid": uid,
                }

                batch.set(mensajes_col.document(doc_id), doc, timeout=30.0)
                batch_count += 1

                if batch_count >= 100:
                    _set_estado_async(
                        f"⬆️ Confirmando lote página {pagina_actual} (total: {total_creados + batch_count})"
                    )
                    _commit_with_retry(batch)
                    total_creados += batch_count
                    batch = db.batch()
                    batch_count = 0

            if batch_count:
                _set_estado_async(
                    f"⬆️ Confirmando lote final página {pagina_actual} (total: {total_creados + batch_count})"
                )
                _commit_with_retry(batch)
                total_creados += batch_count

            ultimo_doc = pagina[-1]

        _set_estado_async(f"✅ Mensajes creados: {total_creados}")
        info(root, "Éxito", f"✅ Se han creado mensajes para {total_creados} usuarios.")
        logger.info("Se crearon %s mensajes pendientes", total_creados)
    except Exception as exc:  # pragma: no cover - logging side effect
        logger.exception("No se pudieron crear los mensajes automáticos")
        _set_estado_async("❌ Error al crear mensajes.")
        error(root, "Error", f"No se pudieron crear los mensajes: {exc}")

# Funciones de sincronización (descargar, subir, etc.)
def seleccionar_carpeta_destino():
    carpeta = filedialog.askdirectory()
    if carpeta:
        carpeta_excel["ruta"] = carpeta
        if estado is not None:
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
        error(_get_root(), "Carpeta no seleccionada", "Debes seleccionar una carpeta de destino primero.")
        return
    root = _get_root()

    def worker():
        try:
            colecciones = list(db.collections())
            if not colecciones:
                info(root, "Descarga", "No se encontraron colecciones en Firestore.")
                _set_estado_async("Sin colecciones para descargar.")
                return

            for coleccion in colecciones:
                nombre = getattr(coleccion, "id", "coleccion")
                _set_estado_async(f"⏳ Descargando: {nombre}...")

                datos: list[dict] = []
                tipos: dict[str, str] = {}

                for doc in iter_collection_safe(coleccion):
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
                        pd.DataFrame([
                            {"campo": k, "tipo": v} for k, v in tipos.items()
                        ]).to_excel(writer, sheet_name="tipos", index=False)

            info(root, "Éxito", "Todas las colecciones fueron exportadas.")
            _set_estado_async("✅ Descarga completada.")
        except Exception as exc:
            logger.exception("Error al descargar colecciones")
            error(root, "Error", str(exc))
            _set_estado_async("❌ Error al descargar.")

    run_bg(worker, _thread_name="descargar_todo")

def subir_archivo():
    archivo = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
    if not archivo:
        return
    try:
        eliminar_faltantes = bool(eliminar_var.get()) if eliminar_var is not None else True
    except Exception:
        eliminar_faltantes = True

    root = _get_root()

    def worker():
        try:
            nombre_coleccion = os.path.splitext(os.path.basename(archivo))[0]
            df = pd.read_excel(archivo, sheet_name="datos")
            df_tipos = pd.read_excel(archivo, sheet_name="tipos")
            tipos_dict = dict(zip(df_tipos["campo"], df_tipos["tipo"]))

            if "_id" not in df.columns:
                error(root, "Error", "El archivo no contiene una columna '_id'")
                return

            _set_estado_async(f"⬆️ Subiendo: {nombre_coleccion}...")

            ids_excel = set()
            for _, fila in df.iterrows():
                doc_id = str(fila["_id"])
                datos_limpios = fila.drop("_id").dropna().to_dict()
                data = convertir_desde_tipos(datos_limpios, tipos_dict)
                db.collection(nombre_coleccion).document(doc_id).set(data)
                ids_excel.add(doc_id)

            if eliminar_faltantes:
                for doc in iter_collection_safe(db.collection(nombre_coleccion)):
                    if doc.id not in ids_excel:
                        db.collection(nombre_coleccion).document(doc.id).delete()

            info(root, "Éxito", f"Archivo '{nombre_coleccion}.xlsx' sincronizado.")
            _set_estado_async("✅ Subida completada.")
        except Exception as exc:
            logger.exception("Error al subir archivo")
            error(root, "Error", str(exc))
            _set_estado_async("❌ Error al subir archivo.")

    run_bg(worker, _thread_name="subir_archivo")

def revisar_mensajes():
    root = _get_root()

    def worker():
        try:
            notificados = _leer_notificados_local()

            nuevos = []
            snapshot = with_retry(
                lambda: db.collection("Mensajes").where(
                    filter=FieldFilter("estado", "==", "Pendiente")
                ).get()
            )
            for doc in snapshot:
                if doc.id not in notificados:
                    mensaje = (doc.to_dict() or {}).get("mensaje", "(sin mensaje)")
                    nuevos.append((doc.id, mensaje))
                    info(root, "📨 Mensaje nuevo", f"{mensaje}")
                    notificados.append(doc.id)

            if nuevos:
                _guardar_notificados_local(notificados)
            else:
                info(root, "Mensajes", "No hay mensajes pendientes nuevos.")
        except Exception as exc:
            logger.exception("Error al revisar mensajes")
            error(root, "Error", f"Error al revisar mensajes: {exc}")

    run_bg(worker, _thread_name="revisar_mensajes")

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
tk.Button(frame, text="📲 Enviar notificaciones push", command=enviar_notificaciones_push, height=2, width=40, bg="lightgreen").pack(pady=5)
tk.Button(frame, text="👥 Gestionar Usuarios", command=lambda: abrir_gestion_usuarios(db), height=2, width=40, bg="lightyellow").pack(pady=5)
tk.Button(frame, text="📜 Gestionar Mensajes", command=lambda: abrir_gestion_mensajes(db), height=2, width=40).pack(pady=5)
tk.Button(frame, text="Peticiones de Días Libres", command=lambda: abrir_gestion_peticiones(db), height=2, width=40).pack(pady=5)
tk.Button(frame, text="Informe", command=abrir_informes, height=2, width=40).pack(pady=5)
tk.Button(frame, text="🆕 Generar mensajes", command=lambda: abrir_generar_mensajes(db), height=2, width=40).pack(pady=5)


eliminar_var = tk.BooleanVar(value=True)
tk.Checkbutton(frame, text="Eliminar documentos no presentes en el Excel", variable=eliminar_var).pack(pady=5)

estado = tk.StringVar(value="Estado: Esperando acción...")
tk.Label(frame, textvariable=estado, fg="blue").pack(pady=10)

ventana.mainloop()

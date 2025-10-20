import csv
import os
import tkinter as tk
from datetime import datetime, timedelta, date
from tkinter import filedialog, messagebox, ttk
from typing import Any, Callable, Dict, List, Optional

import requests
import google.oauth2.service_account
import google.auth.transport.requests
from firebase_admin import firestore

try:
    from tkcalendar import DateEntry
except Exception:
    DateEntry = None

SCOPES = ["https://www.googleapis.com/auth/firebase.messaging"]

SERVICE_ACCOUNT_JSON: Optional[str] = None
PROJECT_ID: Optional[str] = None

ventana_peticiones: Optional[tk.Toplevel] = None
ventana_peticiones_app: Optional["GestionPeticionesUI"] = None

RESPONDER_IDENTIDAD = os.getenv("SANSEBASSMS_RESPONDER", "desktop_app")


class PeticionNoEncontradaError(RuntimeError):
    """Error raised when the request document does not exist."""


class PeticionYaRespondidaError(RuntimeError):
    """Error raised when trying to respond a request already handled."""


def mostrar_dialogo_respuesta(
    root: tk.Misc, titulo: str = "Responder petición"
) -> Optional[str]:
    dlg = tk.Toplevel(root)
    dlg.title(titulo)
    dlg.transient(root)
    dlg.grab_set()
    dlg.resizable(False, False)

    tk.Label(
        dlg,
        text="Selecciona una respuesta para la petición de día libre:",
        wraplength=360,
        justify="left",
    ).pack(padx=16, pady=12)

    choice: Dict[str, Optional[str]] = {"val": None}

    def _set_choice(val: Optional[str]) -> None:
        choice["val"] = val
        dlg.destroy()

    buttons_frame = tk.Frame(dlg)
    buttons_frame.pack(padx=16, pady=(4, 16), fill="x")

    tk.Button(
        buttons_frame,
        text="✅ OK",
        width=12,
        command=lambda: _set_choice("APROBADO"),
    ).pack(side="left", expand=True, padx=4)
    tk.Button(
        buttons_frame,
        text="⛔ Denegar",
        width=12,
        command=lambda: _set_choice("DENEGADO"),
    ).pack(side="left", expand=True, padx=4)
    tk.Button(
        buttons_frame,
        text="Cancelar",
        width=12,
        command=lambda: _set_choice(None),
    ).pack(side="right", expand=True, padx=4)

    dlg.update_idletasks()
    try:
        x = root.winfo_rootx() + (root.winfo_width() - dlg.winfo_width()) // 2
        y = root.winfo_rooty() + (root.winfo_height() - dlg.winfo_height()) // 2
        dlg.geometry(f"+{x}+{y}")
    except Exception:
        pass

    root.wait_window(dlg)
    return choice["val"]


def _cola_notificacion(
    db_client: firestore.Client, uid_solicitante: str, estado: str, solicitud_id: str
) -> None:
    cuerpo = (
        "✅ Tu día libre ha sido aprobado."
        if estado == "APROBADO"
        else "⛔ Tu día libre ha sido denegado."
    )
    db_client.collection("NotificacionesPendientes").add(
        {
            "uid": uid_solicitante,
            "titulo": "Respuesta a tu petición",
            "body": cuerpo,
            "tipo": "dia_libre",
            "solicitudId": solicitud_id,
            "createdAt": firestore.SERVER_TIMESTAMP,
        }
    )


def responder_peticion(
    root: tk.Misc,
    solicitud_id: str,
    uid_solicitante: str,
    on_refrescar_ui: Callable[[], None],
    *,
    db_client: firestore.Client,
    responded_by: str,
) -> None:
    eleccion = mostrar_dialogo_respuesta(root)
    if eleccion is None:
        return

    if not solicitud_id:
        messagebox.showerror("Error", "No se encontró el identificador de la solicitud.")
        return

    if not uid_solicitante:
        messagebox.showerror("Error", "No se encontró el solicitante asociado.")
        return

    root.configure(cursor="watch")
    root.update_idletasks()

    doc_ref = db_client.collection("PeticionesDiaLibre").document(solicitud_id)

    transaction = db_client.transaction()

    @firestore.transactional
    def _ejecutar(trans) -> None:  # type: ignore[override]
        snap = doc_ref.get(transaction=trans)
        if not snap.exists:
            raise PeticionNoEncontradaError()

        data = snap.to_dict() or {}
        estado_actual = (data.get("estado") or data.get("Admitido") or "").upper() or "PENDIENTE"
        if estado_actual != "PENDIENTE":
            raise PeticionYaRespondidaError()

        trans.update(
            doc_ref,
            {
                "estado": eleccion,
                "respondidoPor": responded_by,
                "respondidoEn": firestore.SERVER_TIMESTAMP,
            },
        )

        historial_ref = doc_ref.collection("historial").document()
        trans.set(
            historial_ref,
            {
                "evento": "RESPUESTA",
                "nuevoEstado": eleccion,
                "timestamp": firestore.SERVER_TIMESTAMP,
                "respondidoPor": responded_by,
            },
        )

    try:
        _ejecutar(transaction)
    except PeticionNoEncontradaError:
        messagebox.showerror("Error", "La solicitud seleccionada no existe.")
        try:
            on_refrescar_ui()
        except Exception:
            pass
        return
    except PeticionYaRespondidaError:
        messagebox.showwarning(
            "Aviso",
            "La solicitud ya fue respondida previamente.",
        )
        try:
            on_refrescar_ui()
        except Exception:
            pass
        return
    except Exception as exc:  # noqa: BLE001
        messagebox.showerror(
            "Error",
            f"No se pudo registrar la respuesta:\n{exc}",
        )
        return
    finally:
        root.configure(cursor="")

    try:
        _cola_notificacion(db_client, uid_solicitante, eleccion, solicitud_id)
    except Exception as exc:  # noqa: BLE001
        messagebox.showwarning(
            "Aviso",
            f"La respuesta se guardó pero no se pudo encolar la notificación:\n{exc}",
        )
    else:
        messagebox.showinfo("Respuesta registrada", "Se notificó al solicitante.")

    try:
        on_refrescar_ui()
    except Exception:
        pass


def _to_local(dt):
    from datetime import timezone

    if not dt:
        return None
    if getattr(dt, "tzinfo", None) is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone()


def _fmt_fecha(dt):
    local_dt = _to_local(dt)
    return local_dt.strftime("%d-%m-%Y") if local_dt else ""


def _fmt_fechahora(dt):
    local_dt = _to_local(dt)
    return local_dt.strftime("%d-%m-%Y %H:%M") if local_dt else ""


def normalizar_estado(v: str | None) -> str:
    s = (v or "").strip().lower()
    if s in {"ok", "aprobado"}:
        return "Aprobado"
    if s in {"denegado"}:
        return "Denegado"
    return "Pendiente"


def _parse_fecha_text(s: str):
    from datetime import datetime

    if not s:
        return None
    s = s.strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None


def _get_access_token(sa_path: str) -> str:
    credentials = google.oauth2.service_account.Credentials.from_service_account_file(
        sa_path, scopes=SCOPES
    )
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    return credentials.token


def send_push_to_token(sa_path: str, project_id: str, token: str, title: str, body: str):
    if not token:
        raise ValueError("FCM token vacío")
    access_token = _get_access_token(sa_path)
    url = f"https://fcm.googleapis.com/v1/projects/{project_id}/messages:send"
    payload = {
        "message": {
            "token": token,
            "notification": {"title": title, "body": body},
            "data": {"tipo": "peticion_dia_libre"},
        }
    }
    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        json=payload,
        timeout=10,
    )
    r.raise_for_status()


def enviar_push_resultado(
    db: firestore.Client,
    uid: str,
    fecha_str: str,
    admitido: str,
    token_prefetch: Optional[str] = None,
) -> None:
    token = token_prefetch
    if not token:
        doc = db.collection("UsuariosAutorizados").document(uid).get()
        if not doc.exists:
            messagebox.showwarning("Push no enviado", "Usuario no encontrado.")
            return
        token = (doc.to_dict() or {}).get("fcmToken")
    if not token:
        messagebox.showwarning("Push no enviado", "El usuario no tiene fcmToken.")
        return
    if not SERVICE_ACCOUNT_JSON or not PROJECT_ID:
        raise RuntimeError("Credenciales de servicio no configuradas.")
    title = "Petición de día libre"
    body = f"Tu petición para {fecha_str} ha sido {admitido}."
    send_push_to_token(SERVICE_ACCOUNT_JSON, PROJECT_ID, token, title, body)


class ToolTip:
    def __init__(self, widget: tk.Widget) -> None:
        self.widget = widget
        self.tipwindow: Optional[tk.Toplevel] = None

    def show(self, text: str, x: int, y: int) -> None:
        text = (text or "").strip()
        if not text:
            self.hide()
            return

        self.hide()

        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(
            f"+{self.widget.winfo_rootx() + x + 20}+{self.widget.winfo_rooty() + y + 20}"
        )

        label = tk.Label(
            tw,
            text=text,
            justify="left",
            background="#ffffe0",
            relief="solid",
            borderwidth=1,
            wraplength=400,
        )
        label.pack(ipadx=4, ipady=2)

    def hide(self) -> None:
        if self.tipwindow is not None:
            self.tipwindow.destroy()
            self.tipwindow = None



class GestionPeticionesUI:
    def __init__(self, window: tk.Toplevel, db: firestore.Client, on_close: Callable[[], None]) -> None:
        self.root = window
        self.db = db
        self._after_close_callback = on_close

        self._editor_abierto: Optional[tk.Toplevel] = None
        self._poll_job: Optional[str] = None
        self._peticiones_raw: List[Dict[str, Any]] = []

        self._tree_items_info: Dict[str, Dict[str, Any]] = {}
        self._tooltip_state: Dict[str, Optional[str]] = {"item": None, "text": None}
        self._operacion_en_progreso = False
        self._closed = False

        self.var_estado = tk.StringVar(value="")
        self.var_fecha_desde = tk.StringVar()
        self.var_fecha_hasta = tk.StringVar()
        self.var_texto = tk.StringVar()

        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        self._build_ui()

        try:
            self.cargar_peticiones(dias=60)
        except Exception:
            pass
        self._pintar_en_tree(self._peticiones_raw)

    def _build_ui(self) -> None:
        self.root.grid_rowconfigure(2, weight=1)
        self.root.grid_columnconfigure(0, weight=1)

        top_bar = ttk.Frame(self.root, padding=10)
        top_bar.grid(row=0, column=0, sticky="ew")
        top_bar.grid_columnconfigure(0, weight=1)

        ttk.Button(top_bar, text="Actualizar", command=self.actualizar).grid(
            row=0, column=0, sticky="w"
        )

        filtros = ttk.LabelFrame(self.root, text="Filtros", padding=10)
        filtros.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 8))
        for col in range(0, 8):
            filtros.grid_columnconfigure(col, weight=0)
        filtros.grid_columnconfigure(5, weight=1)

        ttk.Label(filtros, text="Estado").grid(row=0, column=0, sticky="w")
        self.cmb_estado = ttk.Combobox(
            filtros,
            textvariable=self.var_estado,
            state="readonly",
            values=("", "PENDIENTE", "APROBADO", "DENEGADO"),
            width=12,
        )
        self.cmb_estado.grid(row=0, column=1, padx=(4, 12), pady=2, sticky="w")
        self.cmb_estado.bind("<<ComboboxSelected>>", lambda _e: self.aplicar_filtros())

        ttk.Label(filtros, text="Fecha desde (YYYY-MM-DD)").grid(
            row=0, column=2, sticky="w"
        )
        ttk.Entry(filtros, textvariable=self.var_fecha_desde, width=12).grid(
            row=0, column=3, padx=(4, 12), pady=2, sticky="w"
        )

        ttk.Label(filtros, text="Fecha hasta (YYYY-MM-DD)").grid(
            row=0, column=4, sticky="w"
        )
        ttk.Entry(filtros, textvariable=self.var_fecha_hasta, width=12).grid(
            row=0, column=5, padx=(4, 12), pady=2, sticky="w"
        )

        ttk.Label(filtros, text="Texto").grid(row=0, column=6, sticky="w")
        texto_entry = ttk.Entry(filtros, textvariable=self.var_texto, width=28)
        texto_entry.grid(row=0, column=7, padx=(4, 12), pady=2, sticky="w")
        texto_entry.bind("<Return>", lambda _e: self.aplicar_filtros())

        botones = ttk.Frame(filtros)
        botones.grid(row=1, column=0, columnspan=8, sticky="e", pady=(8, 0))
        ttk.Button(botones, text="Filtrar", command=self.aplicar_filtros).grid(
            row=0, column=0, padx=(0, 6)
        )
        ttk.Button(botones, text="Limpiar", command=self.limpiar_filtros).grid(
            row=0, column=1
        )

        tree_frame = ttk.Frame(self.root, padding=(10, 0, 10, 0))
        tree_frame.grid(row=2, column=0, sticky="nsew")
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

        columnas = ("id", "estado", "solicitante", "fecha", "motivo")
        self.tree = ttk.Treeview(
            tree_frame,
            columns=columnas,
            show="headings",
            selectmode="browse",
        )

        yscroll = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        xscroll = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")

        self.tree.heading("id", text="ID")
        self.tree.heading("estado", text="Estado")
        self.tree.heading("solicitante", text="Solicitante")
        self.tree.heading("fecha", text="Fecha")
        self.tree.heading("motivo", text="Motivo")

        self.tree.column("id", width=140, anchor="w")
        self.tree.column("estado", width=120, anchor="center")
        self.tree.column("solicitante", width=220, anchor="w")
        self.tree.column("fecha", width=160, anchor="center")
        self.tree.column("motivo", width=320, anchor="w")

        self.tooltip = ToolTip(self.tree)
        self._clear_tooltip()

        self.tree.bind("<Double-1>", self._on_tree_double_click)
        self.tree.bind("<Motion>", self._on_tree_motion)
        self.tree.bind("<Leave>", lambda _e: self._clear_tooltip())

        bottom_bar = ttk.Frame(self.root, padding=10)
        bottom_bar.grid(row=3, column=0, sticky="ew")
        bottom_bar.grid_columnconfigure(0, weight=1)

        ttk.Button(bottom_bar, text="Responder", command=self.responder_seleccionada).grid(
            row=0, column=0, sticky="w", padx=(0, 8)
        )
        ttk.Button(bottom_bar, text="Exportar CSV", command=self.exportar_csv).grid(
            row=0, column=1, sticky="w"
        )
        ttk.Button(bottom_bar, text="Cerrar", command=self.on_close).grid(
            row=0, column=2, sticky="e"
        )

    def _clear_tooltip(self) -> None:
        if hasattr(self, "tooltip"):
            self.tooltip.hide()
        self._tooltip_state = {"item": None, "text": None}

    def _cerrar_editor(self) -> None:
        if self._editor_abierto is None:
            return
        try:
            if self._editor_abierto.winfo_exists():
                self._editor_abierto.destroy()
        except Exception:
            pass
        finally:
            self._editor_abierto = None

    def on_close(self) -> None:
        if self._closed:
            return
        self._closed = True

        if self._poll_job is not None:
            try:
                self.root.after_cancel(self._poll_job)
            except Exception:
                pass
            finally:
                self._poll_job = None

        try:
            self._cerrar_editor()
        except Exception:
            pass

        try:
            self.root.destroy()
        except Exception:
            pass

        if self._after_close_callback:
            try:
                self._after_close_callback()
            except Exception:
                pass

    def _to_py_dt(self, value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        converter = getattr(value, "to_datetime", None)
        if callable(converter):
            try:
                return converter()
            except Exception:
                return None
        if isinstance(value, str):
            for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"):
                try:
                    return datetime.strptime(value, fmt)
                except Exception:
                    continue
        return None

    def cargar_peticiones(self, dias: int = 60) -> None:
        try:
            documentos = list(self.db.collection("PeticionesDiaLibre").stream())
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror(
                "Error",
                f"No se pudieron leer las peticiones:\n{exc}",
            )
            raise

        limite = None
        if dias and dias > 0:
            limite = datetime.utcnow() - timedelta(days=dias)

        peticiones: List[Dict[str, Any]] = []
        for doc in documentos:
            data = doc.to_dict() or {}
            item: Dict[str, Any] = dict(data)
            item["__id"] = doc.id

            creado = self._to_py_dt(data.get("creadoEn") or data.get("creado_en"))
            if creado:
                item["creadoEn"] = creado
            respondido = self._to_py_dt(data.get("respondidoEn") or data.get("respondido_en"))
            if respondido:
                item["respondidoEn"] = respondido
            fecha_solicitada = self._to_py_dt(
                data.get("fechaSolicitada")
                or data.get("fecha_solicitada")
                or data.get("Fecha")
                or data.get("fecha")
            )
            if fecha_solicitada:
                item["fechaSolicitada"] = fecha_solicitada

            if limite and creado:
                creado_comparable = creado
                if creado_comparable.tzinfo is not None:
                    try:
                        creado_comparable = creado_comparable.astimezone().replace(tzinfo=None)
                    except Exception:
                        creado_comparable = creado_comparable.replace(tzinfo=None)
                if creado_comparable < limite:
                    continue

            if "estado" not in item:
                estado = data.get("Admitido") or data.get("estadoActual") or "PENDIENTE"
                item["estado"] = str(estado or "PENDIENTE").upper()
            else:
                item["estado"] = str(item.get("estado") or "PENDIENTE").upper()

            if "motivo" not in item:
                item["motivo"] = data.get("Motivo") or ""
            item["motivo"] = str(item.get("motivo") or "").strip()

            solicitante_nombre = (
                data.get("nombreSolicitante")
                or data.get("NombreSolicitante")
                or data.get("Nombre")
            )
            if solicitante_nombre:
                item["nombreSolicitante"] = solicitante_nombre

            uid_solicitante = (
                data.get("uidSolicitante")
                or data.get("uid")
                or data.get("uid_solicitante")
            )
            if uid_solicitante:
                item["uidSolicitante"] = uid_solicitante

            peticiones.append(item)

        self._peticiones_raw = peticiones
        print(f"[GestionPeticionesUI] {len(self._peticiones_raw)} peticiones cargadas")

    def _pintar_en_tree(self, items: List[Dict[str, Any]]) -> None:
        for item_id in self.tree.get_children():
            self.tree.delete(item_id)
        self._tree_items_info.clear()
        self._clear_tooltip()

        for registro in items:
            doc_id = (
                registro.get("__id")
                or registro.get("doc_id")
                or f"row_{len(self._tree_items_info)}"
            )
            estado = str(
                registro.get("estado")
                or registro.get("Admitido")
                or "PENDIENTE"
            ).upper()
            solicitante = (
                registro.get("nombreSolicitante")
                or registro.get("Nombre")
                or registro.get("uidSolicitante")
                or registro.get("uid")
                or ""
            )
            fecha_val = registro.get("fechaSolicitada") or registro.get("creadoEn")
            if isinstance(fecha_val, datetime):
                fecha_text = fecha_val.strftime("%Y-%m-%d %H:%M")
            else:
                fecha_text = str(fecha_val or "")
            motivo = str(
                registro.get("motivo")
                or registro.get("Motivo")
                or ""
            )

            item_id = self.tree.insert(
                "",
                "end",
                values=(doc_id, estado, solicitante, fecha_text, motivo),
            )
            self._tree_items_info[item_id] = registro

    def actualizar(self) -> None:
        try:
            self.cargar_peticiones(dias=60)
        except Exception:
            return
        self.aplicar_filtros()

    def _obtener_item_seleccionado(self) -> Optional[str]:
        seleccionado = self.tree.focus()
        if seleccionado:
            return seleccionado
        seleccion = self.tree.selection()
        return seleccion[0] if seleccion else None

    def responder_seleccionada(self) -> None:
        item_id = self._obtener_item_seleccionado()
        if not item_id:
            messagebox.showinfo("Responder", "Selecciona una petición primero.")
            return
        self._responder_item(item_id)

    def _responder_item(self, item_id: str) -> None:
        if self._operacion_en_progreso:
            return

        registro = self._tree_items_info.get(item_id)
        if not registro:
            messagebox.showerror(
                "Error",
                "No se encontró la información de la petición seleccionada.",
            )
            return

        estado_actual = str(
            registro.get("estado")
            or registro.get("Admitido")
            or "PENDIENTE"
        ).upper()
        if estado_actual != "PENDIENTE":
            messagebox.showwarning(
                "Solicitud respondida",
                "Esta petición ya cuenta con una respuesta registrada.",
            )
            return

        self._operacion_en_progreso = True
        try:
            responder_peticion(
                self.root,
                registro.get("__id") or registro.get("doc_id") or "",
                registro.get("uidSolicitante") or registro.get("uid") or "",
                lambda: self.root.after(0, self.actualizar),
                db_client=self.db,
                responded_by=RESPONDER_IDENTIDAD,
            )
        finally:
            self._operacion_en_progreso = False

    def _parse_fecha(self, valor: str, campo: str) -> Optional[date]:
        valor = (valor or "").strip()
        if not valor:
            return None
        try:
            return datetime.strptime(valor, "%Y-%m-%d").date()
        except ValueError as exc:  # noqa: B904
            raise ValueError(f"{campo} debe tener formato YYYY-MM-DD.") from exc

    def aplicar_filtros(self) -> None:
        estado = self.var_estado.get().strip().upper()
        texto = self.var_texto.get().strip().lower()

        try:
            fecha_desde = self._parse_fecha(self.var_fecha_desde.get(), "Fecha desde")
            fecha_hasta = self._parse_fecha(self.var_fecha_hasta.get(), "Fecha hasta")
        except ValueError as exc:
            messagebox.showerror("Filtros", str(exc))
            return

        if fecha_desde and fecha_hasta and fecha_desde > fecha_hasta:
            messagebox.showerror(
                "Filtros",
                "La fecha desde no puede ser mayor que la fecha hasta.",
            )
            return

        resultados: List[Dict[str, Any]] = []
        for registro in self._peticiones_raw:
            estado_actual = str(
                registro.get("estado")
                or registro.get("Admitido")
                or "PENDIENTE"
            ).upper()
            if estado and estado_actual != estado:
                continue

            fecha_base = registro.get("fechaSolicitada") or registro.get("creadoEn")
            fecha_base_dt = None
            if isinstance(fecha_base, datetime):
                fecha_base_dt = fecha_base.date()
            elif isinstance(fecha_base, str):
                parsed = self._to_py_dt(fecha_base)
                if parsed:
                    fecha_base_dt = parsed.date()

            if fecha_desde and (fecha_base_dt is None or fecha_base_dt < fecha_desde):
                continue
            if fecha_hasta and (fecha_base_dt is None or fecha_base_dt > fecha_hasta):
                continue

            if texto:
                campos = [
                    str(registro.get("uidSolicitante") or registro.get("uid") or ""),
                    str(registro.get("nombreSolicitante") or registro.get("Nombre") or ""),
                    str(registro.get("motivo") or registro.get("Motivo") or ""),
                    str(registro.get("comentario") or registro.get("comentarios") or ""),
                ]
                combinado = " ".join(c.lower() for c in campos)
                if texto not in combinado:
                    continue

            resultados.append(registro)

        self._pintar_en_tree(resultados)

    def limpiar_filtros(self) -> None:
        self.var_estado.set("")
        try:
            self.cmb_estado.current(0)
        except Exception:
            pass
        self.var_fecha_desde.set("")
        self.var_fecha_hasta.set("")
        self.var_texto.set("")
        self._pintar_en_tree(self._peticiones_raw)

    def exportar_csv(self) -> None:
        items = self.tree.get_children("")
        if not items:
            messagebox.showinfo("Exportar CSV", "No hay datos para exportar.")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            initialfile="peticiones.csv",
        )
        if not path:
            return

        columnas = ["ID", "Estado", "Solicitante", "Fecha", "Motivo"]
        with open(path, "w", newline="", encoding="utf-8-sig") as archivo:
            writer = csv.writer(archivo, delimiter=";")
            writer.writerow(columnas)
            for item_id in items:
                writer.writerow(self.tree.item(item_id, "values"))
        messagebox.showinfo("Exportar CSV", "Exportación completada.")

    def _on_tree_double_click(self, event) -> None:
        item_id = self.tree.identify_row(event.y)
        if not item_id:
            return
        self._responder_item(item_id)

    def _on_tree_motion(self, event) -> None:
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            self._clear_tooltip()
            return

        column = self.tree.identify_column(event.x)
        if column != "#5":
            self._clear_tooltip()
            return

        item_id = self.tree.identify_row(event.y)
        if not item_id:
            self._clear_tooltip()
            return

        texto = (self.tree.set(item_id, "motivo") or "").strip()
        if not texto or len(texto) <= 48:
            self._clear_tooltip()
            return

        if (
            self._tooltip_state.get("item") == item_id
            and self._tooltip_state.get("text") == texto
        ):
            return

        self._tooltip_state = {"item": item_id, "text": texto}
        self.tooltip.show(texto, event.x, event.y)

def abrir_gestion_peticiones(db: firestore.Client, sa_path: Optional[str], project_id: Optional[str]) -> None:
    global ventana_peticiones, ventana_peticiones_app, SERVICE_ACCOUNT_JSON, PROJECT_ID

    if not sa_path or not project_id:
        messagebox.showerror("Error", "Faltan credenciales de Firebase.")
        return

    SERVICE_ACCOUNT_JSON = sa_path
    PROJECT_ID = project_id

    if ventana_peticiones and ventana_peticiones.winfo_exists():
        ventana_peticiones.lift()
        ventana_peticiones.focus_force()
        return

    ventana_peticiones = tk.Toplevel()
    ventana_peticiones.title("Gestión de Peticiones de Días Libres")
    ventana_peticiones.geometry("900x600")
    ventana_peticiones.minsize(760, 480)

    ventana_peticiones.grid_rowconfigure(2, weight=1)
    ventana_peticiones.grid_columnconfigure(0, weight=1)

    def _after_close() -> None:
        global ventana_peticiones, ventana_peticiones_app
        ventana_peticiones = None
        ventana_peticiones_app = None

    app = GestionPeticionesUI(ventana_peticiones, db, _after_close)
    ventana_peticiones_app = app
    ventana_peticiones.focus_force()

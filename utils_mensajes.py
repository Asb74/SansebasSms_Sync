import datetime
from typing import Optional

from google.cloud import firestore


def reenviar_mensaje(db: firestore.Client, mensaje_id: str, force: bool = True):
    """Reenvía un mensaje existente restableciendo su estado de negocio."""

    doc_ref = db.collection("Mensajes").document(mensaje_id)
    snap = doc_ref.get()
    if not getattr(snap, "exists", False):
        raise ValueError(f"Mensaje no existe: {mensaje_id}")
    data = snap.to_dict() or {}

    patch = {
        "estado": "Pendiente",
        "respuesta": None,
        "respuestaEn": None,
        "pushEstado": None,
        "pushEnviados": 0,
        "pushFallidos": 0,
        "pushError": None,
    }
    doc_ref.update(patch)
    data.update(patch)

    uid = str(data.get("uid", ""))
    user = {}
    if uid:
        user_snap = db.collection("UsuariosAutorizados").document(uid).get()
        if getattr(user_snap, "exists", False):
            user = user_snap.to_dict() or {}

    from notificaciones_push import enviar_push_por_mensaje

    return enviar_push_por_mensaje(
        db,
        mensaje_id,
        data,
        user,
        actualizar_estado=True,
        force=force,
    )


def build_mensaje_id(uid: str, dt: Optional[datetime.datetime] = None) -> str:
    """Construye un ID de mensaje en formato UID_YYYY-MM-DDTHH:MM:SS.mmmmmm."""
    if dt is None:
        dt = datetime.datetime.now(datetime.timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    iso = dt.isoformat(timespec="microseconds")
    return f"{uid}_{iso}"

import json
import logging
import os

from firebase_admin import messaging
from google.cloud import firestore

NOTI_DB = "notificados.json"  # desduplicador por MensajeID


def _load_notificados() -> set[str]:
    if not os.path.exists(NOTI_DB):
        return set()
    try:
        with open(NOTI_DB, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            ids = data.get("ids", [])
        elif isinstance(data, list):
            ids = data
        else:
            ids = []
        return {str(i) for i in ids if i}
    except Exception:
        logging.exception("No se pudo leer notificados.json")
        return set()


def _save_notificados(ids: set[str]) -> None:
    try:
        with open(NOTI_DB, "w", encoding="utf-8") as f:
            json.dump({"ids": sorted(ids)}, f, ensure_ascii=False, indent=2)
    except Exception:
        logging.exception("No se pudo guardar notificados.json")


def _title_body_from_doc(doc_data: dict) -> tuple[str, str]:
    titulo = doc_data.get("mensaje") or "Aviso"
    cuerpo = doc_data.get("cuerpo") or doc_data.get("texto") or "Tienes un nuevo mensaje."
    return str(titulo), str(cuerpo)


def _tokens_from_user(user_doc: dict) -> list[str]:
    tok = user_doc.get("fcmToken")
    if not tok:
        return []
    if isinstance(tok, str):
        return [tok]
    if isinstance(tok, (list, tuple)):
        return [t for t in tok if t]
    return []


def _quitar_tokens_invalidos(
    db: firestore.Client,
    mensaje_data: dict,
    usuario: dict,
    tokens_a_remover: list[str],
) -> None:
    if not tokens_a_remover:
        return

    uid = mensaje_data.get("uid") or usuario.get("UID") or usuario.get("uid")
    if not uid:
        return

    ref = db.collection("UsuariosAutorizados").document(str(uid))
    actual = usuario.get("fcmToken")
    try:
        if isinstance(actual, list):
            nuevos = [t for t in actual if t not in tokens_a_remover]
            ref.update({"fcmToken": nuevos})
            usuario["fcmToken"] = nuevos
        elif isinstance(actual, str):
            if actual in tokens_a_remover:
                ref.update({"fcmToken": firestore.DELETE_FIELD})
                usuario.pop("fcmToken", None)
        else:
            ref.update({"fcmToken": firestore.DELETE_FIELD})
            usuario.pop("fcmToken", None)
    except Exception:
        logging.exception("No se pudo actualizar tokens inválidos para %s", uid)


def enviar_push_por_mensaje(
    db: firestore.Client,
    mensaje_id: str,  # id del doc en Mensajes
    mensaje_data: dict,  # contenido del doc recién guardado
    usuario: dict,  # doc de UsuariosAutorizados del destinatario (por uid)
    actualizar_estado: bool = True,
    *,
    force: bool = False,
) -> dict:
    """Devuelve dict con {enviados:int, fallidos:int}. No lanza si ya fue enviado (dedupe)."""

    enviados = fallidos = 0
    dedupe = _load_notificados()
    if not force and mensaje_id in dedupe:
        logging.info("Notificación ya enviada para %s (dedupe)", mensaje_id)
        return {"enviados": 0, "fallidos": 0}

    tokens = _tokens_from_user(usuario)
    if not tokens:
        logging.warning("Usuario sin fcmToken; uid=%s", usuario.get("UID") or usuario.get("uid"))
        if actualizar_estado:
            db.collection("Mensajes").document(mensaje_id).update({
                "estado": "SinToken",
                "pushError": "Usuario sin fcmToken",
                "pushEnviadoEn": firestore.SERVER_TIMESTAMP,
                "pushFallidos": 1,
                "pushEnviados": 0,
            })
        return {"enviados": 0, "fallidos": 1}

    titulo, cuerpo = _title_body_from_doc(mensaje_data)
    data_payload = {
        "mensajeId": mensaje_id,
        "uid": str(mensaje_data.get("uid", "")),
        "tipo": str(mensaje_data.get("tipo", "")),
        "dia": str(mensaje_data.get("dia", "")),
        "hora": str(mensaje_data.get("hora", "")),
        "click_action": "FLUTTER_NOTIFICATION_CLICK",
        "route": "/usuario",  # tu app lo usa para abrir UsuarioScreen
    }

    batch = []
    for t in tokens:
        batch.append(
            messaging.Message(
                notification=messaging.Notification(title=titulo, body=cuerpo),
                token=t,
                data=data_payload,
            )
        )

    try:
        resp = messaging.send_each(batch, dry_run=False)
        errores: list[str] = []
        tokens_a_remover: list[str] = []
        for token, resultado in zip(tokens, resp.responses):
            if resultado.success:
                enviados += 1
                continue
            fallidos += 1
            exc = resultado.exception
            mensaje_error = getattr(exc, "message", None) or str(exc)
            if mensaje_error:
                errores.append(mensaje_error)
            if isinstance(exc, messaging.UnregisteredError):
                tokens_a_remover.append(token)

        estado_final = "OK"
        if fallidos:
            estado_final = "Parcial" if enviados > 0 else "ErrorPush"

        if actualizar_estado:
            update_payload = {
                "estado": estado_final,
                "pushEnviadoEn": firestore.SERVER_TIMESTAMP,
                "pushFallidos": fallidos,
                "pushEnviados": enviados,
            }
            if errores:
                update_payload["pushError"] = "; ".join(dict.fromkeys(errores))
            else:
                update_payload["pushError"] = firestore.DELETE_FIELD
            db.collection("Mensajes").document(mensaje_id).update(update_payload)

        if tokens_a_remover:
            _quitar_tokens_invalidos(db, mensaje_data, usuario, tokens_a_remover)

        if enviados > 0:
            dedupe.add(mensaje_id)
            _save_notificados(dedupe)

        return {"enviados": enviados, "fallidos": fallidos}
    except Exception as e:
        logging.exception("Error enviando push")
        if actualizar_estado:
            db.collection("Mensajes").document(mensaje_id).update({
                "estado": "ErrorPush",
                "pushError": str(e),
                "pushEnviadoEn": firestore.SERVER_TIMESTAMP,
                "pushFallidos": len(tokens),
                "pushEnviados": 0,
            })
        return {"enviados": 0, "fallidos": len(tokens)}

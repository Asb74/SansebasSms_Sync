import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:cloud_functions/cloud_functions.dart';
import 'package:firebase_auth/firebase_auth.dart';
import 'package:flutter/material.dart';

enum _RespuestaDiaLibre { aprobar, denegar, cancelar }

Future<void> responderPeticionDiaLibre(
  BuildContext context, {
  required String solicitudId,
  required String solicitanteUid,
}) async {
  final accion = await showDialog<_RespuestaDiaLibre>(
    context: context,
    builder: (dialogContext) {
      return AlertDialog(
        title: const Text('Responder petición'),
        content: const Text('¿Qué deseas hacer con la petición de día libre?'),
        actions: [
          TextButton(
            onPressed: () =>
                Navigator.of(dialogContext).pop(_RespuestaDiaLibre.denegar),
            child: const Text('⛔ Denegar'),
          ),
          TextButton(
            onPressed: () =>
                Navigator.of(dialogContext).pop(_RespuestaDiaLibre.aprobar),
            child: const Text('✅ OK'),
          ),
          TextButton(
            onPressed: () =>
                Navigator.of(dialogContext).pop(_RespuestaDiaLibre.cancelar),
            child: const Text('❌ Cancelar'),
          ),
        ],
      );
    },
  );

  if (accion == null || accion == _RespuestaDiaLibre.cancelar) {
    return;
  }

  final firestore = FirebaseFirestore.instance;
  final functions = FirebaseFunctions.instance;
  final ahora = FieldValue.serverTimestamp();
  final uidActual = FirebaseAuth.instance.currentUser?.uid;

  final estado = accion == _RespuestaDiaLibre.aprobar ? 'APROBADO' : 'DENEGADO';
  final body = accion == _RespuestaDiaLibre.aprobar
      ? 'Tu petición de día libre ha sido aprobada.'
      : 'Tu petición de día libre ha sido denegada.';

  try {
    await firestore.collection('PeticionesDiaLibre').doc(solicitudId).update({
      'estado': estado,
      'respondidoPor': uidActual,
      'respondidoEn': ahora,
    });
  } catch (error) {
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(content: Text('No se pudo actualizar la petición: $error')),
    );
    return;
  }

  bool notificacionEnviada = false;
  try {
    final callable = functions.httpsCallable('notificarRespuestaDiaLibre');
    await callable.call(<String, dynamic>{
      'solicitudId': solicitudId,
      'solicitanteUid': solicitanteUid,
      'estado': estado,
    });
    notificacionEnviada = true;
  } on FirebaseFunctionsException catch (error) {
    debugPrint(
      'No fue posible invocar notificarRespuestaDiaLibre: ${error.message}',
    );
  } catch (error) {
    debugPrint('Fallo general al invocar notificarRespuestaDiaLibre: $error');
  }

  if (!notificacionEnviada) {
    try {
      await firestore.collection('NotificacionesPendientes').add({
        'uid': solicitanteUid,
        'titulo': 'Respuesta a tu petición',
        'body': body,
        'tipo': 'dia_libre',
        'solicitudId': solicitudId,
        'createdAt': FieldValue.serverTimestamp(),
      });
    } catch (error) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('No se pudo programar la notificación: $error')),
      );
      return;
    }
  }

  ScaffoldMessenger.of(context).showSnackBar(
    SnackBar(
      content: Text(
        accion == _RespuestaDiaLibre.aprobar
            ? 'Petición aprobada y notificación enviada.'
            : 'Petición denegada y notificación enviada.',
      ),
    ),
  );
}

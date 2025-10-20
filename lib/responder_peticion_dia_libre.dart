import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:firebase_auth/firebase_auth.dart';
import 'package:cloud_functions/cloud_functions.dart';
import 'package:flutter/material.dart';

/// Permite responder una petición de día libre mostrando un diálogo seguro.
Future<void> responderPeticionDiaLibre(
  BuildContext context, {
  required String solicitudId,
  required String solicitanteUid,
}) async {
  final resultado = await showDialog<_RespuestaDiaLibre>(
    context: context,
    barrierDismissible: true,
    builder: (dialogContext) {
      return AlertDialog(
        title: const Text('Responder petición'),
        content: const Text(
          'Selecciona la acción que deseas realizar para esta petición de día libre.',
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(dialogContext).pop(),
            child: const Text('❌ Cancelar'),
          ),
          TextButton(
            onPressed: () =>
                Navigator.of(dialogContext).pop(_RespuestaDiaLibre.denegado),
            child: const Text('⛔ Denegar'),
          ),
          TextButton(
            onPressed: () =>
                Navigator.of(dialogContext).pop(_RespuestaDiaLibre.aprobado),
            child: const Text('✅ OK'),
          ),
        ],
      );
    },
  );

  if (resultado == null) {
    return;
  }

  final firestore = FirebaseFirestore.instance;
  final auth = FirebaseAuth.instance;
  final ahora = FieldValue.serverTimestamp();
  final estado =
      resultado == _RespuestaDiaLibre.aprobado ? 'APROBADO' : 'DENEGADO';

  try {
    await firestore.collection('PeticionesDiaLibre').doc(solicitudId).update({
      'estado': estado,
      'respondidoPor': auth.currentUser?.uid,
      'respondidoEn': ahora,
    });
  } catch (error) {
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text('No se pudo actualizar la petición: $error'),
      ),
    );
    return;
  }

  final mensajeBody = estado == 'APROBADO'
      ? 'Tu petición de día libre ha sido aprobada.'
      : 'Tu petición de día libre ha sido denegada.';

  final funciones = FirebaseFunctions.instance;
  var notificacionEnviada = false;
  try {
    final callable =
        funciones.httpsCallable('notificarRespuestaDiaLibre');
    await callable.call({
      'uid': solicitanteUid,
      'solicitudId': solicitudId,
      'estado': estado,
      'body': mensajeBody,
    });
    notificacionEnviada = true;
  } on FirebaseFunctionsException catch (_) {
    // Ignoramos y probamos con el fallback.
  } catch (_) {
    // Ignoramos y probamos con el fallback.
  }

  if (!notificacionEnviada) {
    try {
      await firestore.collection('NotificacionesPendientes').add({
        'uid': solicitanteUid,
        'titulo': 'Respuesta a tu petición',
        'body': mensajeBody,
        'tipo': 'dia_libre',
        'solicitudId': solicitudId,
        'createdAt': FieldValue.serverTimestamp(),
      });
    } catch (error) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text(
            'Se respondió la petición pero no se pudo programar la notificación: '
            '$error',
          ),
        ),
      );
      return;
    }
  }

  final snackBarMensaje =
      estado == 'APROBADO' ? 'Petición aprobada' : 'Petición denegada';
  ScaffoldMessenger.of(context).showSnackBar(
    SnackBar(content: Text(snackBarMensaje)),
  );
}

enum _RespuestaDiaLibre { aprobado, denegado }

# Sansebassms Sync

## Diagnóstico de FCM en Flutter

Para verificar que la app móvil usa el mismo proyecto que este panel, imprime el `projectId` en Flutter:

```dart
import 'package:firebase_core/firebase_core.dart';

void logProjectId() {
  final options = Firebase.app().options;
  // TODO: reemplaza por tu propio logger o print
  print('Firebase projectId: \${options.projectId}');
}
```

Asegúrate de llamar a esta función durante el arranque (por ejemplo, tras `Firebase.initializeApp()`) para comparar el valor con el `project_id` mostrado en el diagnóstico de la herramienta.


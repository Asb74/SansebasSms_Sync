
; ==============================
; Instalador Inno Setup 6.4.3 - Sansebassms Sync (corregido)
; ==============================

; ---- Datos básicos de la app ----
#define MyAppName        "Sansebassms Sync"
#define MyAppVersion     "1.0.0"
#define MyAppPublisher   "Sansebas"
#define MyAppURL         "https://example.com"
#define MyAppExeName     "Sansebassms Sync.exe"
#define MyBuildFolder    "SansebasSms_Sync"
#define MyIconFile       MyBuildFolder + "\\icono_app.ico"

; ---- Salida del instalador ----
#define OutputBaseName   MyAppName + "-Setup-" + MyAppVersion
#define OutputDir        "output"

; ---- GUID único del producto (DOBLE LLAVE para escapar) ----
#define MyAppId "{{369EA0ED-4D50-44AB-B1BD-37BF28FEF338}}"

[Setup]
AppId={#MyAppId}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}
DefaultDirName={localappdata}\{#MyAppName}
DefaultGroupName={#MyAppName}
DisableDirPage=no
DisableProgramGroupPage=no
; Idiomas
UsePreviousLanguage=no
Compression=lzma
SolidCompression=yes
OutputDir={#OutputDir}
OutputBaseFilename={#OutputBaseName}
WizardStyle=modern
PrivilegesRequired=lowest
UninstallDisplayIcon={app}\{#MyAppExeName}
; Icono del instalador (opcional)
#ifdef MyIconFile
SetupIconFile={#MyIconFile}
#endif
; LicenseFile=license.txt  ; opcional

; ---- Idiomas ----
[Languages]
Name: "es"; MessagesFile: "compiler:Languages\Spanish.isl"
Name: "en"; MessagesFile: "compiler:Default.isl"

; ---- Archivos a incluir ----
[Files]
Source: "{#MyBuildFolder}\{#MyAppExeName}"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#MyBuildFolder}\*"; Excludes: "{#MyBuildFolder}\{#MyAppExeName}"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

; ---- Accesos directos ----
[Icons]
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{group}\Desinstalar {#MyAppName}"; Filename: "{uninstallexe}"
Name: "{userdesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

; ---- Tareas opcionales ----
[Tasks]
Name: "desktopicon"; Description: "Crear icono en el escritorio"; GroupDescription: "Tareas opcionales:"; Flags: unchecked

; ---- Ejecutar al finalizar ----
[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "Iniciar {#MyAppName}"; Flags: nowait postinstall skipifsilent

; ---- Desinstalación limpia (opcional) ----
; [UninstallDelete]
; Type: filesandordirs; Name: "{localappdata}\{#MyAppName}\logs"

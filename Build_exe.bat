@echo off
setlocal
REM === Ir a la carpeta del script (.bat) ===
cd /d "%~dp0"

REM ====== Config ======
set "APP_NAME=Sansebassms Sync"
set "PYLAUNCHER=py -3.11"
set "MODE=onefile"
if /i "%~1"=="onedir" set "MODE=onedir"

REM ====== Crear y activar venv si no existe ======
if not exist ".venv\Scripts\python.exe" (
  %PYLAUNCHER% -m venv .venv
)
call ".venv\Scripts\activate"

REM ====== Dependencias mínimas para build ======
python -m pip install --upgrade pip
pip install pyinstaller pillow firebase-admin google-cloud-firestore google-auth requests python-dateutil pandas openpyxl pytz

REM ====== Si falta .ico pero hay .png, generar .ico ======
if not exist "icono_app.ico" if exist "icono_app.png" (
  python -c "from PIL import Image; im=Image.open('icono_app.png').convert('RGBA'); im.save('icono_app.ico', sizes=[(256,256),(128,128),(64,64),(32,32),(16,16)])"
)

REM ====== Opciones dinámicas (icono y recursos) ======
set "ICON_OPT="
if exist "icono_app.ico" set "ICON_OPT=--icon ""icono_app.ico"""

set "ADDDATA_OPT="
if exist "icono_app.png" set "ADDDATA_OPT=--add-data ""icono_app.png;."""

REM ====== Modo build ======
set "MODE_OPT=--onefile"
if /i "%MODE%"=="onedir" set "MODE_OPT=--onedir"

REM ====== Credenciales (si está sansebassms.json junto al exe, úsalo) ======
if not defined SANSEBASSMS_CREDENTIALS (
  if exist "sansebassms.json" set "SANSEBASSMS_CREDENTIALS=%CD%\sansebassms.json"
)

REM ====== Limpiar build anterior y compilar ======
pyinstaller --noconfirm --clean ^
  --noconsole ^
  %MODE_OPT% ^
  --name "%APP_NAME%" ^
  %ICON_OPT% ^
  %ADDDATA_OPT% ^
  --collect-all google ^
  --collect-all grpc ^
  --collect-all pandas ^
  --collect-all openpyxl ^
  --collect-all pytz ^
  --hidden-import google.rpc ^
  main.py

REM ====== Mostrar ruta de salida ======
if /i "%MODE%"=="onefile" (
  echo.
  echo Ejecutable generado en: "dist\%APP_NAME%.exe"
) else (
  echo.
  echo Ejecutable generado en: "dist\%APP_NAME%\%APP_NAME%.exe"
)

echo.
echo Si el exe tarda en iniciar, prueba el modo: build_exe.bat onedir
echo.
pause


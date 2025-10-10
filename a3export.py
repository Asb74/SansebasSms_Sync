#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extrae A3EXPORT.ZIP con contraseña y genera:
- resumen_contenidos.csv (listado de archivos con tamaños y mime)
- previews.json (primeras líneas de ficheros de texto)
- output_csv/ (CSV normalizados para TXT/CSV/TSV detectando delimitador y encoding)

Uso con args (opcional):
  python a3export.py --zip A3EXPORT.ZIP --password "TU_PASSWORD" [--out "./salida"] [--sevenz "C:\\Program Files\\7-Zip\\7z.exe"] [--clean]

Si omites --zip o --password, el script te los pedirá:
- Intentará abrir un diálogo para elegir el ZIP (si hay GUI).
- Pedirá la contraseña de forma oculta (getpass).
"""

import argparse
import csv
import json
import mimetypes
import os
import shutil
import subprocess
import sys
from pathlib import Path
import getpass

import chardet
try:
    import pyzipper
except ImportError:
    pyzipper = None

TEXT_EXTS = {".txt", ".csv", ".tsv", ".log", ".json", ".xml", ".ini"}
MAX_PREVIEW_BYTES = 1_000_000  # 1 MB
DEFAULT_OUT_SUFFIX = "_extracted"

def detect_7z_path(explicit_7z: str | None) -> str | None:
    if explicit_7z:
        p = Path(explicit_7z)
        return str(p) if p.exists() else None
    try:
        subprocess.run(["7z", "-h"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        return "7z"
    except Exception:
        pass
    for c in (r"C:\Program Files\7-Zip\7z.exe", r"C:\Program Files (x86)\7-Zip\7z.exe"):
        if Path(c).exists():
            return c
    return None

def try_extract_with_pyzipper(zip_path: Path, out_dir: Path, password: str) -> bool:
    if pyzipper is None:
        print("⚠️  pyzipper no está instalado. Saltando a 7z si está disponible.")
        return False
    try:
        with pyzipper.AESZipFile(zip_path, "r") as zf:
            zf.pwd = password.encode("utf-8")
            zf.extractall(out_dir)
        return True
    except Exception as e:
        print(f"ℹ️  pyzipper no pudo extraer el ZIP ({e}). Intentaremos con 7z si está disponible.")
        return False

def try_extract_with_7z(zip_path: Path, out_dir: Path, password: str, sevenz_path: str) -> bool:
    cmd = [sevenz_path, "x", "-y", f"-o{out_dir}", f"-p{password}", str(zip_path)]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode == 0:
            return True
        print("❌  7z falló al extraer.")
        print("---- STDOUT ----")
        print(proc.stdout)
        print("---- STDERR ----")
        print(proc.stderr)
        return False
    except FileNotFoundError:
        print("❌  7z no encontrado.")
        return False

def summarize_directory(out_dir: Path) -> list[dict]:
    rows = []
    for root, _, files in os.walk(out_dir):
        for fname in files:
            fp = Path(root) / fname
            rel = fp.relative_to(out_dir).as_posix()
            mime, _ = mimetypes.guess_type(fp.name)
            rows.append({
                "filename": rel,
                "size_bytes": fp.stat().st_size,
                "mime_guess": mime or "desconocido"
            })
    rows.sort(key=lambda r: r["filename"].lower())
    return rows

def read_text_preview(file_path: Path, max_bytes=MAX_PREVIEW_BYTES) -> str:
    raw = file_path.read_bytes()[:max_bytes]
    det = chardet.detect(raw)
    encoding = det.get("encoding") or "utf-8"
    try:
        text = raw.decode(encoding, errors="replace")
    except Exception:
        text = raw.decode("utf-8", errors="replace")
    lines = text.splitlines()
    head = "\n".join(lines[:20])
    return head

def sniff_delimiter(sample: str) -> str | None:
    try:
        import csv as _csv
        dialect = _csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|", ":"])
        return dialect.delimiter
    except Exception:
        for cand in [";", ",", "\t", "|", ":"]:
            if cand in sample:
                return cand
    return None

def normalize_to_csv(file_path: Path, out_csv_dir: Path) -> Path | None:
    ext = file_path.suffix.lower()
    if ext not in {".csv", ".tsv", ".txt"}:
        return None
    raw = file_path.read_bytes()[:200_000]
    det = chardet.detect(raw)
    encoding = det.get("encoding") or "utf-8"
    text = raw.decode(encoding, errors="replace")
    delim = sniff_delimiter(text) or ";"
    data_raw = file_path.read_bytes()
    text_full = data_raw.decode(encoding, errors="replace")
    out_csv_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_csv_dir / (file_path.stem + ".csv")
    reader = csv.reader(text_full.splitlines(), delimiter=delim)
    with open(out_csv, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, delimiter=";")
        for row in reader:
            w.writerow(row)
    return out_csv

def interactive_inputs(args):
    """Pide ZIP, password y out si no se proporcionaron por CLI."""
    zip_path = args.zip
    password = args.password
    out_dir = args.out

    if not zip_path:
        # Intentar selector de archivos si hay GUI
        chosen = None
        try:
            import tkinter as tk
            from tkinter import filedialog
            root = tk.Tk()
            root.withdraw()
            chosen = filedialog.askopenfilename(
                title="Selecciona el archivo ZIP",
                filetypes=[("ZIP files", "*.zip"), ("Todos", "*.*")]
            )
            root.destroy()
        except Exception:
            chosen = None

        if not chosen:
            # Consola
            chosen = input("Ruta del ZIP (puedes arrastrar el archivo aquí y pulsar Enter): ").strip().strip('"')
        zip_path = chosen

    if not password:
        password = getpass.getpass("Contraseña del ZIP: ")

    # Si no se indicó out, por defecto creamos <zipname>_extracted junto al ZIP
    if not out_dir:
        zp = Path(zip_path)
        out_dir = str(zp.with_suffix("").as_posix() + DEFAULT_OUT_SUFFIX)

    return zip_path, password, out_dir

def main():
    ap = argparse.ArgumentParser(description="Extraer y previsualizar ZIP con password (A3EXPORT).", add_help=True)
    ap.add_argument("--zip", help="Ruta al ZIP (si no se pasa, se pedirá).")
    ap.add_argument("--password", help="Contraseña del ZIP (si no se pasa, se pedirá).")
    ap.add_argument("--out", help="Carpeta de salida (por defecto <zipname>_extracted junto al ZIP).")
    ap.add_argument("--sevenz", help="Ruta al ejecutable 7z (opcional). Si no se indica, se buscará en PATH.")
    ap.add_argument("--clean", action="store_true", help="Borrar la carpeta de salida si existe.")
    args = ap.parse_args()

    # Interactivo si faltan zip o password (o out)
    zip_str, password, out_str = interactive_inputs(args)

    zip_path = Path(zip_str).resolve()
    out_dir = Path(out_str).resolve()
    out_csv_dir = out_dir / "output_csv"

    if not zip_path.exists():
        print(f"❌ No se encuentra el archivo: {zip_path}")
        sys.exit(1)

    if args.clean and out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("📦 Preparando extracción...")
    # 1) pyzipper (AES)
    extracted = try_extract_with_pyzipper(zip_path, out_dir, password)

    # 2) 7z fallback (Deflate64 u otros)
    if not extracted:
        sevenz = detect_7z_path(args.sevenz)
        if not sevenz:
            print("❌ No se pudo extraer con pyzipper y no se encontró 7z.")
            print("   → Instala 7-Zip y vuelve a ejecutar, o descomprime manualmente.")
            sys.exit(2)
        print(f"🧰 Reintentando con 7z: {sevenz}")
        extracted = try_extract_with_7z(zip_path, out_dir, password, sevenz)

    if not extracted:
        print("❌ No se pudo extraer el ZIP con los métodos disponibles.")
        sys.exit(3)

    print(f"✅ Extraído en: {out_dir}")

    # Resumen de contenidos
    rows = summarize_directory(out_dir)
    try:
        import pandas as pd
        resumen_csv = out_dir / "resumen_contenidos.csv"
        pd.DataFrame(rows).to_csv(resumen_csv, index=False, encoding="utf-8")
        print(f"📝 Resumen guardado en: {resumen_csv}")
    except Exception:
        # Sin pandas, guardamos como JSON
        resumen_json = out_dir / "resumen_contenidos.json"
        with open(resumen_json, "w", encoding="utf-8") as fh:
            json.dump(rows, fh, ensure_ascii=False, indent=2)
        print(f"📝 Resumen guardado en: {resumen_json} (instala pandas para CSV)")

    # Previews + normalización
    previews = {}
    normalized = []
    for root, _, files in os.walk(out_dir):
        for fname in files:
            fp = Path(root) / fname
            rel = fp.relative_to(out_dir)
            if fp.suffix.lower() in TEXT_EXTS and fp.stat().st_size <= MAX_PREVIEW_BYTES:
                try:
                    previews[str(rel)] = read_text_preview(fp)
                except Exception as e:
                    previews[str(rel)] = f"<<Error leyendo: {e}>>"

            out_csv = normalize_to_csv(fp, out_csv_dir)
            if out_csv:
                normalized.append(str(out_csv.relative_to(out_dir)))

    previews_path = out_dir / "previews.json"
    with open(previews_path, "w", encoding="utf-8") as fh:
        json.dump(previews, fh, ensure_ascii=False, indent=2)
    print(f"🔎 Previews guardados en: {previews_path}")

    if normalized:
        print("📂 CSV normalizados en:", out_csv_dir)
        for p in normalized:
            print("   -", p)

    print("\n🎯 Siguiente paso:")
    print("   - Revisa 'resumen_contenidos.csv/json' y 'output_csv/'.")
    print("   - Dime qué tablas quieres subir a Firestore y te doy el módulo de ingesta para SansebasSms Sync.")

if __name__ == "__main__":
    main()

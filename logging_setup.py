import datetime
import logging
import os
import sys
import threading
import traceback
from pathlib import Path

_LOG_LOCK = threading.Lock()
_LOGGING_CONFIGURED = False
ERROR_LOG_FILE = Path("error_log.txt")


def _configure_logging() -> None:
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return
    with _LOG_LOCK:
        if _LOGGING_CONFIGURED:
            return
        logs_dir = Path("logs")
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_filename = logs_dir / f"sansebassms_{datetime.datetime.now():%Y%m%d}.log"
        handlers: list[logging.Handler] = [
            logging.FileHandler(log_filename, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ]
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            handlers=handlers,
        )
        _LOGGING_CONFIGURED = True


def _write_error_log(exc_type, exc_value, exc_traceback) -> None:
    try:
        ERROR_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    try:
        with ERROR_LOG_FILE.open("a", encoding="utf-8") as fh:
            fh.write("\n" + "=" * 80 + "\n")
            fh.write(datetime.datetime.now().isoformat(timespec="seconds"))
            fh.write("\n")
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=fh)
    except Exception:
        logging.getLogger(__name__).exception("No se pudo escribir en error_log.txt")


def install_global_excepthook() -> None:
    """Instala un excepthook que registra errores sin finalizar la app."""

    _configure_logging()
    logger = logging.getLogger("global_excepthook")

    def handle_exception(exc_type, exc_value, exc_traceback):
        if exc_value is None:
            exc_value = exc_type()
        logger.error("Excepción no controlada", exc_info=(exc_type, exc_value, exc_traceback))
        _write_error_log(exc_type, exc_value, exc_traceback)

    sys.excepthook = handle_exception

    try:
        import tkinter as tk

        def report_callback_exception(self, exc, val, tb):
            handle_exception(exc, val, tb)

        tk.Tk.report_callback_exception = report_callback_exception  # type: ignore[attr-defined]
    except Exception:
        logger.debug("No se pudo parchear report_callback_exception", exc_info=True)

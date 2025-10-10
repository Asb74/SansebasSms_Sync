import datetime
import logging
import os
import sys
import threading
import traceback
from typing import Optional

_LOG_CONFIGURED = False


def _ensure_logging_configured() -> str:
    global _LOG_CONFIGURED
    if _LOG_CONFIGURED:
        # Already configured; return latest log file path for reference.
        today = datetime.datetime.now().strftime("%Y%m%d")
        return os.path.join("logs", f"sansebassms_{today}.log")

    os.makedirs("logs", exist_ok=True)
    today = datetime.datetime.now().strftime("%Y%m%d")
    log_path = os.path.join("logs", f"sansebassms_{today}.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(threadName)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    _LOG_CONFIGURED = True
    logging.debug("Logging configured. Writing to %s", log_path)
    return log_path


def _write_error_log(message: str) -> None:
    try:
        with open("error_log.txt", "a", encoding="utf-8") as fh:
            fh.write(message)
            if not message.endswith("\n"):
                fh.write("\n")
    except Exception:
        logging.getLogger(__name__).exception("No se pudo escribir en error_log.txt")


def _format_exception(exc_type, exc_value, exc_tb) -> str:
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    exc_text = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
    return f"[{timestamp}] Unhandled exception:\n{exc_text}\n"


def install_global_excepthook() -> None:
    """Configura logging y captura excepciones no manejadas en todos los hilos."""

    _ensure_logging_configured()
    logger = logging.getLogger("SansebasSmsSync")

    def _handle_exception(exc_type, exc_value, exc_tb, *, thread: Optional[threading.Thread] = None) -> None:
        thread_info = f" en hilo '{thread.name}'" if thread else ""
        logger.critical("Excepción no manejada%s", thread_info, exc_info=(exc_type, exc_value, exc_tb))
        _write_error_log(_format_exception(exc_type, exc_value, exc_tb))

    def _sys_hook(exc_type, exc_value, exc_tb):
        _handle_exception(exc_type, exc_value, exc_tb)

    def _thread_hook(args: threading.ExceptHookArgs):
        _handle_exception(args.exc_type, args.exc_value, args.exc_traceback, thread=args.thread)

    sys.excepthook = _sys_hook
    threading.excepthook = _thread_hook


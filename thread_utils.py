import logging
import threading
from typing import Any, Callable


def run_bg(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    thread_name = kwargs.pop("_thread_name", None) or getattr(fn, "__name__", "worker")
    logger = logging.getLogger(fn.__module__ or __name__)

    def _runner() -> None:
        try:
            fn(*args, **kwargs)
        except Exception:
            logger.exception("Error en tarea en segundo plano '%s'", thread_name)

    threading.Thread(target=_runner, daemon=True, name=thread_name).start()

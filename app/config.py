"""Configuración central del servicio.

Todos los valores salen de variables de entorno, pero los *defaults* no son
arbitrarios: están calibrados contra los límites reales del proveedor
(ver provider/app.py). Tenerlos en un único sitio permite ajustar el pipeline
sin tocar la lógica.
"""

import os
from dataclasses import dataclass


def _env_str(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    """Lee un entero del entorno; si el valor es basura, usa el default."""
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    """Igual que _env_int pero para decimales."""
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class Settings:
    # --- Conexión con el proveedor externo -------------------------------
    # El docker-compose arranca esta app con network_mode: "service:provider",
    # es decir, comparte el namespace de red del proveedor. Por eso el
    # proveedor se alcanza en localhost y no en el hostname del servicio.
    provider_url: str = _env_str("PROVIDER_URL", "http://localhost:3001")
    provider_api_key: str = _env_str("PROVIDER_API_KEY", "test-dev-2026")

    # El proveedor duerme entre 0,1 y 0,5 s y además tiene un semáforo de 50.
    # 5 s de lectura cubren el peor caso con holgura sin dejar conexiones
    # colgadas eternamente si el proveedor se atasca.
    connect_timeout: float = _env_float("PROVIDER_CONNECT_TIMEOUT", 2.0)
    read_timeout: float = _env_float("PROVIDER_READ_TIMEOUT", 5.0)
    max_connections: int = _env_int("PROVIDER_MAX_CONNECTIONS", 32)

    # --- Control de caudal (el corazón de la solución) -------------------
    # El proveedor rechaza con 429 cuando acumula 50 peticiones en una
    # ventana deslizante de 10 s => 5 req/s sostenidos. Nos quedamos algo por
    # debajo para no rozar el límite y comernos 429 evitables.
    rate_limit_rps: float = _env_float("RATE_LIMIT_RPS", 4.5)
    rate_limit_burst: int = _env_int("RATE_LIMIT_BURST", 5)

    # Con 4,5 req/s y ~0,3 s de latencia media apenas hay 1,5 peticiones en
    # vuelo. 8 workers sobran; el que marca el ritmo es el rate limiter, no
    # el número de workers.
    worker_count: int = _env_int("WORKER_COUNT", 8)

    # Cola acotada: si se llena, preferimos marcar la solicitud como fallida
    # antes que quedarnos sin memoria. Es backpressure explícito.
    queue_max_size: int = _env_int("QUEUE_MAX_SIZE", 10_000)

    # --- Política de reintentos ------------------------------------------
    max_attempts: int = _env_int("MAX_ATTEMPTS", 4)
    backoff_base: float = _env_float("BACKOFF_BASE", 0.5)
    backoff_max: float = _env_float("BACKOFF_MAX", 8.0)
    # Un 429 significa "estás saturando al proveedor": conviene esperar más
    # que ante un 500 puntual.
    backoff_429: float = _env_float("BACKOFF_429", 2.0)

    # --- Higiene de memoria ----------------------------------------------
    # El estado vive en memoria. Purgamos los registros ya terminados para que
    # una ejecución larga no crezca sin control.
    record_ttl_seconds: float = _env_float("RECORD_TTL_SECONDS", 300.0)
    janitor_interval: float = _env_float("JANITOR_INTERVAL", 60.0)


settings = Settings()

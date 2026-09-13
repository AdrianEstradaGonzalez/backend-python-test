"""Cliente HTTP contra el proveedor externo, con reintentos y backoff.

El proveedor está diseñado para fallar: devuelve 500 en el 10 % de las
llamadas y 429 cuando se supera su límite de caudal. Este módulo aísla toda
esa fealdad para que el resto del pipeline no tenga que saber nada de códigos
HTTP ni de reintentos.

Decisión de diseño: los reintentos están escritos a mano en vez de usar
tenacity. Son unas 40 líneas y a cambio podemos aplicar políticas distintas
según el tipo de error (un 429 no se trata como un 500) y respetar la
cabecera Retry-After, cosas que con el decorador quedan forzadas.

El limitador de caudal vive aquí dentro y no en el pipeline, y la razón es
importante: el límite es una propiedad *del proveedor*, y lo que cuenta el
proveedor son peticiones HTTP, no notificaciones. Si el freno estuviese un
nivel más arriba, los reintentos se colarían sin pedir permiso y el caudal
real sería mayor que el configurado -- que es justamente lo que provoca los
429 que intentamos evitar.
"""

import asyncio
import logging
import random
from dataclasses import dataclass
from typing import Optional

import httpx

from config import Settings
from domain.models import NotificationRecord
from infrastructure.rate_limiter import TokenBucket

logger = logging.getLogger("notification-service.provider")


@dataclass
class SendOutcome:
    """Resultado final de enviar una notificación, ya con reintentos agotados."""

    ok: bool
    provider_id: Optional[str] = None
    error: Optional[str] = None
    attempts: int = 0


class _Attempt:
    """Resultado de UN intento suelto. Uso interno del cliente."""

    __slots__ = ("ok", "retryable", "provider_id", "error", "retry_after")

    def __init__(self, ok, retryable, provider_id=None, error=None, retry_after=None):
        self.ok = ok
        self.retryable = retryable
        self.provider_id = provider_id
        self.error = error
        self.retry_after = retry_after


class ProviderClient:
    """Envoltorio sobre httpx.AsyncClient con la política de reintentos dentro."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: Optional[httpx.AsyncClient] = None
        # Una sola ficha por cada petición que sale hacia el proveedor,
        # reintentos incluidos.
        self._limiter = TokenBucket(settings.rate_limit_rps, settings.rate_limit_burst)

    async def start(self) -> None:
        """Crea el cliente HTTP una sola vez, al arrancar la app.

        Es deliberado que sea un singleton con pool de conexiones: abrir un
        AsyncClient por petición obligaría a rehacer el handshake TCP cada vez
        y dispararía los percentiles p95/p99 que mide el scorecard.
        """
        self._client = httpx.AsyncClient(
            base_url=self._settings.provider_url,
            headers={
                "X-API-Key": self._settings.provider_api_key,
                "Content-Type": "application/json",
            },
            timeout=httpx.Timeout(
                self._settings.read_timeout,
                connect=self._settings.connect_timeout,
            ),
            limits=httpx.Limits(
                max_connections=self._settings.max_connections,
                max_keepalive_connections=self._settings.max_connections,
            ),
        )

    async def aclose(self) -> None:
        """Cierra el pool al apagar la app, sin dejar sockets colgando."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _attempt(self, record: NotificationRecord) -> _Attempt:
        """Hace UNA llamada a POST /v1/notify y clasifica lo que vuelve.

        Empieza pidiendo ficha al limitador: ninguna petición sale de aquí
        sin pasar por el freno, sea un primer intento o el cuarto reintento.

        La clave del resto está en distinguir tres familias:
          - 2xx           -> éxito;
          - 429/5xx/red   -> fallo transitorio, merece la pena reintentar;
          - resto de 4xx  -> fallo nuestro (payload malo, API key inválida),
                             reintentar solo gastaría cuota del proveedor.
        """
        assert self._client is not None, "ProviderClient.start() no fue llamado"

        # Aquí es donde el servicio espera de verdad: ajusta nuestro caudal
        # de salida al que el proveedor es capaz de digerir.
        await self._limiter.acquire()

        try:
            response = await self._client.post(
                "/v1/notify",
                json=record.payload.model_dump(),
                # trace_id permite correlacionar en los logs del proveedor
                # una notificación concreta con nuestra solicitud.
                params={"trace_id": record.id},
            )
        except httpx.TimeoutException as exc:
            return _Attempt(ok=False, retryable=True, error=f"timeout: {exc!r}")
        except httpx.HTTPError as exc:
            # Conexión rechazada, DNS, socket cortado... el proveedor puede
            # estar reiniciándose: lo tratamos como transitorio.
            return _Attempt(ok=False, retryable=True, error=f"network: {exc!r}")

        if response.status_code < 300:
            body = _safe_json(response)
            return _Attempt(ok=True, retryable=False, provider_id=body.get("provider_id"))

        if response.status_code == 429:
            # El proveedor no manda Retry-After, pero lo respetamos si algún
            # día lo hace: es la señal más fiable de cuánto esperar.
            return _Attempt(
                ok=False,
                retryable=True,
                error="rate_limited",
                retry_after=_parse_retry_after(response),
            )

        if response.status_code >= 500:
            return _Attempt(
                ok=False, retryable=True, error=f"provider_5xx:{response.status_code}"
            )

        return _Attempt(
            ok=False, retryable=False, error=f"client_4xx:{response.status_code}"
        )

    def _backoff(self, attempt_number: int, outcome: _Attempt) -> float:
        """Calcula cuánto esperar antes del siguiente intento.

        Dos detalles que importan:
          - la base es mayor para un 429 que para un 500: si nos han frenado,
            insistir rápido solo empeora la situación;
          - el jitter aleatorio es imprescindible. Con 200 usuarios virtuales,
            un backoff determinista haría que todos los reintentos cayesen en
            el mismo instante y volviesen a chocar entre ellos.
        """
        if outcome.retry_after is not None:
            return outcome.retry_after

        base = (
            self._settings.backoff_429
            if outcome.error == "rate_limited"
            else self._settings.backoff_base
        )
        delay = min(base * (2 ** (attempt_number - 1)), self._settings.backoff_max)
        # Full jitter: un valor uniforme entre 0 y el retardo calculado.
        # Reparte los reintentos por toda la ventana en lugar de apilarlos.
        return random.uniform(0, delay)

    async def send(self, record: NotificationRecord) -> SendOutcome:
        """Envía la notificación reintentando hasta max_attempts veces.

        Devuelve siempre un SendOutcome; nunca propaga una excepción HTTP
        hacia arriba. Esa es la frontera que garantiza que un fallo del
        proveedor jamás se convierta en un error hacia nuestro cliente.
        """
        last_error = "unknown"
        for attempt_number in range(1, self._settings.max_attempts + 1):
            record.attempts = attempt_number
            outcome = await self._attempt(record)

            if outcome.ok:
                return SendOutcome(
                    ok=True, provider_id=outcome.provider_id, attempts=attempt_number
                )

            last_error = outcome.error or "unknown"

            if not outcome.retryable:
                logger.warning("Fallo no recuperable en %s: %s", record.id, last_error)
                return SendOutcome(ok=False, error=last_error, attempts=attempt_number)

            # Si este ya era el último intento, no dormimos para nada.
            if attempt_number < self._settings.max_attempts:
                await asyncio.sleep(self._backoff(attempt_number, outcome))

        logger.warning(
            "Reintentos agotados en %s tras %d intentos (%s)",
            record.id,
            self._settings.max_attempts,
            last_error,
        )
        return SendOutcome(
            ok=False, error=last_error, attempts=self._settings.max_attempts
        )


    def stats(self) -> dict:
        """Estado del limitador, para el endpoint /v1/stats."""
        return {
            "rate_limit_rps": self._settings.rate_limit_rps,
            "tokens_available": round(self._limiter.available, 2),
            "max_attempts": self._settings.max_attempts,
        }


def _safe_json(response: httpx.Response) -> dict:
    """Parsea el cuerpo como JSON sin reventar si el proveedor manda basura."""
    try:
        data = response.json()
        return data if isinstance(data, dict) else {}
    except ValueError:
        return {}


def _parse_retry_after(response: httpx.Response) -> Optional[float]:
    """Lee la cabecera Retry-After en segundos, si viene y es un número."""
    raw = response.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        return None

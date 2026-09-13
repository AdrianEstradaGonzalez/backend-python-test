"""El pipeline de procesamiento: cola acotada + pool de workers.

Esta es la pieza que separa "recibir" de "enviar". La API solo apunta trabajo
en una cola y responde al instante; el envío real, que es lento y falla, lo
hacen unos workers de fondo a un ritmo que el proveedor pueda digerir.

"""

import asyncio
import logging
from typing import List, Optional, Set

from config import Settings
from domain.models import RequestStatus
from infrastructure.provider_client import ProviderClient
from infrastructure.store import InMemoryStore

logger = logging.getLogger("notification-service.pipeline")


class NotificationPipeline:
    """Orquesta la cola, los workers, el limitador y el recolector de basura."""

    def __init__(
        self,
        store: InMemoryStore,
        client: ProviderClient,
        settings: Settings,
    ) -> None:
        self._store = store
        self._client = client
        self._settings = settings
        # maxsize > 0 es lo que convierte la cola en un mecanismo de
        # backpressure: cuando se llena, put_nowait falla y nosotros
        # decidimos qué hacer en vez de tragar memoria sin límite.
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=settings.queue_max_size)
        self._workers: List[asyncio.Task] = []
        self._janitor: Optional[asyncio.Task] = None
        self._rejected = 0
        # Ids que están en la cola o siendo procesados ahora mismo. Es lo que
        # hace que submit() sea idempotente: sin este conjunto, dos llamadas
        # seguidas a /process meterían el mismo id dos veces en la cola y el
        # destinatario recibiría la notificación duplicada.
        self._inflight: Set[str] = set()

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Levanta los workers y el recolector de basura.

        Se llama desde el lifespan de FastAPI, o sea dentro del event loop ya
        en marcha. Crear estas tareas antes de que el loop exista daría error.
        """
        self._workers = [
            asyncio.create_task(self._worker(i), name=f"worker-{i}")
            for i in range(self._settings.worker_count)
        ]
        self._janitor = asyncio.create_task(self._janitor_loop(), name="janitor")
        logger.info(
            "Pipeline arrancado: %d workers, %.1f req/s, cola max %d",
            self._settings.worker_count,
            self._settings.rate_limit_rps,
            self._settings.queue_max_size,
        )

    async def stop(self) -> None:
        """Apaga el pipeline de forma ordenada.

        Cancelamos las tareas y esperamos a que terminen. Sin esto, al parar
        el contenedor quedarían corrutinas a medias y httpx se quejaría de
        conexiones abiertas.
        """
        for task in [*self._workers, self._janitor]:
            if task is not None:
                task.cancel()
        # return_exceptions=True para que un CancelledError de un worker no
        # impida esperar a los demás.
        await asyncio.gather(
            *[t for t in [*self._workers, self._janitor] if t is not None],
            return_exceptions=True,
        )
        self._workers = []
        self._janitor = None
        logger.info("Pipeline detenido")

    # ------------------------------------------------------------------
    # Entrada de trabajo
    # ------------------------------------------------------------------

    def submit(self, request_id: str) -> bool:
        """Encola un id para procesarlo. Devuelve False si la cola está llena.

        Es síncrona a propósito: `put_nowait` no bloquea nunca, así que el
        handler HTTP que la llama no cede el control ni añade latencia. Usar
        `await queue.put()` sería el error clásico -- se quedaría esperando
        con la cola llena y convertiría /process en un endpoint lento.

        Es también idempotente: reenviar un id que ya está en vuelo devuelve
        True sin duplicarlo. La alternativa (fiarse de que quien llama mire
        antes el estado) deja una ventana abierta y acaba en envíos dobles.
        """
        if request_id in self._inflight:
            return True

        try:
            self._queue.put_nowait(request_id)
        except asyncio.QueueFull:
            self._rejected += 1
            return False

        self._inflight.add(request_id)
        return True

    # ------------------------------------------------------------------
    # Trabajo de fondo
    # ------------------------------------------------------------------

    async def _worker(self, index: int) -> None:
        """Bucle infinito de un worker: saca un id de la cola y lo procesa.

        Cada excepción se captura dentro del bucle. Un worker que muere por
        un error inesperado dejaría el pipeline cojo para siempre, y como
        nadie lo vigila, el fallo pasaría desapercibido.
        """
        while True:
            request_id = await self._queue.get()
            try:
                await self._process(request_id)
            except asyncio.CancelledError:
                # Apagado ordenado: propagamos para que stop() pueda esperar.
                raise
            except Exception:  # noqa: BLE001 - la red es hostil, el worker no muere
                logger.exception("Error inesperado procesando %s", request_id)
            finally:
                # Se libera aquí, y no al sacarlo de la cola, para que el id
                # siga considerándose "en vuelo" durante todo el envío.
                self._inflight.discard(request_id)
                self._queue.task_done()

    async def _process(self, request_id: str) -> None:
        """Procesa una solicitud concreta de principio a fin.

        Orden de los pasos:
          1. buscar el registro -- puede haber sido purgado mientras esperaba;
          2. descartarlo si otro worker ya lo cerró;
          3. marcar `processing`: a partir de aquí es trabajo en curso;
          4. delegar en el cliente, que se encarga del freno y los reintentos;
          5. traducir el resultado a un estado final.

        Fíjate en lo que NO hay aquí: nada de códigos HTTP, nada de backoff.
        El pipeline decide *qué* se procesa y en qué orden; el cliente decide
        *cómo* se habla con el proveedor.
        """
        record = self._store.get(request_id)
        if record is None:
            # No es un error: el TTL pudo haberlo limpiado mientras esperaba.
            logger.debug("Registro %s ya no existe, se descarta", request_id)
            return

        if record.is_terminal:
            # Alguien encoló dos veces el mismo id y otro worker ya lo cerró.
            return

        self._store.mark_processing(record)
        outcome = await self._client.send(record)

        if outcome.ok:
            self._store.mark_sent(record, outcome.provider_id)
        else:
            self._store.mark_failed(record, outcome.error or "unknown")

    async def _janitor_loop(self) -> None:
        """Purga periódicamente los registros terminales caducados.

        Tarea barata que se ejecuta cada `janitor_interval` segundos y evita
        que el almacén en memoria crezca sin fin en una ejecución larga.
        """
        while True:
            await asyncio.sleep(self._settings.janitor_interval)
            try:
                removed = self._store.prune(self._settings.record_ttl_seconds)
                if removed:
                    logger.info("Recolector: %d registros purgados", removed)
            except Exception:  # noqa: BLE001
                logger.exception("Error en el recolector de basura")

    # ------------------------------------------------------------------
    # Observabilidad
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        """Métricas del pipeline para el endpoint /v1/stats."""
        return {
            "queue_depth": self._queue.qsize(),
            "queue_max_size": self._settings.queue_max_size,
            "inflight": len(self._inflight),
            "rejected_by_backpressure": self._rejected,
            "workers": len(self._workers),
            "provider": self._client.stats(),
        }

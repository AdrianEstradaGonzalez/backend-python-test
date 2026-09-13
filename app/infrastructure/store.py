"""Almacén en memoria del estado de las solicitudes.

Por qué en memoria y no Redis/Postgres: la prueba pide un servicio
autocontenido en un único contenedor y el estado no necesita sobrevivir a un
reinicio. El almacén está detrás de una interfaz pequeña y explícita, así que
sustituirlo por Redis sería cambiar esta clase y nada más.

Nota de concurrencia: la app corre con UN solo worker de uvicorn y todo ocurre
en el mismo event loop. Ninguno de los métodos de abajo hace `await` a mitad de
una modificación, así que cada uno es atómico frente a los demás y no hace
falta un Lock. Meter uno aquí solo añadiría contención sin ganar nada.
"""

import time
import uuid
from typing import Dict, List, Optional

from domain.models import NotificationIn, NotificationRecord, RequestStatus


class InMemoryStore:
    """Diccionario id -> NotificationRecord con unas pocas operaciones útiles."""

    def __init__(self) -> None:
        self._records: Dict[str, NotificationRecord] = {}
        # Contadores acumulados. Los registros se purgan con el tiempo, pero
        # estos totales no, así que sirven para observar el servicio entero.
        self._counters: Dict[str, int] = {
            "created": 0,
            "sent": 0,
            "failed": 0,
            "pruned": 0,
        }

    def create(self, payload: NotificationIn) -> NotificationRecord:
        """Registra una solicitud nueva en estado `queued` y devuelve el registro.

        Genera el id con uuid4: no necesita coordinación ni contador global,
        que es justo lo que interesa cuando entran cientos por segundo.
        """
        now = time.monotonic()
        record = NotificationRecord(
            id=uuid.uuid4().hex,
            payload=payload,
            status=RequestStatus.QUEUED,
            created_at=now,
            updated_at=now,
        )
        self._records[record.id] = record
        self._counters["created"] += 1
        return record

    def get(self, request_id: str) -> Optional[NotificationRecord]:
        """Devuelve el registro o None si no existe (o ya fue purgado)."""
        return self._records.get(request_id)

    def mark_processing(self, record: NotificationRecord) -> None:
        """Marca que estamos llamando al proveedor *ahora mismo*.

        Importante: esto lo llama el worker justo antes del primer intento
        HTTP, no al sacar el id de la cola. Mientras la solicitud espera su
        turno sigue siendo `queued`, que es la verdad: aún no se ha tocado
        al proveedor.
        """
        record.status = RequestStatus.PROCESSING
        record.updated_at = time.monotonic()

    def mark_sent(self, record: NotificationRecord, provider_id: Optional[str]) -> None:
        """El proveedor confirmó la entrega. Estado final feliz."""
        record.status = RequestStatus.SENT
        record.provider_id = provider_id
        record.last_error = None
        record.updated_at = time.monotonic()
        self._counters["sent"] += 1

    def mark_failed(self, record: NotificationRecord, error: str) -> None:
        """Agotamos los reintentos (o el error no era recuperable).

        Guardamos el motivo para poder diagnosticar después. Ojo: esto NO se
        traduce en un error HTTP hacia el cliente; el cliente lo ve como un
        `status: "failed"` en un 200.
        """
        record.status = RequestStatus.FAILED
        record.last_error = error
        record.updated_at = time.monotonic()
        self._counters["failed"] += 1

    def requeue(self, record: NotificationRecord) -> None:
        """Devuelve un registro a `queued` para reintentarlo desde cero.

        Lo usa POST /process cuando se reintenta una solicitud que había
        quedado en `failed`.
        """
        record.status = RequestStatus.QUEUED
        record.attempts = 0
        record.last_error = None
        record.updated_at = time.monotonic()

    def prune(self, ttl_seconds: float) -> int:
        """Borra los registros terminales más viejos que `ttl_seconds`.

        Sin esto, un servicio en marcha varios días acabaría acumulando
        millones de registros muertos. Solo tocamos los terminales: un
        `queued` puede llevar mucho rato esperando y sigue siendo válido.

        Devuelve cuántos borró.
        """
        cutoff = time.monotonic() - ttl_seconds
        expired: List[str] = [
            rid
            for rid, record in self._records.items()
            if record.is_terminal and record.updated_at < cutoff
        ]
        for rid in expired:
            del self._records[rid]
        self._counters["pruned"] += len(expired)
        return len(expired)

    def stats(self) -> Dict[str, int]:
        """Foto del estado actual: contadores acumulados + desglose en vivo.

        Alimenta el endpoint /v1/stats, que no forma parte del contrato pero
        hace mucho más fácil demostrar que el pipeline hace lo que dice.
        """
        by_status: Dict[str, int] = {
            f"current_{status.value}": 0 for status in RequestStatus
        }
        for record in self._records.values():
            by_status[f"current_{record.status.value}"] += 1
        # Prefijamos el desglose en vivo con "current_" para que no pise a los
        # contadores acumulados: "sent" son todos los enviados desde el
        # arranque, "current_sent" solo los que siguen en memoria.
        return {**self._counters, "in_memory": len(self._records), **by_status}

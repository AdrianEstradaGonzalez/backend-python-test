"""Capa HTTP: los tres endpoints del contrato, más dos de apoyo.

Regla que gobierna todo este fichero: **los handlers no hacen trabajo lento**.
Leen o escriben en memoria, encolan, y responden. Cualquier cosa que implique
salir a la red vive en el pipeline. Eso es lo que mantiene la latencia de
/process por debajo del umbral de 500 ms que marca el scorecard como ASYNC.

Segunda regla: **un fallo del proveedor nunca se convierte en un error HTTP
hacia nuestro cliente**. Los problemas del proveedor se cuentan en el campo
`status` de un 200, no en el código de respuesta.
"""

import logging

from fastapi import APIRouter, HTTPException, Request, Response, status

from domain.models import AcceptedOut, CreatedOut, NotificationIn, RequestStatus, StatusOut
from services.pipeline import NotificationPipeline
from infrastructure.store import InMemoryStore

logger = logging.getLogger("notification-service.api")

router = APIRouter()


def _store(request: Request) -> InMemoryStore:
    """Saca el almacén del estado de la app.

    Se inyecta así (y no como singleton de módulo) para que los tests puedan
    montar la app con un almacén limpio.
    """
    return request.app.state.store


def _pipeline(request: Request) -> NotificationPipeline:
    """Igual que _store, pero para el pipeline."""
    return request.app.state.pipeline


@router.post(
    "/v1/requests",
    response_model=CreatedOut,
    status_code=status.HTTP_201_CREATED,
    tags=["Requests"],
    summary="Registrar una solicitud de notificación",
)
async def create_request(payload: NotificationIn, request: Request) -> CreatedOut:
    """Da de alta la solicitud en estado `queued` y devuelve su id.

    Solo registra: no llama al proveedor. Es una escritura en un diccionario,
    así que responde en microsegundos por mucha carga que haya. Pydantic ya
    ha validado el cuerpo antes de llegar aquí.
    """
    record = _store(request).create(payload)
    return CreatedOut(id=record.id)


@router.post(
    "/v1/requests/{request_id}/process",
    response_model=AcceptedOut,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["Requests"],
    summary="Lanzar el procesamiento de una solicitud",
)
async def process_request(request_id: str, request: Request) -> AcceptedOut:
    """Encola la solicitud para que la envíen los workers y responde 202.

    El 202 ("Accepted") es la respuesta honesta: hemos aceptado el encargo,
    todavía no lo hemos cumplido. Devolver 200 con el envío ya hecho exigiría
    esperar al proveedor (0,1-0,5 s por intento, más reintentos) y convertiría
    este endpoint en síncrono.

    Casos que contempla:
      - id desconocido -> 404, que es lo correcto: no existe el recurso;
      - ya enviado o en curso -> 202 sin reencolar (idempotente);
      - fallido previamente -> se reinicia y se vuelve a encolar;
      - cola llena -> se marca `failed` y se avisa con accepted=False,
        pero seguimos devolviendo 202: la saturación es asunto nuestro,
        no un error del cliente.
    """
    store = _store(request)
    record = store.get(request_id)

    if record is None:
        raise HTTPException(status_code=404, detail="request not found")

    # Idempotencia, primera mitad: si ya está en curso o entregado, no hay
    # nada que hacer. La otra mitad (repetir /process sobre algo que sigue en
    # `queued`) la cubre el propio pipeline, que lleva la cuenta de los ids
    # en vuelo. Con reintentos de cliente ambos casos pasan constantemente.
    if record.status in (RequestStatus.PROCESSING, RequestStatus.SENT):
        return AcceptedOut(id=record.id, status=record.status, accepted=True)

    if record.status == RequestStatus.FAILED:
        store.requeue(record)

    if not _pipeline(request).submit(record.id):
        # Backpressure: la cola está llena. Lo decimos claro en el estado en
        # lugar de aceptar trabajo que no vamos a poder hacer.
        store.mark_failed(record, "queue_full")
        logger.warning("Cola llena, solicitud %s rechazada", record.id)
        return AcceptedOut(id=record.id, status=record.status, accepted=False)

    return AcceptedOut(id=record.id, status=record.status, accepted=True)


@router.get(
    "/v1/requests/{request_id}",
    response_model=StatusOut,
    tags=["Requests"],
    summary="Consultar el estado de una solicitud",
)
async def get_request(request_id: str, request: Request) -> StatusOut:
    """Devuelve el estado actual de la solicitud.

    Lectura pura de memoria. El estado puede ser `queued` mucho rato y eso no
    es un fallo: significa que la solicitud está esperando su turno porque el
    proveedor admite mucho menos caudal del que entra.
    """
    record = _store(request).get(request_id)
    if record is None:
        raise HTTPException(status_code=404, detail="request not found")
    return StatusOut(id=record.id, status=record.status)


@router.get("/health", tags=["Ops"], summary="Sonda de salud")
async def health() -> Response:
    """Responde 200 en cuanto la app está viva. La usa el healthcheck de Docker."""
    return Response(status_code=200, content='{"status":"ok"}', media_type="application/json")


@router.get("/v1/stats", tags=["Ops"], summary="Métricas internas del pipeline")
async def stats(request: Request) -> dict:
    """Expone el estado interno: profundidad de cola, contadores, fichas libres.

    No forma parte del contrato de la prueba, pero permite ver en vivo que el
    pipeline se comporta como se dice: la cola crece bajo carga, el caudal de
    salida se mantiene plano y los reintentos hacen su trabajo.
    """
    return {
        "store": _store(request).stats(),
        "pipeline": _pipeline(request).stats(),
    }

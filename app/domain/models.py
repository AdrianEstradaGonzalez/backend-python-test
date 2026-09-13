"""Modelos de datos: lo que entra y sale por la API, y lo que guardamos dentro.

Separamos a propósito el modelo *externo* (lo que exige el contrato de la
prueba) del modelo *interno* (lo que el pipeline necesita saber para trabajar:
intentos, errores, marcas de tiempo...). Así podemos enriquecer el estado
interno sin romper el contrato público.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, Field

NotificationType = Literal["email", "sms", "push"]


class RequestStatus(str, Enum):
    """Los cuatro únicos estados que admite el contrato de la prueba.

    Hereda de str para que FastAPI lo serialice como texto plano
    ("queued") en lugar de como objeto.

    Ciclo de vida:
        queued -> processing -> sent
                             -> failed
    """

    QUEUED = "queued"
    PROCESSING = "processing"
    SENT = "sent"
    FAILED = "failed"


class NotificationIn(BaseModel):
    """Cuerpo de POST /v1/requests.

    Pydantic valida esto antes de que llegue a nuestro código: si el cliente
    manda un `type` inválido, FastAPI responde 422 solo, sin que el pipeline
    llegue a enterarse.
    """

    to: str = Field(..., min_length=1, max_length=320)
    message: str = Field(..., min_length=1, max_length=2000)
    type: NotificationType


class CreatedOut(BaseModel):
    """Respuesta de POST /v1/requests -> {"id": "..."}."""

    id: str


class AcceptedOut(BaseModel):
    """Respuesta de POST /v1/requests/{id}/process.

    Devolvemos también el estado para que el cliente sepa si la solicitud
    entró en la cola o fue rechazada por backpressure, sin tener que hacer
    un GET adicional.
    """

    id: str
    status: RequestStatus
    accepted: bool


class StatusOut(BaseModel):
    """Respuesta de GET /v1/requests/{id} -> {"id": "...", "status": "..."}."""

    id: str
    status: RequestStatus


@dataclass
class NotificationRecord:
    """Estado interno de una solicitud. Nunca sale tal cual por la API.

    Usamos dataclass y no Pydantic porque este objeto se lee y escribe miles
    de veces por segundo y no necesita validación: ya viene validado.
    """

    id: str
    payload: NotificationIn
    status: RequestStatus = RequestStatus.QUEUED
    attempts: int = 0
    provider_id: Optional[str] = None
    last_error: Optional[str] = None
    # Reloj monótono: inmune a cambios de hora del sistema, que es lo que
    # queremos para medir antigüedad y decidir purgas.
    created_at: float = field(default=0.0)
    updated_at: float = field(default=0.0)

    @property
    def is_terminal(self) -> bool:
        """True si la solicitud ya no va a cambiar de estado por sí sola.

        El recolector de basura solo purga registros terminales.
        """
        return self.status in (RequestStatus.SENT, RequestStatus.FAILED)

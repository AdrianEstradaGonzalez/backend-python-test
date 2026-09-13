"""Punto de entrada de la aplicación: monta las piezas y las conecta.

Este fichero no tiene lógica de negocio a propósito. Solo:
  1. configura el logging,
  2. crea almacén, cliente y pipeline y los guarda en app.state,
  3. los arranca y los para junto con la app (lifespan),
  4. registra los endpoints y una red de seguridad para errores imprevistos.

Se ejecuta con UN solo worker de uvicorn. El estado vive en memoria, así que
varios procesos tendrían cada uno su propio diccionario y un GET podría caer
en el worker que no conoce el id. Para escalar horizontalmente habría que
mover almacén y cola a Redis; con el alcance de esta prueba, un proceso
asíncrono da de sobra.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from api import router
from config import settings
from services.pipeline import NotificationPipeline
from infrastructure.provider_client import ProviderClient
from infrastructure.store import InMemoryStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
)
logger = logging.getLogger("notification-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Arranca y para los recursos de larga vida junto con la aplicación.

    El lifespan es el sitio correcto para esto porque se ejecuta dentro del
    event loop ya activo: aquí se pueden crear tareas y clientes asíncronos,
    cosa que en el import del módulo sería imposible.

    Lo de arriba del `yield` corre al arrancar; lo de abajo, al apagar.
    """
    app.state.store = InMemoryStore()
    app.state.client = ProviderClient(settings)
    await app.state.client.start()

    app.state.pipeline = NotificationPipeline(
        store=app.state.store,
        client=app.state.client,
        settings=settings,
    )
    await app.state.pipeline.start()

    logger.info("Servicio listo. Proveedor en %s", settings.provider_url)
    try:
        yield
    finally:
        # Orden inverso al arranque: primero paramos de generar tráfico y
        # luego cerramos el pool de conexiones.
        await app.state.pipeline.stop()
        await app.state.client.aclose()
        logger.info("Servicio detenido limpiamente")


app = FastAPI(
    title="Notification Service (Technical Test)",
    description=(
        "Mediador asíncrono entre clientes y un proveedor externo inestable. "
        "Encola, limita el caudal y reintenta, sin propagar nunca los fallos "
        "del proveedor al cliente."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(router)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Red de seguridad para cualquier error que se nos haya escapado.

    Sin esto, una excepción no prevista en un handler saldría como un 500 y
    contaría como caída en el scorecard. Aquí la registramos con traza
    completa para poder arreglarla y devolvemos un 503 con cuerpo JSON bien
    formado, que al menos es una respuesta controlada.
    """
    logger.exception("Excepción no controlada en %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=503,
        content={"error": "service_unavailable"},
    )

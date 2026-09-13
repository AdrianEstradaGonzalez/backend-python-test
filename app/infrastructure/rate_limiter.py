"""Limitador de caudal (token bucket) para no saturar al proveedor.

Este es el componente que hace que la solución funcione. El proveedor acepta
50 peticiones por ventana deslizante de 10 s (5 req/s) y el test de carga le
manda unas 130 solicitudes por segundo. Sin un freno en nuestro lado, todo lo
que pase de 5 req/s se convierte en 429 y en reintentos que vuelven a chocar.

El *token bucket* resuelve las dos cosas a la vez:
  - ritmo sostenido: se reponen `rate` fichas por segundo;
  - ráfagas cortas: se pueden acumular hasta `capacity` fichas sin usar, así
    que un pico breve tras un rato de calma no se penaliza.
"""

import asyncio
import time


class TokenBucket:
    """Cubo de fichas asíncrono. Un worker pide una ficha y espera si no hay."""

    def __init__(self, rate: float, capacity: int) -> None:
        """
        Args:
            rate: fichas repuestas por segundo (= peticiones/segundo permitidas).
            capacity: fichas máximas acumulables (tamaño de la ráfaga).
        """
        self._rate = rate
        self._capacity = float(capacity)
        # Arrancamos lleno: al empezar no hay deuda con el proveedor.
        self._tokens = float(capacity)
        self._updated_at = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        """Añade las fichas generadas desde la última consulta.

        No hace falta un temporizador en segundo plano: calculamos las fichas
        a posteriori con el tiempo transcurrido, que es más barato y más exacto.
        """
        now = time.monotonic()
        elapsed = now - self._updated_at
        self._updated_at = now
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)

    async def acquire(self) -> None:
        """Consume una ficha, esperando lo que haga falta.

        La espera ocurre *dentro* del lock a propósito. Puede parecer un
        cuello de botella, pero es justo lo que se busca: serializa a los
        workers y reparte las salidas de forma regular. Si cada uno esperase
        por su cuenta, los ocho despertarían a la vez y le meterían al
        proveedor una ráfaga sincronizada -> 429 asegurado.
        """
        async with self._lock:
            self._refill()
            if self._tokens < 1.0:
                # Tiempo exacto que falta para que se genere la ficha que falta.
                wait = (1.0 - self._tokens) / self._rate
                await asyncio.sleep(wait)
                self._refill()
            self._tokens -= 1.0

    @property
    def available(self) -> float:
        """Fichas disponibles ahora mismo. Solo para observabilidad."""
        self._refill()
        return self._tokens

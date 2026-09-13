FROM python:3.12-slim

WORKDIR /code

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

# --workers 1 es intencional, no un descuido: el estado de las solicitudes y la
# cola viven en memoria del proceso. Con varios workers cada uno tendría su
# propio diccionario y un GET podría caer en el que no conoce el id.
#
# --no-access-log evita escribir una línea por petición. Con ~400 req/s eso es
# E/S constante que ensucia los percentiles p95/p99 que mide el scorecard.
CMD ["uvicorn", "main:app", \
     "--host", "0.0.0.0", \
     "--port", "5000", \
     "--workers", "1", \
     "--no-access-log", \
     "--log-level", "info"]

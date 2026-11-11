FROM python:3.15-slim
LABEL maintainer="puijken"
LABEL description="Sungrow inverter Modbus reader: writes to TimescaleDB, publishes to MQTT, pushes to PVOutput/Mindergas"
LABEL org.opencontainers.image.source="https://github.com/puijken/docker-energy-monitor"

WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd --create-home --uid 1000 monitor
COPY --chown=monitor:monitor app.py /app/
# The Monta API client (ENABLE_MONTA) lives in its own repo, github.com/puijken/stroohm-monta-api.
# Pinned to a commit so a rebuild can't pick up an untested change; bump the ref to update it.
ARG STROOHM_MONTA_API_REF=595d8c13bb3715841374e9077254c8fdb10fec10
ADD --chown=monitor:monitor --chmod=644 \
    https://raw.githubusercontent.com/puijken/stroohm-monta-api/${STROOHM_MONTA_API_REF}/stroohm_monta_api.py /app/
USER monitor

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

CMD ["python3", "/app/app.py"]

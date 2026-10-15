FROM python:3.14-slim
LABEL maintainer="puijken"
LABEL description="Sungrow inverter Modbus reader: writes to TimescaleDB, publishes to MQTT, pushes to PVOutput/Mindergas"
LABEL org.opencontainers.image.source="https://github.com/puijken/docker-energy-monitor"

WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd --create-home --uid 1000 monitor
COPY --chown=monitor:monitor app.py /app/
USER monitor

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

CMD ["python3", "/app/app.py"]

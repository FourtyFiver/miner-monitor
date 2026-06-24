FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir paho-mqtt pyyaml

COPY monitor.py /app/monitor.py

RUN mkdir -p /config

CMD ["python3", "/app/monitor.py"]

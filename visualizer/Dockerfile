FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
 && rm -rf /var/lib/apt/lists/*

COPY visualizer/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
COPY visualizer/map_server.py /app/map_server.py

ENV PYTHONUNBUFFERED=1
EXPOSE 8080

ENTRYPOINT ["python3", "/app/map_server.py"]
CMD ["--broker", "kafka"]

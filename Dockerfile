FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && groupadd --system replicafs \
    && useradd --system --gid replicafs --home-dir /app replicafs \
    && mkdir /data \
    && chown replicafs:replicafs /data
COPY --chown=replicafs:replicafs replicafs ./replicafs
USER replicafs
# Compose chooses either the metadata API or a storage API.
CMD ["uvicorn", "replicafs.metadata.app:app", "--host", "0.0.0.0", "--port", "8000"]

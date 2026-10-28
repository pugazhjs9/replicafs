FROM python:3.13-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY replicafs ./replicafs

# The service to run is chosen by `command:` in docker-compose.yml

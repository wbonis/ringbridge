# ringbridge - Ring-Portierung von roger-/blinkbridge
#
# Bewusst python:3.12-slim statt python:alpine (wie im Original):
# ring_doorbell installiert dort ohne Kompilieren sauber durch.
FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir rich ring_doorbell aiohttp paho-mqtt

COPY ringbridge /app/ringbridge

WORKDIR /app/

ENTRYPOINT ["python", "-m", "ringbridge.main"]

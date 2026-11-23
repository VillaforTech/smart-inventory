FROM python:3.12-alpine3.24
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN apk upgrade --no-cache \
 && python -m pip install --no-cache-dir -r requirements.txt \
 && addgroup -S -g 10001 inventory \
 && adduser -S -D -H -u 10001 -G inventory inventory \
 && mkdir -p /data \
 && chown 10001:10001 /data
COPY inventory ./inventory
COPY scripts/demo-order.py ./scripts/demo-order.py
USER inventory
EXPOSE 8000
CMD ["python", "-m", "uvicorn", "inventory.api:app", "--host", "0.0.0.0", "--port", "8000"]

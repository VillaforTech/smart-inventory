FROM python:3.12-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt \
 && groupadd --gid 10001 inventory \
 && useradd --uid 10001 --gid 10001 --no-create-home inventory \
 && install -d -o inventory -g inventory /data
COPY inventory ./inventory
COPY scripts/demo-order.py ./scripts/demo-order.py
USER inventory
EXPOSE 8000
CMD ["python", "-m", "uvicorn", "inventory.api:app", "--host", "0.0.0.0", "--port", "8000"]

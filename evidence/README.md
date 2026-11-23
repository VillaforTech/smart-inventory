# Evidencia

Los logs y resultados incluidos corresponden solo a las ejecuciones aquí registradas. La prueba broker dependiente de RabbitMQ puede aparecer omitida con motivo explícito. No se incluyen tokens, archivos de base, ni datos de clientes. El benchmark usa un ASGI local con SQLite temporal y no demuestra un despliegue o un SLA de producción.

## Comandos reproducibles

- `python -m unittest discover -s tests -v`
- `python scripts/benchmark.py`
- `python scripts/export-openapi.py > docs/openapi.json`
- Para pruebas de broker y operación integrada, usa [el runbook](../docs/OPERACION.md).

Las evidencias de GitHub Actions, el dashboard conectado a un broker real, los escaneos y la intervención de revisores quedan separados hasta tener salidas verificables del entorno de staging del equipo. No se certifican despliegues, PR, tags ni pruebas que no se ejecutaron.

# Runbook: operación local y recuperación

Usar solo fixtures y un broker de la demo. No configurar este Compose contra infraestructura real. Se requiere Docker con Compose v2.

```sh
docker compose config --quiet
docker compose up --build --wait --wait-timeout 150
docker compose ps
curl --fail http://127.0.0.1:8000/live
docker compose exec -T rabbitmq rabbitmqctl -q list_queues name type messages_ready messages_unacknowledged
```

Para la demo autenticada, generar primero el archivo local `.env` con el script indicado en README. Los tokens no se incluyen en la imagen, el repositorio ni el contenedor worker. El seed sintético es voluntario.

El puerto HTTP solo se publica en loopback, por defecto 8000; el broker no expone puertos al host. La clave de RabbitMQ por defecto es un valor de demostración. Si se sustituyen las variables de RabbitMQ para una demo, usar credenciales de demo propias sin separadores reservados de URL, no escribirlas en el repo y no reutilizar el volumen del broker con credenciales previas. El volumen SQLite es compartido por API y worker; el del broker es independiente. `docker compose down` los conserva. No borrar volúmenes con evidencia o datos que se necesiten.

## Publicación y consumo

- La API guarda el cambio de dominio y el outbox en la misma transacción; no espera a RabbitMQ. El publicador singleton toma filas pendientes, publica mensajes persistentes, exige enrutamiento con `mandatory` y confirmación del broker, y solo después marca el outbox.
- Exchange tópico durable `inventory.events`: `inventory.reserved` → `InventoryReserved`; `inventory.low-stock` → `LowStockDetected`; `inventory.replenishment-approved` → `ReplenishmentApproved`. Los tres se enlazan con la cola de dashboard. Mensaje JSON v1 con `eventId`, `eventType`, `schemaVersion`, `occurredAt`, `correlationId`, `producer` y `payload`.
- La proyección materializa el evento y el identificador del inbox en una transacción, con ACK posterior. Si se pierde ACK o confirmación hay posibilidad de duplicado real, no doble efecto. La clave es `eventId`, no el identificador de correlación; un mismo identificador con distinto cuerpo es inválido.
- Error de almacenamiento: máximo dos reintentos de aplicación mediante colas durables con TTL 1 s y 5 s. Se confirma la copia a la cola de retry antes de ACK al original; si falla, se conserva el original con NACK/requeue y se restablece conexión con pausa. Tras agotar intentos, o ante mensaje inválido, NACK sin requeue envía a la DLQ. Las colas de origen son quorum con dead-lettering al menos una vez y overflow que rechaza nuevas publicaciones. El broker también limita las redeliveries repetidas en la cola principal.
- La vista registra `last_sync`, último error observado (histórico), último retardo de procesamiento o edad del outbox más antiguo pendiente, eventos recientes y contadores. El heartbeat del worker permite distinguir una vista sin eventos de un worker desconectado. Usar conteos de cola del broker como fuente complementaria: un disco SQLite inaccesible tampoco puede almacenar su propio contador. Un solo broker es duradero frente a reinicios normales con volumen conservado, no tiene alta disponibilidad ni tolera perder su volumen.

## Escenarios de demo y respuesta

1. Verificar primero estado del worker, métricas y colas. Para probar retraso, parar solo `worker` en la pila de demo con `docker compose stop worker`, realizar una reserva **sintética** y comprobar que se acepta y queda pendiente. `docker compose start worker` debe drenar el outbox y actualizar la proyección sin repetir la reserva.
2. Si falla RabbitMQ, restaurar servicio y revisar `docker compose logs worker rabbitmq`. El outbox pendiente se reintenta automáticamente por reconexión (pausa de 1 a 10 segundos). No borrar mensajes, no crear otra reserva ni inventar evidencia.
3. Si hay mensajes en `inventory.dashboard.dead`, preservar el original, cotejar ID/tipo/versión y la causa en el entorno autorizado. Corregir antes de reintentar. Los mensajes de esquema inválido **no** pueden redirigirse mediante la herramienta de recuperación. No editar el original ni publicar un evento nuevo para esconder el error.
4. Para el mensaje válido exacto en la cabeza de la DLQ, con causa reparada y operador autorizado:

   ```sh
   docker compose exec -T worker python -m inventory.worker redrive \
     --event-id ID_VERIFICADO --confirm-cause-fixed
   ```

   Se conserva `eventId` y se limpia el contador de retry; se publica solo a la cola de dashboard, no se notifica nuevamente a futuros consumidores del exchange inicial. Si la cabeza no coincide o no se confirma la publicación, se deja el original en la DLQ. No es una purga ni un replay masivo. Después revisar proyección y DLQ; si el mensaje reaparece, detener el redrive e investigar. No subvertir el orden de mensajes para forzar la herramienta.
5. Para sustituir política/argumentos de cola, planificar migración en un broker aparte y comprobar compatibilidad. No eliminar una cola con backlog para recuperar el inicio.

## Evidencia y límites

```sh
python -m unittest discover -s tests -p test_worker.py -v
```

Las pruebas unitarias simulan fallos de confirmación, ACK, SQLite y recuperación; no afirman que RabbitMQ fue ejecutado. El workflow incluye Bandit (SAST: severidad alta) y pip-audit (SCA de dependencias Python). No son pruebas DAST sobre un ambiente externo. También inicia la pila local del runner y prueba topología, retry, DLQ y redrive en su broker descartable. La presencia del YAML no significa que haya corrido, ni que exista staging o despliegue. Citar el resultado y revisión que efectivamente se prueben.

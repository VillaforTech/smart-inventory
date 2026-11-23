# Smart Inventory | Arquitectura y decisiones

**Diseño de referencia · Smart Inventory**

Contratos y decisiones del componente. La evidencia de ejecución se mantiene por revisión y entorno, separada de este diseño.

## Alcance funcional

| Historia | Contrato REST | Permiso | Evidencia esperada |
|---|---|---|---|
| Consultar inventario por SKU, ubicación y lote | `GET /api/v1/inventory` | `inventory:read` | Filtros `sku`, `location`, `lot`; paginación y orden estable; cantidades física, reservada y disponible diferenciadas. |
| Reservar mediante FEFO | `POST /api/v1/reservations` | `inventory:reserve` | El lote elegible con vencimiento más próximo se usa primero. La reserva completa y el evento se confirman juntos. Un reintento con la misma `Idempotency-Key` no crea otra reserva. |
| Revisar y aprobar reposición | `GET /api/v1/replenishment/suggestions`; `POST /api/v1/replenishment/suggestions/{id}/approve` | `replenishment:read`; `replenishment:approve` | Filtros `state`, `sku`, `supplier`; sugerencias por debajo del umbral y aprobación con validación de estado y permiso. |
| Monitorear la operación | `GET /api/v1/dashboard` | `dashboard:read` | Vista de inventario y órdenes/reservas, atraso de sincronización, errores y última sincronización. Una proyección desactualizada no debe parecer actual. |

Las consultas de inventario y sugerencias usan `page` y `page_size` y devuelven `items` y `total`. Los esquemas y códigos exactos deben coincidir con el OpenAPI de la ejecución. Un filtro no debe eludir la autorización; si se habilita alcance por ubicación, debe validarse en servidor también al reservar y aprobar. Los tokens o claves de demostración no constituyen identidad federada ni seguridad de producción.

## Componentes y recorrido

```text
Cliente / panel
  └─ API REST: validación, permisos y límites
       ├─ consultas → SQLite de escritura (estado operativo)
       ├─ comandos → transacción SQLite (negocio + outbox)
       └─ dashboard → proyección de lectura + señales de salud
                           ↑
Outbox → publicador → RabbitMQ → consumidor de proyección
```

- **Estado operativo:** cantidades de inventario por lote y ubicación, reservas y asignaciones, sugerencias de reposición y sus transiciones. Disponible = físico − reservado; una reserva no descuenta físicamente mercadería.
- **Transacción FEFO:** dentro de la misma unidad de escritura se comprueba disponibilidad y elegibilidad, se recorren lotes por vencimiento con desempate por identificador de lote; se excluyen vencidos según la fecha UTC (válidos hasta su día de vencimiento) y se escriben asignaciones. Si el total no alcanza, no se conserva una reserva parcial ni un evento de éxito. Solicitudes concurrentes deben preservar `0 ≤ reservado ≤ físico`.
- **Idempotencia:** almacenar identidad de la operación y resultado junto con el cambio de negocio. Misma clave y mismo contenido devuelve el resultado original; reutilizarla para un contenido distinto produce conflicto. No es equivalente a desduplicar mensajes de RabbitMQ.
- **Outbox:** la misma transacción de negocio registra el evento. Se publica con confirmación del broker antes de avanzar el estado de envío; si se interrumpe entre ambos pasos, es posible publicar otra vez. No prometer entrega exactamente una vez.
- **Consumidor:** escritura de proyección y registro del identificador procesado deben ser atómicos; confirmar al broker después de persistir. Un fallo recuperable requiere política acotada para no generar un bucle; un mensaje inválido no debe convertirse en actualización exitosa. La vista de lectura es eventualmente consistente y no autoriza reservas.
- **Reposición:** el motor emite una sugerencia al alcanzar o cruzar hacia abajo el punto de reposición disponible. Consultar no cambia estado; aprobar es una transición `PENDING → APPROVED` autorizada, no una compra, envío ni pago real. Un cambio inválido no debe generar un segundo efecto.
- **Panel:** distinguir un sistema sin eventos de un consumidor caído. Última sincronización debe indicar la última actualización verificable, no simplemente el instante de recarga. Mostrar el origen/unidad de lag y distinguir eventos pendientes de antigüedad temporal; no presentar métricas de una sola capa como salud de todo el pipeline. El atraso de outbox si hay pendientes y el atraso del último evento aplicado en otro caso son señales distintas; ninguna, por sí sola, mide la cola del broker.

## Contrato de eventos

Sobre común:

| Campo | Responsabilidad |
|---|---|
| `eventId` | Identificador estable del evento; se conserva al reintentar la entrega. |
| `eventType` | Tipo de evento de dominio que decide el contrato de `payload`. |
| `schemaVersion` | Versión del contrato; cambios incompatibles requieren tratamiento explícito. |
| `occurredAt` | Instante de ocurrencia en UTC, no hora de redelivery. |
| `correlationId` | Une comando, publicación y consumo en la evidencia. |
| `producer` | Identifica el componente emisor. |
| `payload` | Datos mínimos de dominio, validados por tipo y versión. Sin credenciales o datos personales. |

Los tipos de la implementación local son `InventoryReserved`, `LowStockDetected` y `ReplenishmentApproved`, todos con versión 1 y productor `smart-inventory`. Validar campos y versión en el límite del consumidor. No deducir orden global de `occurredAt`; la proyección debe soportar las garantías reales de orden de su flujo y desduplicación. En operaciones que actualicen el mismo agregado, verificar cómo se evita aplicar estados viejos sobre nuevos antes de confiar en un replay completo.

## Decisiones y límites

| Decisión | Beneficio | Costo / límite |
|---|---|---|
| SQLite transaccional para la demo | Un arranque reproducible y una frontera atómica fácil de comprobar. | Un escritor efectivo; no sustenta escalado multiinstancia ni reemplaza una base de producción. |
| REST para lectura/comando y RabbitMQ para proyección | Respuesta operativa inmediata y procesamiento desacoplado. | El panel puede quedar rezagado; se necesitan observabilidad, recuperación y desduplicación. |
| Ventana fija frente a token bucket | Ventana fija es fácil de explicar y reproducir con un contador por identidad/ruta. | Puede permitir ráfagas al cruzar límites de ventana. Token bucket amortigua ráfagas cortas mediante capacidad y tasa de reposición, pero exige reloj/estado y coordinación entre réplicas. Si el contador es en memoria, el límite aplica por proceso y se reinicia con este. No presentar el algoritmo comparado como implementado sin verificar. |
| Permisos por operación | Separa lectura, reserva y aprobación de reposición. | No sustituye un proveedor de identidad, rotación, gestión de secretos ni auditoría productiva. |
| Evidencia sintética y pruebas de fallos controladas | Demostración segura y repetible. | No garantiza rendimiento real ni entrega exactamente una vez. |

## Rendimiento y observabilidad

Definir objetivos p95 **por separado** para consultas y comandos antes de medir y rotularlos como metas elegidas por el equipo. Reportar para cada grupo: ruta, muestra, concurrencia, entorno, latencia extremo a extremo, p95 y errores. Medir también el atraso comando→proyección como métrica aparte. Una ejecución funcional o unas pocas solicitudes no constituyen validación de carga. Usar el mismo `correlationId` en registro, evento y procesamiento; no llamar “trazas distribuidas” a simples logs correlacionados si no existe un colector de trazas.

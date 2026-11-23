# Smart Inventory | Guion de demo (11 min 30 s)

**Entorno local reproducible · Datos sintéticos**

Este es un guion, no un registro de ejecución. Ejecutar con la misma revisión que se está evaluando. Preparar, con las instrucciones del README, los procesos de API, broker, publicador y consumidor necesarios. Tener OpenAPI, navegador y consola visibles. No usar datos, tokens o pedidos reales; no mostrar claves aunque sean de desarrollo. Identificar claramente cualquier caída inducida como simulada.

## Datos y roles

- Con seed fresco, usar `MILK-1L` en `UIO-01`: `LOT-A` tiene 4 unidades vigentes y `LOT-B` tiene 8; `EXPIRED` no es elegible. Una reserva de 6 asigna 4 + 2 y cruza el punto de reposición (disponible vigente inicial 12, restante 6). Así aparece una sugerencia `PENDING` del proveedor de demo `DEMO-DAIRY`.
- Usar identificadores de demostración con permisos distintos: consulta; reserva; lectura de reposición; aprobación; panel. Si la misma identidad combina permisos, preparar otra que carezca de aprobación para el control negativo.
- Preparar un `Idempotency-Key` nuevo por ensayo y anotar el `correlationId` de la reserva que se siga de punta a punta.
- Confirmar rutas y parámetros contra OpenAPI. Mantener el estado inicial y las cantidades anotados para comparar antes y después.

## Recorrido cronometrado

| Tiempo | Mostrar | Qué debe observarse |
|---|---|---|
| 0:00–0:50 | Objetivo, contrato REST y diagrama. | Diferencia entre fuente de verdad operativa y proyección del panel. |
| 0:50–2:15 | `GET /api/v1/inventory` con filtros SKU, ubicación y lote. Mostrar dos páginas cuando los datos lo permitan. | Solo coincidencias; físico, reservado y disponible son cantidades distintas. |
| 2:15–4:40 | `POST /api/v1/reservations` de dos lotes, revisar asignación; repetir misma clave y cuerpo; repetir con clave igual y cuerpo cambiado; comprobar insuficiencia con una clave distinta. | FEFO visible; no se duplica la reserva ni la reducción de disponible; los conflictos no mutan el inventario. |
| 4:40–6:05 | `GET /api/v1/replenishment/suggestions` con los filtros aplicables; intentar aprobación con lector y después con permiso de aprobación. | El intento no autorizado no cambia estado; el autorizado cambia la sugerencia, no genera compra real. |
| 6:05–8:50 | Seguir la reserva ya creada por el outbox, broker y consumidor. Si el ensayo verificado lo permite, pausar el worker local, emitir una reserva sintética, observar outbox pendiente y reanudar. Mostrar replay/duplicado solo con un caso controlado. | Correlación común, estado atrasado real y puesta al día, ningún doble efecto en la vista al repetir el evento. No simular evidencia si la falla no se reproduce. |
| 8:50–10:15 | `GET /api/v1/dashboard` y panel: inventario, órdenes/reservas, lag, error y última sincronización. | El panel admite inconsistencia temporal y explica qué capa está degradada. Una señal de lag no significa que el comando haya fallado. |
| 10:15–11:30 | Cerrar con resultados comprobados, dos decisiones y límites. | Mostrar versión, pruebas ejecutadas y estado real de CI/staging; comparar ventana fija y token bucket sin confundirlos. |

## Verificación y recuperación

1. Si el panel no avanza, mirar si el servidor confirmó el comando, si el evento quedó pendiente en outbox, si el broker recibió el mensaje y si el consumidor persistió la proyección. Buscar el mismo identificador en cada paso; no repetir una reserva con otra clave para «arreglarla».
2. Si no hay broker disponible en la defensa, ejecutar la demo REST verificable y mostrar un registro previo de integración **solo si existe**, rotulado con versión, fecha y entorno. Si no existe, decir que el tramo no se pudo verificar.
3. Si el navegador o panel falla, mostrar la respuesta JSON del dashboard. No sustituirla por valores a mano ni presentar capturas de otra versión.
4. Restablecer fixtures solo con el mecanismo documentado y en un entorno descartable. No borrar volúmenes de un ambiente ajeno o compartido para reiniciar una demo.

### Tres respuestas de defensa

- **¿Qué evita la sobre-reserva?** La comprobación y el cambio de cantidades están en la misma transacción de escritura. Ni el panel ni los mensajes deciden el saldo disponible.
- **¿Qué pasa si el broker recibe el evento y el publicador se cae?** Puede haber redelivery. El consumidor vuelve seguro el efecto en su proyección registrando el identificador del evento junto con el cambio.
- **¿Cómo sabemos que la reposición fue aprobada?** La transición fue autorizada y guardada; eso no equivale a que una orden de compra externa se haya enviado.

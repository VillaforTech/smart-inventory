# Smart Inventory | Plan de releases y criterios de aceptación

**Plan de implementación y matriz de comprobación**

Plan por hitos y resultados esperados. No constituye un historial de actividad.

## Equipo

Los créditos de contexto están en el README. Registrar las contribuciones a este componente con sus commits o PRs.

## Plan de cuatro sprints

| Sprint | Objetivo y trabajo planificado | Evidencia a vincular al cierre | Estado documental |
|---|---|---|---|
| 1 | Refinar requisitos e historias; arquitectura y contratos; backlog priorizado; OpenAPI base; criterios SAT; decisiones (ADR) y plan de prueba. | Historias aceptadas, contrato versionado, criterios SAT y decisiones de diseño. | Planificado; historial de ejecución no verificado. |
| 2 | Implementar inventario y reserva; persistencia; productor, consumidor y flujo por eventos. | Resultados unitarios, de integración y de contrato; demo de publicación, consumo y desduplicación. | Planificado; historial de ejecución no verificado. |
| 3 | Pipeline CI/CD, staging, límites de tasa y resiliencia. | URL/ID de workflow **ejecutado**, imagen versionada/digest, ambiente y pruebas SAST/SCA/SAT/DAST con resultados. | Planificado; CI/staging no comprobados aquí. |
| 4 | Observabilidad, carga, correcciones, publicación final y preparación de demostración. | Panel, logs, correlación/trazas según implementación, informe de carga, runbook, tag real, retrospectiva y presentación. | Planificado; publicación y retrospectiva no comprobadas aquí. |

## Backlog de aceptación

| ID | Criterio comprobable | Tipo de evidencia |
|---|---|---|
| SI-01 | Filtros y paginación devuelven solo los registros esperados; físico, reservado y disponible se reconcilian; un lector sin scope es rechazado. | OpenAPI + prueba de contrato e integración. |
| SI-02 | FEFO asigna por vencimiento; conflicto de disponibilidad no deja estado parcial; concurrencia no sobre-reserva; reintento idéntico no repite efecto; reuso de clave con cambios falla. | Pruebas de base de datos y HTTP; evidencia de asignación y contador. |
| SI-03 | Bajo umbral genera una sugerencia visible según los filtros; aprobación sin permiso falla; aprobación autorizada cambia estado una sola vez. | Prueba de autorización y transición de estado. |
| SI-04 | Panel distingue estado operativo de proyección e informa atraso/error/última sincronización, más inventario y órdenes/reservas. | Integración y ensayo de atraso/recuperación. |
| SI-05 | Evento conforma el sobre acordado; duplicado no duplica proyección; se verifica el comportamiento concreto cuando el broker/consumidor se recupera. | Prueba de contrato, replay y entorno independiente si está disponible. |
| SI-06 | Límites, calidad y release reproducible: documentar algoritmo aplicado, pasos de arranque, alcance efectivo de análisis y resultados. | Ejecución local/CI según proceda; registro de limitaciones. |

## Registro de evidencia (completar solo tras observarla)

Para cada comprobación anotar: fecha/hora y zona, commit o versión reproducida, comando exacto, entorno, artefacto o enlace, resultado y límites. Estados recomendados: **no ejecutada**, **aprobada**, **fallida** o **no verificable**. Un workflow en YAML, una captura del panel sin eventos o una imagen local sin digest no prueban despliegue ni prueba de carga.

| Comprobación | Qué conservar |
|---|---|
| Unitarias e integración | Comando reproducible, número de resultados, entorno; fallas y excepciones visibles. |
| Contratos | Esquema REST/OpenAPI y sobre de evento contrastados con pruebas reales. |
| CI/CD y staging | Enlace del run, commit, imagen/tag/digest, URL de staging si existe. No reutilizar resultados de otro repositorio. |
| SAST / SCA / DAST | Herramienta, versión, configuración/alcance, resultados y hallazgos pendientes. Una prueba HTTP de humo no es un DAST por sí sola. |
| SAT | Caso, precondición, resultado esperado, resultado observado, responsable verificado. |
| Carga | Umbrales p95 de consulta y comando, datos sintéticos, tamaño de muestra, concurrencia, errores y entorno. |
| Operación | Evidencia del panel y logs vinculados por correlación, procedimiento de recuperación con resultado visible. |
| Release | Versión, artefacto, instrucciones y pendientes. |

**Definición de terminado:** revisión del criterio de aceptación, prueba reproducible, ausencia de secretos/datos reales, cambios coherentes con el contrato y limitaciones explícitas. Mantener casos pendientes visibles en lugar de completar el estado por inferencia.

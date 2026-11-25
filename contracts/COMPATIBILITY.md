# Smart Inventory · Compatibility report

| Surface | Previous clients | Versioned contract |
|---|---|---|
| REST routes, auth, request and success shape | Kept on /api/v1. Route and schema settings checked against the legacy snapshot. | OpenAPI project version 1.1.0; the routes still use v1. |
| REST errors | Without an explicit Accept preference, existing application/json detail envelopes remain. | Explicit Accept: application/problem+json selects RFC 9457 errors and keeps authentication/retry headers. |
| Replenishment state | PENDING is the wire representation of the open business state. | PENDING → APPROVED; OPEN as a new wire enum would be a breaking change and is not silently substituted. |
| RabbitMQ | The existing dashboard queue, legacy routing keys and message shapes remain. | Two .v1 keys are published in addition to the old keys, carrying a semver envelope and renamed fields. A versioned consumer must bind its own queue. |
| Correlation | The old HTTP and broker correlation values remain unchanged. | UUID values are preserved and normalized. Non-UUID legacy values are deterministically mapped with UUIDv5. |
| Duplicate delivery | Dashboard applies once using eventId. | Both forms keep the same eventId. A consumer subscribed to both forms must deduplicate or choose one representation. |

The reservation event fires after the FEFO transaction creates a durable reservation. The risk event fires when eligible stock reaches or goes below the configured threshold and a new suggestion is created; replaying a request or an existing open alert does not create another event. It is not a forecast.

Both publishes are broker-confirmed before the outbox is marked; a failure can replay the legacy message with its original ID. The versioned message is not mandatory: an unbound route is not delivered to any consumer, and confirmation is not end-to-end receipt. Subscribers need durable bindings and own their ACK/retry/DLQ. The legacy dashboard continues ACK after persistence, two retry queues and terminal DLQ. Keep legacy bindings until all affected consumers migrate. To roll back, stop the versioned publisher; the legacy dashboard wire format does not change.

For a compatible change, add optional fields, preserve existing field semantics, and update the v1 minor version and compatibility tests together; existing consumers should ignore unknown members. Each published minor version retains its own validation snapshot. Removing/renaming required fields or changing their meaning requires a new .v2 type and route and a parallel migration window. Never reuse a published version for changed semantics.

These checks cover this migration and exercised fixtures, not arbitrary future schema differences. The PR job validates both published specifications and runs the HTTP and event data checks. Delivery to third-party queues, staging and production were not verified.

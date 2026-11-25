"""Regenerate the course contract snapshots with the recorded Smart Inventory scope."""
import copy, json, sys
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from inventory.contract_events import ROUTES
BASE = json.loads((ROOT / "docs/openapi.json").read_text())
OUT = ROOT / "contracts"
OUT.mkdir(exist_ok=True)
(OUT / "schemas").mkdir(exist_ok=True)
PATHS = {
    "/api/v1/inventory": ("get", "inventory:read", [400,401,403,422,429,503]),
    "/api/v1/reservations": ("post","inventory:reserve",[400,401,403,409,422,429,503]),
    "/api/v1/replenishment/suggestions": ("get","replenishment:read",[400,401,403,422,429,503]),
    "/api/v1/replenishment/suggestions/{suggestion_id}/approve": ("post","replenishment:approve",[401,403,404,409,429,503]),
}
STR = {"type":"string","minLength":1}
UUID = {"type":"string","format":"uuid"}
POS = {"type":"integer","minimum":1}
NONNEG = {"type":"integer","minimum":0}

def obj(props):
    return {"type":"object","properties":props,"required":list(props)}

def emit(name,data):
    write_yaml(name,data)

class NoAliases(yaml.SafeDumper):
    def ignore_aliases(self,data):
        return True

def write_yaml(name,data):
    (OUT/name).write_text(yaml.dump(data,Dumper=NoAliases,sort_keys=False,allow_unicode=True),encoding="utf-8")

spec = {**copy.deepcopy(BASE), "info":{
    **BASE["info"],"version":"1.1.0","description":
    "Contrato de cuatro operaciones. Para errores RFC 9457 envía Accept: application/problem+json. Sin opt-in se conserva application/json."
}, "paths":{key:copy.deepcopy(BASE["paths"][key]) for key in PATHS}}
spec["servers"]=[{"url":"http://127.0.0.1:8000","description":"Demo local con datos sintéticos"}]
spec["components"]["schemas"]["ProblemDetails"] = obj({
    "type":{"type":"string","format":"uri-reference"}, "title":STR, "status":{"type":"integer","minimum":400,"maximum":599},
    "detail":STR,"code":STR,"correlationId":STR})
spec["components"]["schemas"]["ProblemDetails"]["properties"]["fields"]={"type":"array","items":STR}
legacy_detail = obj({"code":STR,"message":STR})
legacy_detail["properties"]["fields"]={"type":"array","items":STR}
spec["components"]["schemas"]["LegacyError"] = obj({"detail":legacy_detail})
lot={"sku":"MILK-1L","location":"UIO-01","lot":"LOT-A","expires_on":"2099-01-02","physical":4,"reserved":0,"available":4}
sid="12345678-1234-4234-8234-123456789abc"
examples=[
 {"items":[lot],"page":1,"page_size":20,"total":1},
 {"reservation_id":sid,"sku":"MILK-1L","location":"UIO-01","quantity":6,"allocations":[{"lot":"LOT-A","quantity":4},{"lot":"LOT-B","quantity":2}],"replayed":False},
 {"items":[{"id":sid,"sku":"MILK-1L","location":"UIO-01","supplier":"DEMO-DAIRY","suggested_quantity":15,"state":"PENDING","created_at":"2026-11-25T00:00:00Z","approved_at":None}],"page":1,"page_size":20,"total":1},
 {"id":sid,"state":"APPROVED"}
]
from http import HTTPStatus
for (path,(method,scope,statuses)),example in zip(PATHS.items(),examples):
    operation=spec["paths"][path][method]
    operation["x-required-scopes"]=[scope]
    operation["description"]="Permiso requerido: "+scope+". "+{
        "inventory:read":"Orden fijo por SKU, ubicación, fecha de vencimiento y lote. No expone existencias de ubicaciones no autorizadas.",
        "inventory:reserve":"Asigna lotes FEFO no vencidos. Idempotency-Key por titular; repetir con cuerpo distinto devuelve 409.",
        "replenishment:read":"No cambia stock. PENDING es el estado OPEN de negocio, conservado por compatibilidad.",
        "replenishment:approve":"Compras: estado pendiente (PENDING, equivalente a OPEN) a APPROVED; no crea una compra automáticamente."
    }[scope]
    ok=next(code for code in operation["responses"] if code.startswith("2"))
    operation["responses"][ok]["content"]["application/json"]["example"]=example
    if "requestBody" in operation:
        operation["requestBody"]["content"]["application/json"]["example"]={"sku":"MILK-1L","location":"UIO-01","quantity":6}
    for param in operation.get("parameters",[]):
        if param["name"]=="Idempotency-Key":
            param["example"]="demo-reservation-01"
    for code in statuses:
        old=operation["responses"].get(str(code),{})
        entry={"description":old.get("description",HTTPStatus(code).phrase),"content":{
            "application/problem+json":{"schema":{"$ref":"#/components/schemas/ProblemDetails"}},
            "application/json":{"schema":{"$ref":"#/components/schemas/LegacyError"}}
        }}
        if code in (401,429):
            h="WWW-Authenticate" if code==401 else "Retry-After"
            entry["headers"]={h:{"schema":{"type":"string" if code==401 else "integer"}}}
        operation["responses"][str(code)]=entry
emit("openapi.yaml",spec)

events = {
    "stockReserved": (ROUTES["InventoryReserved"],"reservation-created.v1.schema.json",
        obj({"reservationId":UUID,"sku":STR,"qty":POS,"location":STR,
             "lots":{"type":"array","minItems":1,"items":obj({"lot":STR,"qty":POS})}}),
        "Una reserva FEFO se confirma, no se emite al repetir la misma clave."),
    "riskDetected": (ROUTES["LowStockDetected"],"replenishment-risk-detected.v1.schema.json",
        obj({"suggestionId":UUID,"sku":STR,"location":STR,"available":NONNEG,"reorderPoint":NONNEG,
             "supplier":STR,"suggestedQty":NONNEG}),
        "Stock elegible cae al umbral o menos y se crea una nueva sugerencia; no predice riesgo futuro.")
}
asyncapi={"asyncapi":"3.0.0","info":{"title":"Smart Inventory versioned events","version":"1.0.0",
         "description":"Proyecciones adicionales con los mismos identificadores de evento. La cola dashboard conserva el formato anterior."},
         "servers":{"local":{"host":"127.0.0.1:5672","protocol":"amqp","description":"Broker local; usa el perfil RabbitMQ del proyecto"}},
         "defaultContentType":"application/json","channels":{},"operations":{},"components":{"messages":{}}}
for key,(route,filename,payload,meaning) in events.items():
    schema={"$schema":"http://json-schema.org/draft-07/schema#","title":route,**obj({
        "eventId":UUID,"eventType":{"const":route.removesuffix(".v1")},"schemaVersion":{"const":"1.0.0"},
        "occurredAt":{"type":"string","format":"date-time","pattern":"(?:Z|\\+00:00)$"},
        "correlationId":UUID,"producer":{"const":"smart-inventory"},"payload":payload
    })}
    (OUT/"schemas"/filename).write_text(json.dumps(schema,indent=2)+"\n",encoding="utf-8")
    asyncapi["channels"][key]={"address":route,"description":meaning,
        "servers":[{"$ref":"#/servers/local"}],
        "bindings":{"amqp":{"is":"routingKey","exchange":{"name":"inventory.events","type":"topic",
             "durable":True,"autoDelete":False},"bindingVersion":"0.3.0"}},
        "messages":{key:{"$ref":f"#/components/messages/{key}"}}}
    asyncapi["operations"]["send"+key[0].upper()+key[1:]]={"action":"send","channel":{"$ref":f"#/channels/{key}"},
        "messages":[{"$ref":f"#/channels/{key}/messages/{key}"}],
        "bindings":{"amqp":{"deliveryMode":2,"mandatory":False,"bindingVersion":"0.3.0"}}}
    asyncapi["components"]["messages"][key]={"name":key,"title":route,"description":meaning,
        "contentType":"application/json","correlationId":{"location":"$message.payload#/correlationId"},
        "payload":{"schemaFormat":"application/schema+json;version=draft-07","schema":{"$ref":f"./schemas/{filename}"}}}
asyncapi["x-consumer-policy"]={"publisher":"smart-inventory worker, outbox transaccional y publisher confirms",
 "versionedSubscribers":"Cada consumidor declara su cola y liga una routing key .v1. El productor confirma aceptación del broker; sin binding no hay entrega. No se autoaprovisionan colas de terceros.",
 "legacyDashboard":{"queue":"inventory.dashboard","bindings":["inventory.reserved","inventory.low-stock"],
 "ack":"ACK tras persistencia idempotente; duplicados por eventId no reaplican efectos.",
 "retry":"Dos colas de reintento con 1 y 5 segundos. NACK sin requeue en errores terminales.",
 "deadLetterQueue":"inventory.dashboard.dead"},
 "versionedSubscriberRecommendation":"ACK después de persistir; deduplicar por eventId; retry/DLQ es responsabilidad del consumidor."}
emit("asyncapi.yaml",asyncapi)

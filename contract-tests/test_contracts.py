"""Verify documented HTTP exchanges and broker envelopes against their schemas."""
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

from jsonschema import Draft7Validator, Draft202012Validator, FormatChecker, ValidationError
import yaml

from inventory import worker
from inventory.contract_events import ROUTES
import test_api

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "contracts"
HTTP = yaml.safe_load((CONTRACTS / "openapi.yaml").read_text())
EVENTS = yaml.safe_load((CONTRACTS / "asyncapi.yaml").read_text())
LEGACY = json.loads((ROOT / "docs" / "openapi.json").read_text())

class ContractTests(unittest.TestCase):
    tokens = test_api.ApiTests.tokens
    setUp = test_api.ApiTests.setUp
    headers = test_api.ApiTests.headers
    request_reservation = test_api.ApiTests.request_reservation
    def _check(self, spec_path, method, response):
        media = response.headers["content-type"].split(";")[0]
        formats = HTTP["paths"][spec_path][method]["responses"][str(response.status_code)]["content"]
        self.assertIn(media, formats)
        schema = formats[media]["schema"]
        Draft202012Validator({**schema, "components": HTTP["components"]}, format_checker=FormatChecker()).validate(response.json())
        if media == "application/problem+json":
            self.assertEqual(response.json()["status"], response.status_code)
            self.assertEqual(response.json()["correlationId"], response.headers["X-Correlation-Id"])
            self.assertEqual(response.headers["Vary"], "Accept")

    def test_requests_and_responses_match_published_contract(self):
        i, r, s, a = list(HTTP["paths"])
        auth = self.headers()
        body={"sku":"MILK-1L","location":"UIO-01","quantity":6}
        request_schema=HTTP["paths"][r]["post"]["requestBody"]["content"]["application/json"]["schema"]
        Draft202012Validator({**request_schema, "components":HTTP["components"]}).validate(body)
        self._check(i, "get", self.client.get(i, headers=auth))
        self._check(r, "post", self.request_reservation())
        result=self.client.get(s,headers=auth)
        self._check(s, "get", result)
        endpoint=a.replace("{suggestion_id}",result.json()["items"][0]["id"])
        self._check(a, "post", self.client.post(endpoint,headers=auth))
        problem={"Accept":"application/problem+json"}
        for p,method,actual,code in [
            (i,"get",self.client.get(i,headers=problem),401),
            (i,"get",self.client.get(i,params={"location":"UIO-02"},headers=self.headers("one",**problem)),403),
            (r,"post",self.request_reservation(5,**problem),409),
            (r,"post",self.request_reservation(0,**problem),422),
            (s,"get",self.client.get(s,params={"state":"WRONG"},headers=self.headers(**problem)),400),
            (a,"post",self.client.post(endpoint,headers=self.headers(**problem)),409),
        ]:
            self.assertEqual(actual.status_code,code)
            self._check(p,method,actual)
        self._check(r,"post",self.request_reservation(5))
        qzero=self.client.get(i,headers={"Accept":"application/problem+json;q=0, application/json"})
        self.assertEqual(qzero.headers["content-type"],"application/json")

    def test_existing_request_and_success_shapes_unchanged_for_legacy_clients(self):
        old_api=self.api.openapi()
        for path, ops in HTTP["paths"].items():
            for method, operation in ops.items():
                baseline=LEGACY["paths"][path][method]
                runtime=old_api["paths"][path][method]
                self.assertEqual(operation["security"],baseline["security"])
                self.assertEqual(runtime["security"],baseline["security"])
                self.assertEqual(
                    [(p["name"],p["in"],p["required"]) for p in runtime.get("parameters",[])],
                    [(p["name"],p["in"],p["required"]) for p in baseline.get("parameters",[])])
                code=next(k for k in baseline["responses"] if k.startswith("2"))
                self.assertEqual(runtime["responses"][code]["content"]["application/json"]["schema"],
                                 operation["responses"][code]["content"]["application/json"]["schema"])

    def test_versioned_wire_messages_and_legacy_bindings(self):
        self.request_reservation(**{"X-Correlation-Id":"tracked-in-legacy"})
        self.request_reservation(**{"X-Correlation-Id":"tracked-in-legacy"})
        sent=[]
        channel=SimpleNamespace(basic_publish=lambda **packet:sent.append(packet))
        worker.publish_pending(channel,self.api.state.store,self.db,SimpleNamespace,include_versioned=True)
        self.assertEqual([p["routing_key"] for p in sent if p["mandatory"]],
                         ["inventory.reserved","inventory.low-stock"])
        published=[p for p in sent if not p["mandatory"]]
        self.assertEqual({p["routing_key"] for p in published},set(ROUTES.values()))
        self.assertEqual(len(published),2)
        for p in published:
            route = p["routing_key"]
            key = next(k for k,v in EVENTS["channels"].items() if v["address"]==route)
            ref = EVENTS["components"]["messages"][key]["payload"]["schema"]["$ref"]
            schema=json.loads((CONTRACTS/ref).read_text())
            Draft7Validator.check_schema(schema)
            validator=Draft7Validator(schema,format_checker=FormatChecker())
            actual=json.loads(p["body"])
            validator.validate(actual)
            validator.validate({**actual,"payload":{**actual["payload"],"optionalNote":"future"}})
            with self.assertRaises(ValidationError):
                validator.validate({**actual,"schemaVersion":"2.0.0"})
            self.assertEqual(p["properties"].message_id,actual["eventId"])
            self.assertEqual(p["properties"].delivery_mode,2)
        self.assertEqual(self.api.state.store.pending_events(),[])


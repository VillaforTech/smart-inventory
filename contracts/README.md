# API contracts

Contract-first artifacts for the inventory/reservations and replenishment flows.

- [OpenAPI](openapi.yaml): request/response schemas, scopes, and error representations.
- [AsyncAPI](asyncapi.yaml): versioned messages, RabbitMQ topology, and subscriber behavior.
- [Schemas](schemas/): the JSON envelope and payload schema for each event.
- [Compatibility report](COMPATIBILITY.md): what stayed unchanged and what subscribers need to migrate.

From the repository root with Python 3.12:

```sh
pip install -r requirements.txt -r requirements-contracts.txt
python -m openapi_spec_validator contracts/openapi.yaml
npx --yes --package=@asyncapi/cli asyncapi validate contracts/asyncapi.yaml
python -m unittest discover -s tests -v
PYTHONPATH=tests:. python -m unittest discover -s contract-tests -v
python scripts/render-contract-docs.py
```

The contract job fails on invalid specifications, failing request/response or event fixtures, or generated YAML drift. The HTML is generated locally, not published to an external documentation service. The public demo and test data remain synthetic.

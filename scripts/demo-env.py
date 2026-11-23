"""Create local-only credentials; refuse to overwrite a current configuration."""
import json
import os
import secrets
from pathlib import Path

path = Path('.env')
admin, viewer, north = (secrets.token_hex(24) for _ in range(3))
tokens = {
    admin: {"subject": "demo-admin", "scopes": ["inventory:read", "inventory:reserve", "replenishment:read", "replenishment:approve", "dashboard:read"], "locations": ["*"]},
    viewer: {"subject": "demo-viewer", "scopes": ["inventory:read", "replenishment:read", "dashboard:read"], "locations": ["*"]},
    north: {"subject": "demo-bodega-uio", "scopes": ["inventory:read", "inventory:reserve", "replenishment:read"], "locations": ["UIO-01"]},
}
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, 'w') as target:
    target.write(f"INVENTORY_TOKENS_JSON='{json.dumps(tokens,separators=(',',':'))}'\n")
    target.write("INVENTORY_SEED_DEMO=true\nRABBITMQ_USER=inventory\n")
    target.write(f"RABBITMQ_PASSWORD={secrets.token_hex(20)}\n")
print("Created .env with local-only tokens. Do not commit or publish .env.")

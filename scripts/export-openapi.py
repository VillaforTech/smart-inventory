"""Export the API contract without contacting a server or using real credentials."""
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from inventory.api import app
print(json.dumps(app.openapi(),ensure_ascii=False,indent=2))

"""Build a standalone escaped HTML contract overview, with no external assets."""
import html
from pathlib import Path

import yaml

root = Path(__file__).resolve().parents[1]
out = root / "contract-docs.html"
files = [root/"contracts"/name for name in ("openapi.yaml","asyncapi.yaml","COMPATIBILITY.md")]
records = []
for path in files:
    value = path.read_text()
    if path.suffix==".yaml":
        yaml.safe_load(value)
    records.append(f"<section><h2>{html.escape(path.name)}</h2><pre>{html.escape(value)}</pre></section>")
out.write_text("<!doctype html><html lang='es'><meta charset='utf-8'><meta name='viewport' content='width=device-width'><title>Smart Inventory · Contratos</title><style>body{max-width:72rem;margin:3rem auto;padding:0 1rem;font:1rem system-ui;color:#253046}pre{padding:1.25rem;background:#f2f5fa;overflow:auto;border-radius:.5rem}</style><h1>Smart Inventory · Contratos</h1>"+"".join(records)+"</html>")

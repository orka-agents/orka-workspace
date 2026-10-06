#!/usr/bin/env python3
"""Add the native documented stub resolver to this isolated kind CoreDNS."""
import ipaddress
import json
import sys

dns_ip = str(ipaddress.IPv4Address(sys.argv[1]))
current = json.load(sys.stdin)
original = current["data"]["Corefile"]
zone = "actors.resources.substrate.ate.dev"
block = f"\n{zone}:53 {{\n    errors\n    cache 30\n    forward . {dns_ip}\n}}\n"
if zone in original and block not in original:
    raise SystemExit("Refusing to replace an existing foreign native DNS rule")
updated = original if block in original else original + block
print(json.dumps([
    {"op": "test", "path": "/metadata/uid", "value": current["metadata"]["uid"]},
    {"op": "test", "path": "/metadata/resourceVersion", "value": current["metadata"]["resourceVersion"]},
    {"op": "test", "path": "/data/Corefile", "value": original},
    {"op": "replace", "path": "/data/Corefile", "value": updated},
]))

#!/usr/bin/env python3
"""Check imports and module graphs without depending on either implementation."""

import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
SHARED = "github.com/orka-agents/orka-workspace"
PROVIDERS = SHARED + "/providers"
ORKA = "github.com/orka-agents/orka"
BACKEND_MODULE = re.compile(r"(?:^|[/.-])(?:agent-sandbox|substrate|fiberd)(?:[/.-]|$)")


def json_stream(output):
    decoder = json.JSONDecoder()
    while output.strip():
        value, end = decoder.raw_decode(output.lstrip())
        yield value
        output = output.lstrip()[end:]


def go_json(directory, *args):
    result = subprocess.run(
        ["go", *args], cwd=directory, text=True, capture_output=True, check=True
    )
    return list(json_stream(result.stdout))


def within(path, module):
    return path == module or path.startswith(module + "/")


def provider_name(path):
    if not path.startswith(PROVIDERS + "/"):
        return None
    name = path.removeprefix(PROVIDERS + "/").split("/")[0]
    return name.removesuffix(".test").removesuffix("_test")


def check():
    violations = set()
    for directory, shared in [(ROOT, True), (ROOT / "providers", False)]:
        for module in go_json(directory, "list", "-m", "-json", "all"):
            path = module["Path"]
            if within(path, ORKA):
                violations.add(f"{directory.name}: module graph depends on Orka: {path}")
            if shared and (within(path, PROVIDERS) or BACKEND_MODULE.search(path)):
                violations.add(f"shared module contains a provider dependency: {path}")

        for package in go_json(directory, "list", "-deps", "-test", "-json", "./..."):
            source = package.get("ForTest", package["ImportPath"]).split(" [")[0]
            # Imports includes compiled test imports when go list uses -test.
            for imported in package.get("Imports", []):
                target = imported.split(" [")[0]
                if within(target, ORKA):
                    violations.add(f"{source} imports Orka: {target}")
                if shared and within(target, PROVIDERS):
                    violations.add(f"shared package {source} imports provider: {target}")
                owner = provider_name(source)
                other = provider_name(target)
                if owner and other and owner != other:
                    violations.add(f"provider {owner} imports provider {other}: {source} -> {target}")

    if violations:
        for violation in sorted(violations):
            print(violation, file=sys.stderr)
        return 1
    print("Dependency boundaries passed for shared and provider modules.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(check())
    except subprocess.CalledProcessError as error:
        print(error.stderr, file=sys.stderr, end="")
        sys.exit(error.returncode)

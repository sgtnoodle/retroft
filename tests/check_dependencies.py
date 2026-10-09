#!/usr/bin/env python3
"""Check the recorder/exporter dependency boundary without building or tracing."""
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def cargo(*args):
    return subprocess.check_output(["cargo", *args, "--offline"], cwd=ROOT, text=True)


def dependencies(package, *options):
    lines = cargo("tree", "-p", package, "--edges", "normal,build", "--prefix", "none", *options)
    return {line.split()[0] for line in lines.splitlines() if line.strip()}


metadata = json.loads(cargo("metadata", "--no-deps", "--format-version", "1"))
packages = {package["id"]: package["name"] for package in metadata["packages"]}
assert [packages[key] for key in metadata["workspace_default_members"]] == ["retroft"]

checks = [
    ("retroft", (), {"retroft-export", "retroft-trigger", "prost", "prost-derive"}),
    ("retroft", ("--no-default-features",),
     {"retroft-export", "retroft-trigger", "prost", "prost-derive", "lz4", "lz4-sys"}),
    ("retroft-trigger", (), {"retroft", "retroft-export", "retroft-format", "io-uring", "crc32c", "lz4", "lz4-sys", "prost"}),
    ("retroft-export", (), {"retroft", "retroft-trigger", "io-uring", "prost", "prost-derive"}),
    ("retroft-format", (), {"retroft", "retroft-export", "retroft-trigger", "io-uring", "lz4", "lz4-sys", "serde", "serde_json", "prost"}),
]
for package, options, forbidden in checks:
    actual = dependencies(package, *options)
    assert not (actual & forbidden), (package, options, actual & forbidden)
    print(package, " ".join(options) or "default features", "dependency boundary: verified")

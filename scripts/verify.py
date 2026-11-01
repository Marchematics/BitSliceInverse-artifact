#!/usr/bin/env python3
"""Verify the published source and measurement checksums."""
import hashlib
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]


def main():
    manifest=json.loads((ROOT/'manifest.json').read_text())
    for name,expected in manifest['files'].items():
        p=ROOT/name
        assert hashlib.sha256(p.read_bytes()).hexdigest()==expected,name
        assert p.suffix.lower() not in {'.tex','.bib','.sty','.cls','.pdf','.pptx'},name
    frozen=ROOT/'research/real_decoders_20261024/src_eai_20261101/eai_cube3.cpp'
    assert hashlib.sha256(frozen.read_bytes()).hexdigest()=='c93a4530f7b21d5e1a1f174374f5fdd49783a20ad32638b370baabf3e86562f0'
    print(f"Verified {len(manifest['files'])} files and the frozen ECUBE03 source.")


if __name__=='__main__':main()

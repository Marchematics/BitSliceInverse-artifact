"""Shared helpers: BSSTR01 dictionary writer/reader + canonical row comparison."""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import hashlib
import json
import struct


def write_bsstr(path, names):
    """BSSTR01: magic(8) + 40-byte digest + u32 k + (u32 len, bytes)*k."""
    with open(path, "wb") as f:
        f.write(b"BSSTR01\n")
        f.write(hashlib.sha256("\n".join(names).encode()).hexdigest()[:40].encode())
        f.write(struct.pack("<I", len(names)))
        for n in names:
            b = n.encode()
            f.write(struct.pack("<I", len(b)))
            f.write(b)


def read_bsstr(path):
    d = open(path, "rb").read()
    if d[:8] != b"BSSTR01\n":
        raise ValueError("bad BSSTR01 magic in %s" % path)
    k = struct.unpack("<I", d[48:52])[0]
    p = 52
    names = []
    for _ in range(k):
        ln = struct.unpack("<I", d[p:p + 4])[0]
        p += 4
        names.append(d[p:p + ln].decode())
        p += ln
    return names


def norm_val(x):
    if x is None:
        return "NULL"
    if isinstance(x, bool):
        return str(int(x))
    if isinstance(x, int):
        return str(x)
    if isinstance(x, str):
        return x
    s = str(x)
    if "." in s:  # Decimal from e.g. SUM over DECIMAL - should not occur
        return str(int(x)) if x == int(x) else s
    return s


def canon_row(r):
    return (norm_val(r[0]),) + tuple(norm_val(x) for x in r[1:])


def canon_rows(rows):
    return sorted([list(canon_row(r)) for r in rows])


def rows_digest(rows):
    return hashlib.sha256(
        json.dumps(rows, separators=(",", ":")).encode()).hexdigest()


def digest_cube_rows(rows):
    """rows: list of 5-element string lists from the cube JSON."""
    return hashlib.sha256(
        json.dumps(sorted(rows), separators=(",", ":")).encode()).hexdigest()

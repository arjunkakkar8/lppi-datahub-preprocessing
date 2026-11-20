from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

CHUNK = 1 << 20

PRODUCTS = ("acs1", "acs5")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK):
            h.update(chunk)
    return h.hexdigest()


def canonical_json(obj: Any) -> bytes:
    """Stable serialisation used for every content hash."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sha256_json(obj: Any) -> str:
    return hashlib.sha256(canonical_json(obj)).hexdigest()


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=False, ensure_ascii=False) + "\n")


def write_json_atomic(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(json.dumps(obj, indent=2, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parquet_columns(path: Path) -> list[dict[str, Any]]:
    """Column list in the manifest's shape: name, Arrow type, nullable."""
    schema = pq.read_schema(path)
    return [{"name": f.name, "arrow_type": str(f.type), "nullable": bool(f.nullable)} for f in schema]


def parquet_rows(path: Path) -> int:
    return pq.ParquetFile(path).metadata.num_rows


def parquet_kv(path: Path) -> dict[str, str]:
    md = pq.read_schema(path).metadata or {}
    return {k.decode(): v.decode() for k, v in md.items() if not k.startswith(b"ARROW:")}


def link_or_copy(src: Path, dst: Path) -> None:
    """Hard-link a store file into a release; copy when the filesystems differ."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def make_read_only(root: Path) -> None:
    for p in root.rglob("*"):
        if p.is_file():
            p.chmod(0o444)


def check_product(product: str) -> None:
    if product not in PRODUCTS:
        raise ValueError(f"unknown product {product!r}; expected one of {PRODUCTS}")

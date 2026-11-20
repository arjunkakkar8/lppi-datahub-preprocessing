"""Content-addressed vintage store.

Each (product, period) is built once and kept. A build directory is named by the hash of
its files, so the same bytes always land in the same place, and a release only lists the
builds it serves. ``inputs_key`` (a hash of the extract, pipeline commit, contract version
and registry) lets the pipeline skip a rebuild when nothing it depends on changed.
"""

from __future__ import annotations

import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .util import (
    check_product,
    make_read_only,
    now_iso,
    parquet_columns,
    parquet_rows,
    read_json,
    sha256_file,
    sha256_json,
    write_json,
)

PERSONS = "persons.parquet"
RECORD = "vintage.json"
WEIGHTS = ("standard", "experimental")


@dataclass(frozen=True)
class StoredVintage:
    path: Path
    record: dict[str, Any]

    @property
    def build_hash(self) -> str:
        return self.record["build_hash"]

    @property
    def product(self) -> str:
        return self.record["product"]

    @property
    def vintage(self) -> int:
        return self.record["vintage"]

    def file(self, name: str) -> Path:
        return self.path / name


def period_dir(product: str, period: tuple[int, int]) -> str:
    start, end = period
    return f"{product}-{end}" if product == "acs1" else f"{product}-{start}-{end}"


class VintageStore:
    def __init__(self, data_root: Path):
        self.root = Path(data_root) / "store"

    def put(
        self,
        *,
        product: str,
        period: tuple[int, int],
        files: dict[str, Path],
        inputs: dict[str, Any],
        dollar_year: int | None = None,
        weights: str = "standard",
        puma_vintage: str | None = None,
    ) -> StoredVintage:
        """Copy a finished build into the store. Idempotent for identical bytes."""
        check_product(product)
        start, end = period
        if product == "acs1" and start != end:
            raise ValueError("acs1 period must be a single year")
        if product == "acs5" and end - start != 4:
            raise ValueError("acs5 period must span five years")
        if weights not in WEIGHTS:
            raise ValueError(f"weights must be one of {WEIGHTS}")
        if PERSONS not in files:
            raise ValueError(f"a vintage build needs {PERSONS}")

        entries = []
        for name in sorted(files):
            src = Path(files[name])
            entry = {"name": name, "sha256": sha256_file(src), "bytes": src.stat().st_size}
            if name.endswith(".parquet"):
                entry["rows"] = parquet_rows(src)
            entries.append(entry)
        build_hash = sha256_json(
            {"product": product, "period": [start, end], "files": entries}
        )

        parent = self.root / period_dir(product, period)
        target = parent / build_hash
        if target.exists():
            return self.load(target)

        record = {
            "product": product,
            "vintage": end,
            "period": [start, end],
            "build_hash": build_hash,
            "inputs": inputs,
            "inputs_key": sha256_json(inputs),
            "dollar_year": dollar_year if dollar_year is not None else end,
            "weights": weights,
            "puma_vintage": puma_vintage,
            "rows": next(e["rows"] for e in entries if e["name"] == PERSONS),
            "files": entries,
            "columns": parquet_columns(Path(files[PERSONS])),
            "created_at": now_iso(),
        }

        parent.mkdir(parents=True, exist_ok=True)
        tmp = Path(tempfile.mkdtemp(dir=parent, prefix=".tmp-"))
        try:
            for name, src in files.items():
                shutil.copy2(src, tmp / name)
            # Re-check the copies so a torn copy never enters the store.
            for e in entries:
                if sha256_file(tmp / e["name"]) != e["sha256"]:
                    raise OSError(f"copy of {e['name']} does not match its source")
            write_json(tmp / RECORD, record)
            make_read_only(tmp)
            tmp.rename(target)
        except BaseException:
            shutil.rmtree(tmp, ignore_errors=True)
            raise
        return self.load(target)

    def load(self, path: Path) -> StoredVintage:
        return StoredVintage(path=path, record=read_json(path / RECORD))

    def builds(self, product: str | None = None, vintage: int | None = None) -> list[StoredVintage]:
        out = []
        if not self.root.exists():
            return out
        for rec in sorted(self.root.glob(f"*/*/{RECORD}")):
            sv = self.load(rec.parent)
            if product and sv.product != product:
                continue
            if vintage and sv.vintage != vintage:
                continue
            out.append(sv)
        return sorted(out, key=lambda s: s.record["created_at"])

    def find(self, product: str, vintage: int, build: str = "latest") -> StoredVintage:
        """Resolve a build: "latest", a build-hash prefix, or ``inputs:<inputs_key>``."""
        cands = self.builds(product, vintage)
        if not cands:
            raise LookupError(f"no stored build for {product} {vintage}")
        if build == "latest":
            return cands[-1]
        if build.startswith("inputs:"):
            key = build.removeprefix("inputs:")
            hits = [c for c in cands if c.record["inputs_key"] == key]
        else:
            hits = [c for c in cands if c.build_hash.startswith(build)]
        if len(hits) != 1:
            raise LookupError(f"{product} {vintage} build {build!r} matched {len(hits)} builds")
        return hits[0]

    def has_inputs(self, product: str, vintage: int, inputs: dict[str, Any]) -> StoredVintage | None:
        """Return the stored build made from exactly these inputs, so the pipeline can skip it."""
        key = sha256_json(inputs)
        for c in self.builds(product, vintage):
            if c.record["inputs_key"] == key:
                return c
        return None

    def unreferenced(self, data_root: Path) -> list[StoredVintage]:
        """Builds no release lists. Reported only; deleting is left to a person."""
        used: set[str] = set()
        for m in (Path(data_root) / "releases").glob("*/manifest.json"):
            used.update(v["store_build"] for v in read_json(m)["vintages"])
        return [b for b in self.builds() if b.build_hash not in used]

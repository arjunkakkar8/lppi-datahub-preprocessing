"""Assemble an immutable release folder from stored vintages and shared files."""

from __future__ import annotations

import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .store import PERSONS, VintageStore
from .util import (
    PRODUCTS,
    check_product,
    link_or_copy,
    make_read_only,
    now_iso,
    read_json,
    sha256_file,
    sha256_json,
    write_json,
)

MANIFEST = "manifest.json"
CODEBOOK_FILES = ("variables.json", "values.json")


@dataclass(frozen=True)
class VintageRef:
    product: str
    vintage: int
    build: str = "latest"  # "latest", a build-hash prefix, or "inputs:<key>"


@dataclass
class ReleaseSpec:
    """What a release serves. Usually read from release.json in the pipeline repo."""

    label: str  # e.g. "ldh-2026.r1"
    contract_version: str
    vintages: list[VintageRef]
    codebook: dict[str, Path]  # variables.json, values.json
    rules: dict[str, Path]  # one rules.json per product
    tables: dict[str, Path] = field(default_factory=dict)  # voting.parquet, ...
    pipeline_commit: str | None = None

    @classmethod
    def from_json(cls, path: Path) -> ReleaseSpec:
        raw = read_json(path)
        base = path.parent
        return cls(
            label=raw["label"],
            contract_version=raw["contract_version"],
            vintages=[VintageRef(**v) for v in raw["vintages"]],
            codebook={k: base / v for k, v in raw["codebook"].items()},
            rules={k: base / v for k, v in raw["rules"].items()},
            tables={k: base / v for k, v in raw.get("tables", {}).items()},
            pipeline_commit=raw.get("pipeline_commit"),
        )


def _vintage_entry(sv, product: str) -> dict[str, Any]:
    rec = sv.record
    prefix = f"product={product}/vintage={rec['vintage']}"
    return {
        "product": product,
        "vintage": rec["vintage"],
        "period": rec["period"],
        "dollar_year": rec["dollar_year"],
        "weights": rec["weights"],
        "puma_vintage": rec["puma_vintage"],
        "ipums_extract": rec["inputs"].get("ipums_extract"),
        "ipums_doi": rec["inputs"].get("ipums_doi"),
        "rows": rec["rows"],
        "store_build": rec["build_hash"],
        "files": [
            {"path": f"{prefix}/{e['name']}", "sha256": e["sha256"], "bytes": e["bytes"]}
            | ({"rows": e["rows"]} if "rows" in e else {})
            for e in rec["files"]
        ],
        "columns": rec["columns"],
    }


def build_release(spec: ReleaseSpec, data_root: Path) -> Path:
    """Write releases/<release_id>/ and return its path.

    release_id = "<label>-<first 8 hex of the content hash>". The content hash covers the
    contract version, every vintage entry and every shared file, and not the build time, so
    building the same inputs twice gives the same id and the same folder.
    """
    data_root = Path(data_root)
    store = VintageStore(data_root)

    seen: set[tuple[str, int]] = set()
    stored = []
    for ref in spec.vintages:
        check_product(ref.product)
        key = (ref.product, ref.vintage)
        if key in seen:
            raise ValueError(f"vintage listed twice: {key}")
        seen.add(key)
        stored.append(store.find(ref.product, ref.vintage, ref.build))

    products = sorted({sv.product for sv in stored})
    missing_rules = [p for p in products if p not in spec.rules]
    if missing_rules:
        raise ValueError(f"no rules.json for product(s) {missing_rules}")
    extra_rules = [p for p in spec.rules if p not in PRODUCTS]
    if extra_rules:
        raise ValueError(f"rules for unknown product(s) {extra_rules}")
    missing_codebook = [n for n in CODEBOOK_FILES if n not in spec.codebook]
    if missing_codebook:
        raise ValueError(f"codebook is missing {missing_codebook}")

    vintages = sorted(
        (_vintage_entry(sv, sv.product) for sv in stored),
        key=lambda v: (v["product"], -v["vintage"]),
    )

    shared: list[tuple[str, Path]] = [(n, spec.codebook[n]) for n in CODEBOOK_FILES]
    shared += [(f"product={p}/rules.json", spec.rules[p]) for p in products]
    shared += [(f"{name}.parquet" if not name.endswith(".parquet") else name, src)
               for name, src in sorted(spec.tables.items())]
    files = [
        {"path": rel, "sha256": sha256_file(src), "bytes": src.stat().st_size}
        for rel, src in shared
    ]

    body = {
        "contract_version": spec.contract_version,
        "label": spec.label,
        "vintages": vintages,
        "files": files,
    }
    content_hash = sha256_json(body)
    release_id = f"{spec.label}-{content_hash[:8]}"
    manifest = {
        "contract_version": spec.contract_version,
        "release_id": release_id,
        "label": spec.label,
        "content_hash": content_hash,
        "created_at": now_iso(),
        "pipeline_commit": spec.pipeline_commit,
        "vintages": vintages,
        "files": files,
    }

    releases = data_root / "releases"
    target = releases / release_id
    if target.exists():
        existing = read_json(target / MANIFEST)
        if existing["content_hash"] != content_hash:  # only if a hash prefix collides
            raise FileExistsError(f"{release_id} exists with different content")
        return target

    releases.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(dir=releases, prefix=".tmp-"))
    try:
        for sv, entry in zip(sorted(stored, key=lambda s: (s.product, -s.vintage)), vintages):
            for f in entry["files"]:
                link_or_copy(sv.file(Path(f["path"]).name), tmp / f["path"])
        for (rel, src), f in zip(shared, files):
            dst = tmp / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            if sha256_file(dst) != f["sha256"]:
                raise OSError(f"copy of {rel} does not match its source")
        write_json(tmp / MANIFEST, manifest)
        make_read_only(tmp)
        tmp.rename(target)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    return target


def persons_path(release_dir: Path, product: str, vintage: int) -> Path:
    return release_dir / f"product={product}/vintage={vintage}/{PERSONS}"

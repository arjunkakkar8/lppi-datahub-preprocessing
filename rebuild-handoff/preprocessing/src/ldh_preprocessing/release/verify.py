"""Check a release folder against its manifest before anything serves it.

The engine repeats the schema part at load time with the Zod schemas; this is the
pipeline's own gate, and its result is what ``publish`` requires.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .assemble import MANIFEST
from .util import (
    PRODUCTS,
    now_iso,
    parquet_columns,
    parquet_kv,
    parquet_rows,
    read_json,
    sha256_file,
    sha256_json,
    write_json_atomic,
)

REQUIRED_TOP = ("contract_version", "release_id", "label", "content_hash", "vintages", "files")
REQUIRED_VINTAGE = ("product", "vintage", "period", "dollar_year", "weights", "rows",
                    "store_build", "files", "columns")


@dataclass
class Report:
    release_id: str
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    checked_files: int = 0

    @property
    def ok(self) -> bool:
        return not self.errors

    def as_dict(self, manifest_sha256: str) -> dict[str, Any]:
        return {
            "release_id": self.release_id,
            "status": "passed" if self.ok else "failed",
            "manifest_sha256": manifest_sha256,
            "checked_at": now_iso(),
            "checked_files": self.checked_files,
            "errors": self.errors,
            "warnings": self.warnings,
        }


def _check_file(rel_root: Path, entry: dict[str, Any], rep: Report) -> Path | None:
    p = rel_root / entry["path"]
    if not p.is_file():
        rep.errors.append(f"{entry['path']}: missing")
        return None
    rep.checked_files += 1
    if p.stat().st_size != entry["bytes"]:
        rep.errors.append(f"{entry['path']}: {p.stat().st_size} bytes, manifest says {entry['bytes']}")
    if sha256_file(p) != entry["sha256"]:
        rep.errors.append(f"{entry['path']}: sha256 does not match the manifest")
    if "rows" in entry and parquet_rows(p) != entry["rows"]:
        rep.errors.append(f"{entry['path']}: {parquet_rows(p)} rows, manifest says {entry['rows']}")
    return p


def _codebook_columns(variables: Any) -> dict[str, dict[str, Any]]:
    """Accept either {"variables": {name: {...}}} or {"variables": [{"name": ...}]}."""
    v = variables.get("variables", variables) if isinstance(variables, dict) else variables
    if isinstance(v, dict):
        return {k: (val or {}) for k, val in v.items()}
    return {item["name"]: item for item in v}


def _available(entry: dict[str, Any], product: str, vintage: int) -> bool:
    """A variable is expected in a vintage unless its availability says otherwise."""
    avail = entry.get("availability")
    if avail is None:
        return False  # aggregate-only or not declared per vintage: not checked here
    return vintage in (avail.get(product) or [])


def verify_release(release_dir: Path, *, write_report: bool = True) -> Report:
    release_dir = Path(release_dir)
    data_root = release_dir.parent.parent
    mpath = release_dir / MANIFEST
    m = read_json(mpath)
    rep = Report(release_id=m.get("release_id", release_dir.name))

    for k in REQUIRED_TOP:
        if k not in m:
            rep.errors.append(f"manifest: missing {k}")
    if rep.errors:
        return _finish(rep, mpath, data_root, write_report)

    if m["release_id"] != release_dir.name:
        rep.errors.append(f"manifest release_id {m['release_id']} != folder {release_dir.name}")
    body = {k: m[k] for k in ("contract_version", "label", "vintages", "files")}
    content_hash = sha256_json(body)
    if content_hash != m["content_hash"]:
        rep.errors.append("manifest content_hash does not match its contents")
    if not m["release_id"].endswith(content_hash[:8]):
        rep.errors.append("release_id does not end with the content hash")

    seen = set()
    for v in m["vintages"]:
        missing = [k for k in REQUIRED_VINTAGE if k not in v]
        if missing:
            rep.errors.append(f"vintage {v.get('product')} {v.get('vintage')}: missing {missing}")
            continue
        key = (v["product"], v["vintage"])
        if v["product"] not in PRODUCTS:
            rep.errors.append(f"vintage {key}: unknown product")
        if key in seen:
            rep.errors.append(f"vintage {key}: listed twice")
        seen.add(key)
        if v["period"][1] != v["vintage"]:
            rep.errors.append(f"vintage {key}: vintage must be the period's last year")
        if v["weights"] not in ("standard", "experimental"):
            rep.errors.append(f"vintage {key}: weights must be standard or experimental")
        if v["product"] == "acs1" and v["vintage"] == 2020 and v["weights"] != "experimental":
            rep.errors.append("acs1 2020 must carry weights = experimental")

        persons = None
        for f in v["files"]:
            p = _check_file(release_dir, f, rep)
            if p is not None and p.name == "persons.parquet":
                persons = p
        if persons is None:
            rep.errors.append(f"vintage {key}: no persons.parquet")
            continue
        if parquet_rows(persons) != v["rows"]:
            rep.errors.append(f"vintage {key}: row count differs from manifest")
        actual = parquet_columns(persons)
        if actual != v["columns"]:
            want = {c["name"]: c for c in v["columns"]}
            have = {c["name"]: c for c in actual}
            diffs = [f"column {n} missing from file" for n in sorted(want.keys() - have.keys())]
            diffs += [f"column {n} not in manifest" for n in sorted(have.keys() - want.keys())]
            diffs += [
                f"column {n} is {have[n]}, manifest says {want[n]}"
                for n in sorted(want.keys() & have.keys())
                if want[n] != have[n]
            ]
            rep.errors += [f"vintage {key}: {d}" for d in diffs or ["column order differs from manifest"]]
        kv = parquet_kv(persons)
        if kv.get("contract_version") != m["contract_version"]:
            rep.errors.append(
                f"vintage {key}: file contract_version {kv.get('contract_version')} != {m['contract_version']}"
            )
        if kv.get("product") != v["product"] or kv.get("vintage") != str(v["vintage"]):
            rep.errors.append(f"vintage {key}: file metadata names {kv.get('product')} {kv.get('vintage')}")

    paths = {f["path"] for f in m["files"]}
    for f in m["files"]:
        p = _check_file(release_dir, f, rep)
        if p is not None and p.suffix == ".json":
            try:
                json.loads(p.read_text())
            except ValueError as e:
                rep.errors.append(f"{f['path']}: invalid JSON ({e})")
    for product in sorted({v["product"] for v in m["vintages"]}):
        if f"product={product}/rules.json" not in paths:
            rep.errors.append(f"product {product}: no rules.json")
    for name in ("variables.json", "values.json"):
        if name not in paths:
            rep.errors.append(f"{name} missing from manifest")

    # Every variable the codebook says exists in a vintage must be a column of that vintage.
    var_path = release_dir / "variables.json"
    if var_path.is_file():
        try:
            variables = _codebook_columns(read_json(var_path))
        except (ValueError, KeyError, TypeError) as e:
            rep.errors.append(f"variables.json: unreadable codebook ({e})")
            variables = {}
        for v in m["vintages"]:
            cols = {c["name"] for c in v["columns"]}
            for name, entry in variables.items():
                if _available(entry, v["product"], v["vintage"]) and name not in cols:
                    rep.errors.append(
                        f"variables.json: {name} is listed for {v['product']} {v['vintage']} but is not a column"
                    )

    # Files on disk that the manifest does not list.
    listed = paths | {f["path"] for v in m["vintages"] for f in v["files"]} | {MANIFEST}
    for p in release_dir.rglob("*"):
        if p.is_file() and str(p.relative_to(release_dir)) not in listed:
            rep.warnings.append(f"{p.relative_to(release_dir)}: on disk but not in the manifest")

    return _finish(rep, mpath, data_root, write_report)


def _finish(rep: Report, mpath: Path, data_root: Path, write_report: bool) -> Report:
    if write_report:
        write_json_atomic(
            data_root / "checks" / rep.release_id / "verify.json",
            rep.as_dict(sha256_file(mpath)),
        )
    return rep

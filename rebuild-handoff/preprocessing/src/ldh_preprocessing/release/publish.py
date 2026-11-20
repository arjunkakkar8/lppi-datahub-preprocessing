"""Publish pointer: which release a channel serves.

channels/<channel>.json is replaced atomically (write a temp file, then rename), so a
reader sees the old pointer or the new one, never half of either. Every move is appended
to channels/history.jsonl. Rolling back is publishing the previous release again.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .assemble import MANIFEST
from .util import now_iso, read_json, sha256_file, write_json_atomic

CHANNEL_NAMES = ("candidate", "preview", "production")


class PublishRefused(RuntimeError):
    pass


def read_channel(data_root: Path, channel: str) -> dict[str, Any] | None:
    p = Path(data_root) / "channels" / f"{channel}.json"
    return read_json(p) if p.exists() else None


def publish(
    data_root: Path,
    release_id: str,
    channel: str = "candidate",
    *,
    require: tuple[str, ...] = ("verify",),
    note: str | None = None,
) -> dict[str, Any]:
    """Point ``channel`` at ``release_id``.

    Refuses unless every check in ``require`` (checks/<release_id>/<name>.json) passed
    against the manifest exactly as it is now. The parity harness can write
    checks/<release_id>/parity.json in the same shape and be added to ``require``.
    """
    if channel not in CHANNEL_NAMES:
        raise ValueError(f"channel must be one of {CHANNEL_NAMES}")
    data_root = Path(data_root)
    release_dir = data_root / "releases" / release_id
    mpath = release_dir / MANIFEST
    if not mpath.is_file():
        raise PublishRefused(f"no release {release_id}")
    manifest_sha = sha256_file(mpath)

    for name in require:
        cp = data_root / "checks" / release_id / f"{name}.json"
        if not cp.is_file():
            raise PublishRefused(f"{release_id}: no {name} check; run it first")
        check = read_json(cp)
        if check.get("status") != "passed":
            raise PublishRefused(f"{release_id}: {name} check {check.get('status')}")
        if check.get("manifest_sha256") != manifest_sha:
            raise PublishRefused(f"{release_id}: {name} check ran on a different manifest")

    previous = read_channel(data_root, channel)
    if previous and previous["release_id"] == release_id:
        return previous

    manifest = read_json(mpath)
    pointer = {
        "channel": channel,
        "release_id": release_id,
        "manifest_sha256": manifest_sha,
        "contract_version": manifest["contract_version"],
        "vintages": [[v["product"], v["vintage"]] for v in manifest["vintages"]],
        "published_at": now_iso(),
        "previous": previous["release_id"] if previous else None,
        "checks": list(require),
        "note": note,
    }
    write_json_atomic(data_root / "channels" / f"{channel}.json", pointer)
    with open(data_root / "channels" / "history.jsonl", "a") as f:
        f.write(json.dumps(pointer) + "\n")
    return pointer

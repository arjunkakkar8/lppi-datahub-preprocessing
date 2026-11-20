"""Command line: python -m ldh_preprocessing.release <command> ...

    put       copy a finished vintage build into the store
    build     assemble a release from release.json
    verify    check a release against its manifest (writes checks/<id>/verify.json)
    publish   point a channel at a verified release
    status    show channels and stored builds
    unused    list stored builds that no release serves (never deletes)
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .assemble import ReleaseSpec, build_release
from .publish import CHANNEL_NAMES, PublishRefused, publish, read_channel
from .store import VintageStore
from .verify import verify_release


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ldh-release")
    ap.add_argument("--data", type=Path, default=Path("ldh-data"), help="data root (default ./ldh-data)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("put")
    p.add_argument("product", choices=["acs1", "acs5"])
    p.add_argument("period", help="e.g. 2020-2024, or 2024 for acs1")
    p.add_argument("--persons", type=Path, required=True)
    p.add_argument("--geographies", type=Path)
    p.add_argument("--inputs", type=Path, required=True, help="JSON: extract, DOI, pipeline commit, ...")
    p.add_argument("--weights", default="standard", choices=["standard", "experimental"])
    p.add_argument("--dollar-year", type=int)
    p.add_argument("--puma-vintage")

    p = sub.add_parser("build")
    p.add_argument("spec", type=Path, help="release.json")

    p = sub.add_parser("verify")
    p.add_argument("release_id")

    p = sub.add_parser("publish")
    p.add_argument("release_id")
    p.add_argument("--channel", default="candidate", choices=CHANNEL_NAMES)
    p.add_argument("--require", default="verify", help="comma-separated checks that must pass")
    p.add_argument("--note")

    sub.add_parser("status")
    sub.add_parser("unused")

    a = ap.parse_args(argv)
    store = VintageStore(a.data)

    if a.cmd == "put":
        start, _, end = a.period.partition("-")
        period = (int(start), int(end or start))
        files = {"persons.parquet": a.persons}
        if a.geographies:
            files["geographies.parquet"] = a.geographies
        sv = store.put(
            product=a.product, period=period, files=files,
            inputs=json.loads(a.inputs.read_text()), weights=a.weights,
            dollar_year=a.dollar_year, puma_vintage=a.puma_vintage,
        )
        print(f"{sv.product} {sv.vintage} build {sv.build_hash[:12]} at {sv.path}")
    elif a.cmd == "build":
        path = build_release(ReleaseSpec.from_json(a.spec), a.data)
        print(path.name)
    elif a.cmd == "verify":
        rep = verify_release(a.data / "releases" / a.release_id)
        for w in rep.warnings:
            print(f"warning: {w}")
        for e in rep.errors:
            print(f"error: {e}")
        print(f"{a.release_id}: {'passed' if rep.ok else 'FAILED'} ({rep.checked_files} files)")
        return 0 if rep.ok else 1
    elif a.cmd == "publish":
        try:
            ptr = publish(a.data, a.release_id, a.channel,
                          require=tuple(x for x in a.require.split(",") if x), note=a.note)
        except PublishRefused as e:
            print(f"refused: {e}", file=sys.stderr)
            return 2
        print(f"{a.channel} -> {ptr['release_id']} (was {ptr['previous']})")
    elif a.cmd == "status":
        for ch in CHANNEL_NAMES:
            ptr = read_channel(a.data, ch)
            print(f"{ch:11} {ptr['release_id'] if ptr else '-'}")
        for sv in store.builds():
            print(f"store {sv.product} {sv.vintage} {sv.build_hash[:12]} rows={sv.record['rows']} "
                  f"weights={sv.record['weights']}")
    elif a.cmd == "unused":
        for sv in store.unreferenced(a.data):
            print(f"{sv.product} {sv.vintage} {sv.build_hash[:12]} {sv.path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

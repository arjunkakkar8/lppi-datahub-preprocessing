from __future__ import annotations

import json
import os
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest

from ldh_preprocessing.release import (
    VintageStore,
    build_release,
    publish,
    read_channel,
    verify_release,
    write_persons_parquet,
)
from ldh_preprocessing.release.__main__ import main as cli
from ldh_preprocessing.release.publish import PublishRefused
from ldh_preprocessing.release.write import ContractViolation

from .conftest import CONTRACT, persons_frame


# ---------- write ----------

def test_write_sorts_marks_required_and_tags(tmp_path):
    p = write_persons_parquet(persons_frame(), tmp_path / "p.parquet", contract_version=CONTRACT,
                              product="acs5", vintage=2024, period=(2020, 2024))
    schema = pq.read_schema(p)
    assert not schema.field("perwt").nullable and not schema.field("cd119_id").nullable
    assert schema.field("married").nullable
    assert schema.metadata[b"contract_version"] == b"1.0.0"
    assert schema.metadata[b"period"] == b"2020-2024"
    df = pl.read_parquet(p)
    assert df.select(["statefip", "county_id", "serial", "pernum"]).equals(
        df.select(["statefip", "county_id", "serial", "pernum"]).sort(["statefip", "county_id", "serial", "pernum"]))
    md = pq.ParquetFile(p).metadata
    assert md.row_group(0).column(0).compression == "ZSTD"
    assert md.row_group(0).column(0).statistics is not None


def test_write_rejects_null_in_required(tmp_path):
    df = persons_frame().with_columns(pl.when(pl.col("pernum") == 1).then(None).otherwise(pl.col("perwt")).alias("perwt"))
    with pytest.raises(ContractViolation, match="perwt"):
        write_persons_parquet(df, tmp_path / "p.parquet", contract_version=CONTRACT,
                              product="acs5", vintage=2024, period=(2020, 2024))


# ---------- store ----------

def test_store_put_is_idempotent_and_content_addressed(make_vintage, data_root):
    a = make_vintage(seed=1)
    b = make_vintage(seed=1)
    c = make_vintage(seed=2)
    assert a.build_hash == b.build_hash and a.path == b.path
    assert c.build_hash != a.build_hash
    assert a.path.parent.name == "acs5-2020-2024"
    assert len(VintageStore(data_root).builds("acs5", 2024)) == 2
    assert not os.access(a.file("persons.parquet"), os.W_OK) or os.geteuid() == 0
    assert oct(a.file("persons.parquet").stat().st_mode)[-3:] == "444"


def test_store_find_and_inputs(make_vintage, data_root):
    a = make_vintage(seed=1)
    b = make_vintage(seed=2)
    store = VintageStore(data_root)
    assert store.find("acs5", 2024).build_hash == b.build_hash  # latest
    assert store.find("acs5", 2024, a.build_hash[:10]).build_hash == a.build_hash
    assert store.find("acs5", 2024, "inputs:" + a.record["inputs_key"]).build_hash == a.build_hash
    assert store.has_inputs("acs5", 2024, a.record["inputs"]).build_hash == a.build_hash
    assert store.has_inputs("acs5", 2024, {"other": 1}) is None
    with pytest.raises(LookupError):
        store.find("acs5", 2019)


def test_store_rejects_bad_periods(make_vintage):
    with pytest.raises(ValueError, match="five years"):
        make_vintage(product="acs5", period=(2021, 2024))
    with pytest.raises(ValueError, match="single year"):
        make_vintage(product="acs1", period=(2023, 2024))


# ---------- assemble ----------

def test_build_release_layout_and_manifest(make_vintage, spec, data_root):
    v24 = make_vintage(seed=1)
    make_vintage(period=(2015, 2019), seed=2)
    make_vintage(product="acs1", period=(2024, 2024), seed=3)
    make_vintage(product="acs1", period=(2020, 2020), seed=4, weights="experimental")
    rel = build_release(spec([("acs5", 2024), ("acs5", 2019), ("acs1", 2024), ("acs1", 2020)]), data_root)
    m = json.loads((rel / "manifest.json").read_text())
    assert m["release_id"] == rel.name and rel.name.startswith("ldh-2026.r1-")
    assert [(v["product"], v["vintage"]) for v in m["vintages"]] == [
        ("acs1", 2024), ("acs1", 2020), ("acs5", 2024), ("acs5", 2019)]
    acs1_2020 = next(v for v in m["vintages"] if v["vintage"] == 2020)
    assert acs1_2020["weights"] == "experimental"
    v = next(v for v in m["vintages"] if (v["product"], v["vintage"]) == ("acs5", 2024))
    assert v["period"] == [2020, 2024] and v["store_build"] == v24.build_hash
    assert {c["name"] for c in v["columns"]} >= {"perwt", "cd119_id", "married"}
    for f in ("product=acs5/vintage=2024/persons.parquet", "product=acs5/vintage=2019/geographies.parquet",
              "product=acs1/rules.json", "product=acs5/rules.json", "variables.json", "values.json",
              "voting.parquet"):
        assert (rel / f).is_file(), f
    # Hard-linked from the store, not copied.
    assert (rel / "product=acs5/vintage=2024/persons.parquet").stat().st_ino == v24.file("persons.parquet").stat().st_ino


def test_release_id_is_deterministic_and_content_sensitive(make_vintage, spec, data_root, shared_files):
    make_vintage(seed=1)
    s = spec([("acs5", 2024)], products=("acs5",))
    a = build_release(s, data_root)
    b = build_release(s, data_root)
    assert a == b
    (shared_files / "rules-acs5.json").write_text(json.dumps({"validity": {"min_n": 50}}))
    c = build_release(s, data_root)
    assert c != a and c.name.startswith("ldh-2026.r1-")


def test_new_release_reuses_unchanged_vintages(make_vintage, spec, data_root):
    """The 5-year series shifts yearly; unchanged builds must be shared byte for byte."""
    make_vintage(period=(2015, 2019), seed=2)
    make_vintage(seed=1)
    r1 = build_release(spec([("acs5", 2024), ("acs5", 2019)], products=("acs5",)), data_root)
    make_vintage(product="acs1", period=(2024, 2024), seed=3)
    r2 = build_release(spec([("acs5", 2024), ("acs5", 2019), ("acs1", 2024)]), data_root)
    f = "product=acs5/vintage=2019/persons.parquet"
    assert (r1 / f).stat().st_ino == (r2 / f).stat().st_ino
    m1, m2 = (json.loads((r / "manifest.json").read_text()) for r in (r1, r2))
    s1 = {(v["product"], v["vintage"]): v["store_build"] for v in m1["vintages"]}
    s2 = {(v["product"], v["vintage"]): v["store_build"] for v in m2["vintages"]}
    assert s1[("acs5", 2019)] == s2[("acs5", 2019)]


def test_build_release_rejects_bad_specs(make_vintage, spec, data_root):
    make_vintage(seed=1)
    with pytest.raises(ValueError, match="twice"):
        build_release(spec([("acs5", 2024), ("acs5", 2024)]), data_root)
    with pytest.raises(ValueError, match="rules"):
        build_release(spec([("acs5", 2024)], products=("acs1",)), data_root)
    with pytest.raises(LookupError):
        build_release(spec([("acs1", 2023)]), data_root)


# ---------- verify ----------

def _writable_copy(rel: Path, tmp_path: Path) -> Path:
    """Tamper tests work on a copy so the shared store files stay intact."""
    import shutil
    dst = tmp_path / "tamper" / "releases" / rel.name
    shutil.copytree(rel, dst, copy_function=shutil.copyfile)
    for p in dst.rglob("*"):
        if p.is_file():
            p.chmod(0o644)
    return dst


def test_verify_passes_and_writes_check(make_vintage, spec, data_root):
    make_vintage(seed=1)
    rel = build_release(spec([("acs5", 2024)], products=("acs5",)), data_root)
    rep = verify_release(rel)
    assert rep.ok, rep.errors
    check = json.loads((data_root / "checks" / rel.name / "verify.json").read_text())
    assert check["status"] == "passed" and check["checked_files"] == 6


def test_verify_catches_tampered_bytes(make_vintage, spec, data_root, tmp_path):
    make_vintage(seed=1)
    rel = _writable_copy(build_release(spec([("acs5", 2024)], products=("acs5",)), data_root), tmp_path)
    with open(rel / "values.json", "a") as f:
        f.write(" ")
    rep = verify_release(rel, write_report=False)
    assert any("values.json" in e and "bytes" in e for e in rep.errors)
    assert any("values.json" in e and "sha256" in e for e in rep.errors)


def test_verify_catches_schema_drift(make_vintage, spec, data_root, tmp_path):
    make_vintage(seed=1)
    rel = _writable_copy(build_release(spec([("acs5", 2024)], products=("acs5",)), data_root), tmp_path)
    m = json.loads((rel / "manifest.json").read_text())
    m["vintages"][0]["columns"][-1]["arrow_type"] = "uint16"
    (rel / "manifest.json").write_text(json.dumps(m))
    rep = verify_release(rel, write_report=False)
    assert any("content_hash" in e for e in rep.errors)
    assert any("column married" in e for e in rep.errors)


def test_verify_checks_codebook_availability(make_vintage, spec, data_root, shared_files):
    make_vintage(seed=1)
    v = json.loads((shared_files / "variables.json").read_text())
    v["variables"]["internet"] = {"availability": {"acs5": [2024]}}
    (shared_files / "variables.json").write_text(json.dumps(v))
    rel = build_release(spec([("acs5", 2024)], products=("acs5",)), data_root)
    rep = verify_release(rel, write_report=False)
    assert any("internet is listed for acs5 2024" in e for e in rep.errors)


def test_verify_requires_experimental_2020(make_vintage, spec, data_root):
    make_vintage(product="acs1", period=(2020, 2020), seed=4, weights="standard")
    rel = build_release(spec([("acs1", 2020)], products=("acs1",)), data_root)
    rep = verify_release(rel, write_report=False)
    assert any("experimental" in e for e in rep.errors)


# ---------- publish ----------

def test_publish_requires_passing_check(make_vintage, spec, data_root):
    make_vintage(seed=1)
    rel = build_release(spec([("acs5", 2024)], products=("acs5",)), data_root)
    with pytest.raises(PublishRefused, match="no verify check"):
        publish(data_root, rel.name)
    verify_release(rel)
    ptr = publish(data_root, rel.name, "candidate")
    assert ptr["previous"] is None and read_channel(data_root, "candidate")["release_id"] == rel.name
    with pytest.raises(PublishRefused, match="no parity check"):
        publish(data_root, rel.name, "production", require=("verify", "parity"))


def test_publish_refuses_failed_check(make_vintage, spec, data_root):
    make_vintage(product="acs1", period=(2020, 2020), seed=4, weights="standard")
    rel = build_release(spec([("acs1", 2020)], products=("acs1",)), data_root)
    verify_release(rel)
    with pytest.raises(PublishRefused, match="failed"):
        publish(data_root, rel.name)


def test_publish_moves_pointer_and_rolls_back(make_vintage, spec, data_root):
    make_vintage(seed=1)
    r1 = build_release(spec([("acs5", 2024)], products=("acs5",)), data_root)
    make_vintage(period=(2015, 2019), seed=2)
    r2 = build_release(spec([("acs5", 2024), ("acs5", 2019)], products=("acs5",)), data_root)
    for r in (r1, r2):
        verify_release(r)
    publish(data_root, r1.name, "production")
    ptr = publish(data_root, r2.name, "production")
    assert ptr["previous"] == r1.name
    back = publish(data_root, ptr["previous"], "production", note="rollback")
    assert back["release_id"] == r1.name and back["previous"] == r2.name
    hist = (data_root / "channels" / "history.jsonl").read_text().splitlines()
    assert [json.loads(h)["release_id"] for h in hist] == [r1.name, r2.name, r1.name]
    # Publishing the release already live is a no-op.
    publish(data_root, r1.name, "production")
    assert len((data_root / "channels" / "history.jsonl").read_text().splitlines()) == 3
    assert not list((data_root / "channels").glob(".*.tmp"))


def test_unreferenced_builds(make_vintage, spec, data_root):
    make_vintage(seed=1)
    build_release(spec([("acs5", 2024)], products=("acs5",)), data_root)
    old = make_vintage(period=(2015, 2019), seed=9)
    assert [b.build_hash for b in VintageStore(data_root).unreferenced(data_root)] == [old.build_hash]


# ---------- CLI ----------

def test_cli_end_to_end(tmp_path, shared_files, capsys):
    data = tmp_path / "cli-data"
    work = tmp_path / "w"
    persons = write_persons_parquet(persons_frame(seed=5), work / "persons.parquet", contract_version=CONTRACT,
                                    product="acs5", vintage=2024, period=(2020, 2024))
    (work / "inputs.json").write_text(json.dumps({"ipums_extract": "usa_00200"}))
    assert cli(["--data", str(data), "put", "acs5", "2020-2024", "--persons", str(persons),
                "--inputs", str(work / "inputs.json")]) == 0
    spec = {
        "label": "ldh-2026.r1", "contract_version": CONTRACT,
        "vintages": [{"product": "acs5", "vintage": 2024}],
        "codebook": {"variables.json": str(shared_files / "variables.json"),
                     "values.json": str(shared_files / "values.json")},
        "rules": {"acs5": str(shared_files / "rules-acs5.json")},
    }
    (work / "release.json").write_text(json.dumps(spec))
    capsys.readouterr()
    assert cli(["--data", str(data), "build", str(work / "release.json")]) == 0
    rid = capsys.readouterr().out.strip()
    assert cli(["--data", str(data), "publish", rid]) == 2  # not verified yet
    assert cli(["--data", str(data), "verify", rid]) == 0
    assert cli(["--data", str(data), "publish", rid, "--channel", "preview"]) == 0
    assert cli(["--data", str(data), "status"]) == 0
    assert rid in capsys.readouterr().out

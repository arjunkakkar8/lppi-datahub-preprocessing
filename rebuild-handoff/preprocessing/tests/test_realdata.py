"""Full-size run on a real 5-year file. Skipped unless LDH_TEST_PARQUET points at one.

    LDH_TEST_PARQUET=/path/to/acs.parquet pytest -m realdata -s
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import polars as pl
import pytest

from ldh_preprocessing.release import (
    VintageStore, build_release, publish, verify_release, write_persons_parquet,
)

from .conftest import CONTRACT

SRC = os.environ.get("LDH_TEST_PARQUET")
pytestmark = [pytest.mark.realdata, pytest.mark.skipif(not SRC, reason="LDH_TEST_PARQUET not set")]


def test_full_size_release(tmp_path, data_root, spec):
    t = {}
    t0 = time.perf_counter()
    df = pl.read_parquet(SRC)
    # processed_2022 uses -1 for missing; the contract uses real nulls.
    df = df.with_columns(
        [pl.when(pl.col(c) == -1).then(None).otherwise(pl.col(c)).alias(c)
         for c, d in df.schema.items() if d.is_integer() and c not in ("perwt", "hhwt")]
    )
    t["read"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    persons = write_persons_parquet(df, tmp_path / "w" / "persons.parquet", contract_version=CONTRACT,
                                    product="acs5", vintage=2022, period=(2018, 2022))
    t["write"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    sv = VintageStore(data_root).put(product="acs5", period=(2018, 2022),
                                     files={"persons.parquet": persons},
                                     inputs={"source": Path(SRC).name})
    t["store_put"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    rel = build_release(spec([("acs5", 2022)], products=("acs5",)), data_root)
    t["build"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    rep = verify_release(rel)
    t["verify"] = time.perf_counter() - t0
    assert rep.ok, rep.errors

    publish(data_root, rel.name, "candidate")
    size = persons.stat().st_size
    print(json.dumps({"rows": sv.record["rows"], "mb": round(size / 1e6, 1),
                      "seconds": {k: round(v, 1) for k, v in t.items()}}, indent=1))

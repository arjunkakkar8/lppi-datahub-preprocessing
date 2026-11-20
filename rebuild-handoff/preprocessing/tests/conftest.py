from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from ldh_preprocessing.release import VintageStore, write_persons_parquet

CONTRACT = "1.0.0"


def persons_frame(n: int = 1000, seed: int = 0, extra: bool = False) -> pl.DataFrame:
    import random

    r = random.Random(seed)
    df = pl.DataFrame(
        {
            "serial": [i // 3 + 1 for i in range(n)],
            "pernum": [i % 3 + 1 for i in range(n)],
            "perwt": [r.randint(1, 300) for _ in range(n)],
            "hhwt": [r.randint(1, 300) for _ in range(n)],
            "statefip": [r.choice([6, 12, 48, 50]) for _ in range(n)],
            "county_id": [r.randint(1, 40) for _ in range(n)],
            "cd119_id": [r.randint(100, 5000) for _ in range(n)],
            "sex": [r.choice([1, 2]) for _ in range(n)],
            "married": [r.choice([1, 2, 3, None]) for _ in range(n)],
        },
        schema={
            "serial": pl.Int32, "pernum": pl.UInt8, "perwt": pl.Int32, "hhwt": pl.Int32,
            "statefip": pl.UInt8, "county_id": pl.UInt32, "cd119_id": pl.UInt16,
            "sex": pl.UInt8, "married": pl.UInt8,
        },
    )
    if extra:
        df = df.with_columns(pl.lit(1, dtype=pl.UInt8).alias("internet"))
    return df


@pytest.fixture
def data_root(tmp_path: Path) -> Path:
    return tmp_path / "ldh-data"


@pytest.fixture
def make_vintage(tmp_path: Path, data_root: Path):
    store = VintageStore(data_root)

    def _make(product="acs5", period=(2020, 2024), seed=0, weights="standard", extra=False, inputs=None):
        work = tmp_path / f"build-{product}-{period[1]}-{seed}-{extra}"
        persons = write_persons_parquet(
            persons_frame(seed=seed, extra=extra), work / "persons.parquet",
            contract_version=CONTRACT, product=product, vintage=period[1], period=period,
        )
        geo = work / "geographies.parquet"
        pl.DataFrame({"geo_type": ["state"], "geo_id": [6], "available": [True]}).write_parquet(geo)
        return store.put(
            product=product, period=period,
            files={"persons.parquet": persons, "geographies.parquet": geo},
            inputs=inputs or {"ipums_extract": f"usa_{seed:05d}", "pipeline_commit": "abc123", "seed": seed},
            weights=weights, puma_vintage="puma2020",
        )

    return _make


@pytest.fixture
def shared_files(tmp_path: Path):
    d = tmp_path / "shared"
    d.mkdir()
    variables = {
        "variables": {
            "sex": {"availability": {"acs5": [2024, 2019], "acs1": [2024, 2020]}},
            "married": {"availability": {"acs5": [2024, 2019], "acs1": [2024, 2020]}},
            "voted": {},
        }
    }
    (d / "variables.json").write_text(json.dumps(variables))
    (d / "values.json").write_text(json.dumps({"sex": {"1": {"en": "Male", "es": "Hombre"}}}))
    for p in ("acs1", "acs5"):
        (d / f"rules-{p}.json").write_text(json.dumps({
            "validity": {"min_n": 100, "source": "inferred", "overrides": {}},
            "rounding": [{"min_abs": 1000, "to": 1000}, {"min_abs": 0, "to": 100}],
        }))
    pl.DataFrame({"statefip": [6, 0], "voted": [0.5, 0.6]}).write_parquet(d / "voting.parquet")
    return d


@pytest.fixture
def spec(shared_files: Path):
    from ldh_preprocessing.release import ReleaseSpec, VintageRef

    def _spec(vintages, label="ldh-2026.r1", products=("acs1", "acs5")):
        return ReleaseSpec(
            label=label,
            contract_version=CONTRACT,
            vintages=[VintageRef(*v) for v in vintages],
            codebook={n: shared_files / n for n in ("variables.json", "values.json")},
            rules={p: shared_files / f"rules-{p}.json" for p in products},
            tables={"voting": shared_files / "voting.parquet"},
            pipeline_commit="abc123",
        )

    return _spec

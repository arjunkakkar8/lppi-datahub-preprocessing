"""Encode-and-write stage: persons.parquet with the contract's Parquet settings."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

SORT_KEYS = ("statefip", "county_id", "serial", "pernum")
ROW_GROUP_ROWS = 500_000
ZSTD_LEVEL = 9
# Columns the contract declares non-null on every row. county_id is not one of them:
# in processed_2022, 4.1M records (72.5M weighted, 43 states) sit in PUMAs that belong to
# no published county group, so it is null there.
REQUIRED = ("serial", "pernum", "perwt", "hhwt", "statefip", "cd119_id")


class ContractViolation(ValueError):
    pass


def write_persons_parquet(
    df: pl.DataFrame,
    path: Path,
    *,
    contract_version: str,
    product: str,
    vintage: int,
    period: tuple[int, int],
    required: Iterable[str] = REQUIRED,
    row_group_rows: int = ROW_GROUP_ROWS,
) -> Path:
    """Sort, mark required columns non-null, and write zstd Parquet with key-value metadata.

    The file carries contract_version, product, vintage and period, but not release_id:
    store files are reused byte for byte across releases, so the release lives in the manifest.
    """
    required = [c for c in required if c in df.columns]
    nulls = {c: n for c in required if (n := df.get_column(c).null_count())}
    if nulls:
        raise ContractViolation(f"required columns contain nulls: {nulls}")

    sort_by = [c for c in SORT_KEYS if c in df.columns]
    if sort_by:
        df = df.sort(sort_by)

    table = df.to_arrow()
    fields = [
        pa.field(f.name, f.type, nullable=f.name not in required) for f in table.schema
    ]
    metadata = {
        "contract_version": contract_version,
        "product": product,
        "vintage": str(vintage),
        "period": f"{period[0]}-{period[1]}",
    }
    table = table.cast(pa.schema(fields, metadata=metadata))

    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        table,
        path,
        compression="zstd",
        compression_level=ZSTD_LEVEL,
        row_group_size=row_group_rows,
        use_dictionary=True,
        write_statistics=True,
        version="2.6",
    )
    return path

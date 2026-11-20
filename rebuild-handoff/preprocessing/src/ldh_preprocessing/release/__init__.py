"""Release builder: vintage store, release assembly, verification and publish pointer.

Layout under one data root (``ldh-data/``)::

    store/<product>-<start>-<end>/<build_hash>/   # each vintage built once, read-only
        persons.parquet
        geographies.parquet
        vintage.json                              # build record
    releases/<release_id>/                         # immutable, files linked from the store
        manifest.json
        variables.json, values.json
        product=acs5/rules.json
        product=acs5/vintage=2024/persons.parquet
        product=acs5/vintage=2024/geographies.parquet
        voting.parquet, ...
    checks/<release_id>/verify.json                # results written after the fact
    channels/<channel>.json                        # publish pointer, switched atomically
    channels/history.jsonl                         # every pointer move
"""

from .assemble import ReleaseSpec, VintageRef, build_release
from .publish import publish, read_channel
from .store import StoredVintage, VintageStore
from .verify import verify_release
from .write import write_persons_parquet

__all__ = [
    "ReleaseSpec",
    "StoredVintage",
    "VintageRef",
    "VintageStore",
    "build_release",
    "publish",
    "read_channel",
    "verify_release",
    "write_persons_parquet",
]

# pipelines/preprocessing

Turns an IPUMS extract into a verified LDH data release. This folder holds the release
builder (the last stages of the pipeline). Extract, derive and validate stages come next.

## Release builder

```
ldh-data/
  store/<product>-<period>/<build_hash>/     each vintage built once, read-only
  releases/<release_id>/                     immutable; files hard-linked from the store
  checks/<release_id>/verify.json            written by verify (parity.json by the parity harness)
  channels/{candidate,preview,production}.json   publish pointers, switched atomically
  channels/history.jsonl                     every pointer move
```

```sh
uv run ldh-release --data ldh-data put acs5 2020-2024 --persons out/persons.parquet \
    --geographies out/geographies.parquet --inputs out/inputs.json
uv run ldh-release --data ldh-data build release.json        # prints the release_id
uv run ldh-release --data ldh-data verify ldh-2026.r1-1a2b3c4d
uv run ldh-release --data ldh-data publish ldh-2026.r1-1a2b3c4d --channel candidate
uv run ldh-release --data ldh-data publish ldh-2026.r1-1a2b3c4d --channel production --require verify,parity
uv run ldh-release --data ldh-data status
```

- **release_id** is `<label>-<8 hex of the content hash>`. The hash covers the contract
  version, every vintage entry (store build, files, columns) and every shared file, and not
  the build time. The same inputs always give the same id, and any change gives a new one.
- **Store builds** are named by the hash of their files. `inputs.json` (extract id, DOI,
  pipeline commit, contract version, registry hash) is hashed into `inputs_key`, so the
  pipeline can call `has_inputs` and skip a vintage that is already built.
- **verify** checks the manifest hash, each file's bytes, sha256 and row count, each
  vintage's Parquet columns (name, Arrow type, nullable) and key-value metadata, rules per
  product, the 2020 1-year experimental flag, and that every variable the codebook lists for
  a vintage is a column of it.
- **publish** refuses unless each required check passed against the manifest as it is now.
  Rolling back is publishing the previous release again.
- Nothing deletes. `ldh-release unused` lists store builds no release serves.

The engine's image build, Vercel deploy, promote and rollback start from a channel pointer
and live in services/engine.

## Tests

All tests run locally (no CI minutes).

```sh
uv run pytest                                   # unit tests, under a second
LDH_TEST_PARQUET=/path/to/acs.parquet uv run pytest -m realdata -s   # full-size run
```

The full-size run on processed_2022 (15.7M rows) writes the 279 MB persons file in 21 s and
puts, builds and verifies it in under a second.

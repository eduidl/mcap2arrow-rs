# transmcap-core scripts

This directory is managed as a small `uv` project.

## Prerequisite

Install `uv`:

```bash
# https://docs.astral.sh/uv/
uv --version
```

## Generate fixtures

```bash
cd crates/transmcap-core/scripts
uv run gen_fixtures.py
```

`gen_fixtures.py` writes fixtures into `../tests/fixtures/`.

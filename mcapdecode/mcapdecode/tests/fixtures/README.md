# MCAP Test Fixtures

These fixtures are used by `transmcap-core` tests.

- `with_summary.mcap`: includes summary records and summary offsets.
- `no_summary.mcap`: same logical messages as `with_summary.mcap`, but written without summary records and summary offsets.

Message contents in both files:

- Channel `/decoded`
  - schema name: `test.Msg`
  - schema encoding: `jsonschema`
  - message encoding: `json`
  - messages:
    - `{\"x\":1,\"nested\":{\"y\":\"a\"}}`
    - `{\"x\":2,\"nested\":{\"y\":\"b\"}}`
- Channel `/raw`
  - no schema
  - message encoding: `application/octet-stream`
  - message bytes: `01 02 03`

## Regenerating

Use the `uv` project in `crates/transmcap-core/scripts`:

```bash
cd crates/transmcap-core/scripts
uv run gen_fixtures.py
```

#!/usr/bin/env python3
"""Generate MCAP fixtures for transmcap-core tests.

Run with uv from this directory:
    uv run gen_fixtures.py
"""

from __future__ import annotations

from pathlib import Path

from mcap.writer import IndexType, Writer

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"


def write_fixture(path: Path, include_summary: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        if include_summary:
            writer = Writer(f)
        else:
            writer = Writer(
                f,
                index_types=IndexType.NONE,
                repeat_channels=False,
                repeat_schemas=False,
                use_statistics=False,
                use_summary_offsets=False,
            )
        writer.start(profile="", library="transmcap-test")

        schema_id = writer.register_schema(
            name="test.Msg",
            encoding="jsonschema",
            data=b'{"type":"object"}',
        )
        decoded_channel_id = writer.register_channel(
            topic="/decoded",
            message_encoding="json",
            schema_id=schema_id,
        )
        raw_channel_id = writer.register_channel(
            topic="/raw",
            message_encoding="application/octet-stream",
            schema_id=0,
        )

        writer.add_message(
            channel_id=decoded_channel_id,
            log_time=1,
            publish_time=1,
            data=b'{"x":1,"nested":{"y":"a"}}',
        )
        writer.add_message(
            channel_id=decoded_channel_id,
            log_time=2,
            publish_time=2,
            data=b'{"x":2,"nested":{"y":"b"}}',
        )
        writer.add_message(
            channel_id=raw_channel_id,
            log_time=3,
            publish_time=3,
            data=bytes([0x01, 0x02, 0x03]),
        )

        writer.finish()


def main() -> None:
    write_fixture(FIXTURES / "with_summary.mcap", include_summary=True)
    write_fixture(FIXTURES / "no_summary.mcap", include_summary=False)
    print(f"Generated fixtures under {FIXTURES}")


if __name__ == "__main__":
    main()

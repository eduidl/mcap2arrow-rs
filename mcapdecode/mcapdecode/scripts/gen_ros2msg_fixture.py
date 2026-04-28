#!/usr/bin/env python3
"""Generate a small ROS 2 .msg + CDR MCAP fixture.

Run with uv from this directory:
    uv run gen_ros2msg_fixture.py
"""

from __future__ import annotations

from pathlib import Path
import struct

from mcap.writer import Writer

REPO_ROOT = Path(__file__).resolve().parents[3]
OUT_PATH = REPO_ROOT / "data" / "ros2msg_test.mcap"


def align(buf: bytearray, n: int, align_base: int = 4) -> None:
    rel = len(buf) - align_base
    pad = (n - (rel % n)) % n
    if pad:
        buf.extend(b"\x00" * pad)


def encode_cdr_msg(x: int, name: str, value: float) -> bytes:
    # CDR encapsulation header: little-endian
    payload = bytearray(b"\x00\x01\x00\x00")

    align(payload, 4)
    payload.extend(struct.pack("<i", x))

    name_bytes = name.encode("utf-8") + b"\x00"
    align(payload, 4)
    payload.extend(struct.pack("<I", len(name_bytes)))
    payload.extend(name_bytes)

    align(payload, 8)
    payload.extend(struct.pack("<d", value))
    return bytes(payload)


def write_fixture(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        writer = Writer(f)
        writer.start(profile="ros2", library="transmcap-ros2msg-test")

        schema_text = b"int32 x\nstring name\nfloat64 value\n"
        schema_id = writer.register_schema(
            name="example_msgs/msg/Sample",
            encoding="ros2msg",
            data=schema_text,
        )
        channel_id = writer.register_channel(
            topic="/example/sample",
            message_encoding="cdr",
            schema_id=schema_id,
        )

        writer.add_message(
            channel_id=channel_id,
            log_time=1,
            publish_time=1,
            data=encode_cdr_msg(10, "alpha", 1.5),
        )
        writer.add_message(
            channel_id=channel_id,
            log_time=2,
            publish_time=2,
            data=encode_cdr_msg(20, "beta", 2.5),
        )

        writer.finish()


def main() -> None:
    write_fixture(OUT_PATH)
    print(f"Generated {OUT_PATH}")


if __name__ == "__main__":
    main()

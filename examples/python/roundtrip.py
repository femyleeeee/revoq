#!/usr/bin/env python3
import struct
import tempfile

import revoq


def main() -> int:
    msg_type = 100
    payloads = [struct.pack("<Qd", i, 100.0 + i) for i in range(3)]

    with tempfile.TemporaryDirectory(prefix="revoq-python-roundtrip-") as path:
        location = revoq.make_location(
            path,
            mode="live",
            category="executiondata",
            group="examples",
            name="python-roundtrip",
        )

        writer = revoq.JournalWriter(location, 0, prefault=False, background_threads=False)
        for i, payload in enumerate(payloads):
            writer.write(msg_type=msg_type, gen_time=i + 1, data=payload)
        del writer

        reader = revoq.JournalReader(prefault=False, background_threads=False)
        reader.join(location, 0, 0)
        frames = [frame for frame in reader if frame.msg_type == msg_type]

        assert len(frames) == len(payloads)
        for frame, expected in zip(frames, payloads):
            assert bytes(frame.data) == expected

    print(f"ok python_roundtrip messages={len(payloads)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

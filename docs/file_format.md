# File Format

Journal files are page files under a location directory. Each page contains:

- `PageHeader`: version, header length, page size, frame header length, and last-frame offset.
- Zero or more aligned frames.
- A `PageEnd` sentinel when the writer rotates to the next page.

Each frame contains:

- `length`: logical frame length, published last.
- `msg_type`: application message type.
- `flags`: frame flags.
- `sequence`: writer sequence number.
- `gen_time`: producer timestamp.
- `event_time`: optional application timestamp.
- payload bytes immediately after the header.

Physical frame stride is aligned to `FRAME_ALIGNMENT`.

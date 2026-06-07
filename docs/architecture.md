# Architecture

Revoq is a file-backed journal built around memory-mapped pages.

Core pieces:

- `JournalWriter`: appends frames to a destination journal.
- `JournalReader`: joins one or more journals and reads committed frames.
- `FrameHeader`: fixed-size metadata published with release semantics.
- `Page`: memory-mapped journal page with a fixed header and aligned frames.
- `PageWarmer`: optional background page preparation for tuned runs.

Writers publish a frame by filling payload bytes first and storing frame length last. Readers use acquire loads of the frame length before reading metadata and payload.

The benchmark exercises the application path from writer timestamp to reader handler endpoint using a direct static dispatch strategy.

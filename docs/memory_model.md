# Memory Model

Revoq uses a simple publication rule:

1. The writer reserves a frame slot.
2. The writer fills metadata and payload.
3. The writer publishes the frame length with `memory_order_release`.
4. The reader loads frame length with `memory_order_acquire`.
5. The reader reads metadata and payload only after the frame is committed.

This keeps the hot path compact while making producer-consumer visibility explicit.

The default writer policy is single-writer. A multi-writer policy is available for serialized writes.

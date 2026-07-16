# sema-storage architecture

A standalone central daemon is the prototype's sole durable write authority and fixture-scoped identifier allocator. Every request traverses state-bearing Kameo actors in Signal → Nexus → SEMA order. SEMA alone owns `sema_engine::Engine`; its versioned log is authoritative. Nexus fans committed changes to push subscribers.

## Revisable leans
- One closed typed record family stores document versions and allocator cursors. Split families only when different retention or schema evolution requires it.
- The Unix socket and `/tmp/new-language-engine` defaults isolate prototype state; production Spirit paths are never used.
- Fixture allocation is monotonic per explicit scope. This deliberately says nothing about permanent split/merge lineage or authority placement.

# sema-storage architecture

A standalone central daemon is the prototype's sole durable write authority and fixture-scoped identifier allocator. Every request traverses state-bearing Kameo actors in Signal → Nexus → SEMA order. SEMA alone owns `sema_engine::Engine`; its versioned log is authoritative. Nexus fans committed changes to push subscribers.

## Revisable leans
- One closed typed record family stores document versions and allocator cursors. Split families only when different retention or schema evolution requires it.
- The Unix socket and `/tmp/new-language-engine` defaults isolate prototype state; production Spirit paths are never used.
- Fixture allocation is monotonic per explicit scope. This deliberately says nothing about permanent split/merge lineage.
- **Central-Sema-daemon storage axis.** One central `sema-storage` daemon is the sole durable write authority; the schema, nomos, and logos daemons run as stateless socket clients that hold no durable state and persist only through it. The charter's fuller vision instead seats a `sema-engine` inside each component daemon (per-daemon storage). This lean keeps storage central for the prototype. Revise it when a component needs independent durable state, per-daemon retention or schema-evolution divergence, or write throughput a single central daemon cannot serve — at which point storage seating moves per-daemon.
- The *allocation-authority* seat is settled, not a lean: the psyche ruled the id-allocation authority central-in-Sema (2026-07-17, "yes, seat it centrally in sema"; bead `primary-56d1.11`). Only the *storage* seating in the entry above remains revisable; keep the two distinct.

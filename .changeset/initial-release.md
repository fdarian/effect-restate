---
"effect-restate": minor
---

Initial release. Effect-native bindings for Restate: define services, virtual objects and workflows with Effect and Schema, and use durable operations (`run`, `sleep`, state, awakeables, workflow promises) and typed errors that cross calls. Call handlers durably from inside a handler or through ingress clients from outside. `Restate.layer()` serves them to a real Restate server through the SDK, and `Restate.layerInMemory()` runs the same handlers in-process for tests, with no server, retries or replay.

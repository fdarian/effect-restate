# effect-restate

> Effect-native bindings for Restate, with an in-memory implementation for tests.

A single `Restate` tag with two constructors: one that serves your handlers to a real [Restate](https://restate.dev) server, and one that runs them entirely in-process. Define services, virtual objects and workflows once with Effect + Schema; swap the layer to run them under Vitest with no server.

## Install

```bash
bun add effect-restate @restatedev/restate-sdk @restatedev/restate-sdk-clients effect
```

`@restatedev/restate-sdk` and `@restatedev/restate-sdk-clients` are peer dependencies. They are only loaded at runtime when you use `Restate.layer`, so tests using `Restate.layerInMemory` never touch them.

## Quick start

Define a service and serve it:

```ts
import { Effect, Schema as S } from "effect"
import { Restate } from "effect-restate"

export const Greeter = Restate.service({
  name: "Greeter",
  handlers: {
    greet: Restate.handler(
      { input: S.String, output: S.String },
      (name) => Effect.succeed(`Hello ${name}`),
    ),
  },
})

const server = Effect.gen(function* () {
  const served = yield* Restate.serve([Greeter])
  yield* Effect.log(`serving on :${served.port}`)
  return yield* Effect.never
})

server.pipe(
  Effect.scoped,
  Effect.provide(Restate.layer()),
  Effect.runPromise,
)
```

Register the endpoint with your Restate server once (for example `restate deployments register http://localhost:9080`), then call it from anywhere with the ingress client:

```ts
const caller = Effect.gen(function* () {
  const greeting = yield* Restate.ingress.client(Greeter).greet("world")
  console.log(greeting) // "Hello world"
})

caller.pipe(Effect.provide(Restate.layer()), Effect.runPromise)
```

`Restate.layer()` reads its configuration from the environment via Effect `Config`:

| Variable                | Default                 | Used for                                |
| ----------------------- | ----------------------- | --------------------------------------- |
| `RESTATE_INGRESS_URL`   | `http://localhost:8080` | ingress clients                         |
| `RESTATE_ADMIN_URL`     | `http://localhost:9070` | `Restate.ingress.cancel`                |
| `RESTATE_IDENTITY_KEYS` | none                    | comma-separated keys to verify requests |
| `PORT`                  | `9080`                  | default port of `Restate.serve`         |

## Test without a Restate server

Swap `Restate.layer` for `Restate.layerInMemory` and the same handlers run in-process: no endpoint, no HTTP, no server.

```ts
import { it } from "@effect/vitest"
import { Effect } from "effect"
import { expect } from "vitest"
import { Restate } from "effect-restate"
import { Greeter } from "./greeter"

it.effect("greets", () =>
  Effect.gen(function* () {
    yield* Restate.serve([Greeter])

    const greeting = yield* Restate.ingress.client(Greeter).greet("world")
    expect(greeting).toBe("Hello world")
  }).pipe(Effect.provide(Restate.layerInMemory())),
)
```

`Restate.sleep` and delayed sends use Effect's clock under `layerInMemory`, so `TestClock.adjust` fast-forwards them. See [In-memory differences](#in-memory-differences) for what is not simulated.

## Guides

Read in order: define handlers, know what each kind can use, call them, handle errors, serve them, then the rules that keep them replay-safe.

### Defining services, objects and workflows

Restate has three kinds of component, each built from handlers. `Restate.handler` and `Restate.shared` take the same options, all optional:

```ts
Restate.handler(
  {
    input: S.Struct({ to: S.String }), // no input: the handler takes no argument
    output: S.String,                  // no output: `unknown`, passed through as JSON
    error: PaymentDeclined,            // see "Typed errors across calls"
    options: { /* Restate SDK handler options, live layer only */ },
  },
  (input) => Effect.succeed("ok"),
)
```

`input`, `output` and `error` are Schemas. Values cross the wire as JSON, so they are encoded on the way out and decoded on the way in.

| Component | Define with        | Handlers                                                                           |
| --------- | ------------------ | ---------------------------------------------------------------------------------- |
| Service   | `Restate.service`  | `Restate.handler` only. Stateless, any number run concurrently.                    |
| Object    | `Restate.object`   | `handler` is exclusive per key and can write state. `shared` runs concurrently and reads only. |
| Workflow  | `Restate.workflow` | A required `run` (a `handler`, runs once per key), plus `shared` handlers.         |

```ts
const count = Restate.state("count", S.Number)

export const Counter = Restate.object({
  name: "Counter",
  handlers: {
    add: Restate.handler({ input: S.Number, output: S.Number }, (amount) =>
      Effect.gen(function* () {
        const next = Option.getOrElse(yield* count.get, () => 0) + amount
        yield* count.set(next)
        return next
      }),
    ),
    get: Restate.shared({ output: S.Number }, () =>
      Effect.map(count.get, Option.getOrElse(() => 0)),
    ),
  },
})
```

```ts
const approval = Restate.promise("approval", S.Boolean)

export const Signup = Restate.workflow({
  name: "Signup",
  handlers: {
    run: Restate.handler({ input: S.String, output: S.String }, (email) =>
      Effect.gen(function* () {
        const approved = yield* approval.await
        return approved ? `${email} approved` : `${email} rejected`
      }),
    ),
    approve: Restate.shared({ input: S.Boolean }, (decision) =>
      approval.resolve(decision),
    ),
  },
})
```

A workflow's `run` must exist and must not be `shared`; its other handlers must be `shared`. The names `submit`, `attach`, `output`, `workflowSubmit`, `workflowAttach` and `workflowOutput` are reserved. Invalid definitions throw when you call `Restate.service`/`object`/`workflow`.

### What each handler can use

The runtime provides a different set of contexts depending on the handler's kind. The operations you use determine what the handler requires, and `Restate.serve` checks that the runtime provides it.

| Handler                           | Provides                                                       |
| --------------------------------- | -------------------------------------------------------------- |
| Service handler                   | `RestateContext`                                               |
| Object `handler`                  | `RestateContext`, `SharedObjectContext`, `ObjectContext`       |
| Object `shared`                   | `RestateContext`, `SharedObjectContext`                        |
| Workflow `run`                    | all four: adds `WorkflowContext`                               |
| Workflow `shared`                 | `RestateContext`, `SharedObjectContext`, `WorkflowContext`     |

| Context               | Operations                                                                                                  |
| --------------------- | ----------------------------------------------------------------------------------------------------------- |
| `RestateContext`      | `run`, `sleep`, `now`, `random`, `uuid`, `invocationId`, `awakeable`, `resolveAwakeable`, `rejectAwakeable`, `cancel`, `client`, `sendClient`, `workflowClient` |
| `SharedObjectContext` | `state(..).get`, `stateKeys`, `objectKey`                                                                   |
| `ObjectContext`       | `state(..).set`, `state(..).clear`, `clearAllState`                                                         |
| `WorkflowContext`     | `promise(..)`: `await`, `peek`, `resolve`, `reject`                                                         |

The handler itself type-checks, but using something its kind does not provide leaves an unsatisfied requirement on the effect returned by `Restate.serve`. It becomes a type error where you run that effect:

```ts
const Broken = Restate.object({
  name: "Broken",
  handlers: {
    reset: Restate.shared({}, () => count.set(0)), // shared handlers cannot write state
  },
})

Restate.serve([Broken]).pipe(
  Effect.scoped,
  Effect.provide(Restate.layer()),
  Effect.runPromise, // type error: ObjectContext is not provided
)
```

Requirements beyond the Restate contexts (your own services) are provided to `Restate.serve`, see [Serving](#serving).

### Durable operations

Everything below is journaled by Restate: when an invocation is retried or resumed, completed steps return their recorded results instead of running again.

#### `Restate.run`

Wraps a side effect so it runs once and its result is journaled.

```ts
class PaymentDeclined extends S.TaggedError<PaymentDeclined>()("PaymentDeclined", {
  reason: S.String,
}) {}

declare const chargeCard: (
  amount: number,
) => Effect.Effect<{ id: string }, PaymentDeclined>

const Checkout = Restate.service({
  name: "Checkout",
  handlers: {
    pay: Restate.handler({ input: S.Number, output: S.String }, (amount) =>
      Effect.gen(function* () {
        const receipt = yield* Restate.run("charge", chargeCard(amount), {
          output: S.Struct({ id: S.String }),
          error: PaymentDeclined,
          retry: { maxAttempts: 5, initialInterval: "200 millis" },
        })
        return receipt.id
      }).pipe(Effect.catchTag("PaymentDeclined", () => Effect.succeed("declined"))),
    ),
  },
})
```

- **Typed failures are terminal and journaled.** They are never retried and replay returns the same failure. Because the failure has to be rebuilt on replay, an effect with a typed error requires the `error` schema; an effect that cannot fail typed does not.
- **Defects and interruptions are retried**, following `retry` (or the server's default policy). Once retries run out, the invocation ends with a `TerminalFailure`.
- `output` is the Schema for the result; without it the value goes through as plain JSON.
- Other Effect services stay available inside `run`. Restate contexts do not, see [Rules](#rules).

#### Sleep, time and randomness

```ts
yield* Restate.sleep("30 minutes")        // durable timer, survives restarts
const startedAt = yield* Restate.now      // journaled Date.now()
const roll = yield* Restate.random        // journaled random number in [0, 1)
const id = yield* Restate.uuid
const invocation = yield* Restate.invocationId
```

#### State

Object state is typed by a Schema and keyed by name. `get` returns an `Option`; `set` and `clear` need an exclusive handler.

```ts
const profile = Restate.state("profile", S.Struct({ name: S.String }))

yield* profile.set({ name: "Ada" })
const stored = yield* profile.get // Option<{ name: string }>
yield* profile.clear
```

`Restate.objectKey` is the key of the current object (or workflow), `Restate.stateKeys` lists the stored names, and `Restate.clearAllState` wipes them. A stored value that no longer decodes with its Schema is a defect.

#### Awakeables

An awakeable is a promise that something outside the handler completes: a webhook, a person, another service.

```ts
const Review = Restate.service({
  name: "Review",
  handlers: {
    request: Restate.handler({ input: S.String, output: S.String }, (document) =>
      Effect.gen(function* () {
        const verdict = yield* Restate.awakeable(S.String)
        yield* Restate.run("notify", notifyReviewer(document, verdict.id))
        return yield* verdict.await
      }),
    ),
  },
})

declare const notifyReviewer: (document: string, awakeableId: string) => Effect.Effect<void>
```

Complete it with `Restate.ingress.resolveAwakeable(id, S.String, "approved")` from outside, or `Restate.resolveAwakeable` from another handler. `Restate.rejectAwakeable(id, reason)` (and its ingress twin) makes `verdict.await` fail with a `TerminalFailure` carrying `reason`.

#### Workflow promises

`Restate.promise(name, schema)` is a durable promise scoped to one workflow instance, completed by a `shared` handler (see `Signup` above).

```ts
const decision = Restate.promise("approval", S.Boolean)

yield* decision.await   // waits; fails with TerminalFailure if rejected
yield* decision.peek    // Option: none while pending
yield* decision.resolve(true)
yield* decision.reject("withdrawn")
```

`resolve` and `reject` fail with a `TerminalFailure` of code 409 if the promise was already completed (the first value stays). `await` and `peek` fail with a `TerminalFailure` carrying the reason if it was rejected. These are typed failures, so a handler can recover from them:

```ts
decision.resolve(true).pipe(
  Effect.catchTag("TerminalFailure", (failure) =>
    failure.code === 409 ? Effect.void : Effect.fail(failure),
  ),
)
```

### Calling handlers

Two sets of clients, depending on where you are.

**Inside a handler**: durable clients, journaled with the invocation. They need `RestateContext`.

```ts
const Relay = Restate.service({
  name: "Relay",
  handlers: {
    greetAndCount: Restate.handler({ input: S.String, output: S.Number }, (name) =>
      Effect.gen(function* () {
        const greeting = yield* Restate.client(Greeter).greet(name)
        yield* Restate.sendClient(Greeter).greet(name, { delay: "1 hour" })
        return yield* Restate.client(Counter, name).add(greeting.length)
      }),
    ),
  },
})
```

- `Restate.client(def, key?)` calls and waits. Objects take their key as the second argument.
- `Restate.sendClient(def, key?)` sends one-way and returns the invocation id. Takes `{ delay, idempotencyKey }`.
- `Restate.workflowClient(def, key)` has `run` (wait), `submit` (start) and a method per shared handler.
- Handlers without `input` take their options (`{ idempotencyKey }`) as the only argument.

**Outside a handler**: ingress clients, which talk to the Restate server. They require the `Restate` service and add `IngressError` to every call.

```ts
const outside = Effect.gen(function* () {
  const total = yield* Restate.ingress.client(Counter, "visits").add(1)

  const handle = yield* Restate.ingress
    .sendClient(Greeter)
    .greet("later", { delay: "10 seconds", idempotencyKey: "greet-1" })
  const greeting = yield* Restate.ingress.result(handle)

  const signup = Restate.ingress.workflowClient(Signup, "user-1")
  yield* signup.submit("ada@example.com")
  yield* signup.approve(true)
  const outcome = yield* signup.attach          // waits for `run`
  const maybe = yield* signup.output            // Option: none while still running

  return { total, greeting, outcome, maybe }
})
```

- A send returns a `SendHandle`. `Restate.ingress.result(handle)` awaits it, but only if the send had an `idempotencyKey`; otherwise it fails with `NotAttachable`. Workflow submits are always attachable.
- `Restate.ingress.resolveAwakeable`, `rejectAwakeable` and `cancel` complete awakeables and cancel invocations from outside.

### Typed errors across calls

By default a failing handler reaches its caller as a `TerminalFailure { message, code }`. Give the handler an `error` Schema and its caller gets your tagged error back in the error channel instead, on both durable and ingress clients:

```ts
class OutOfStock extends S.TaggedError<OutOfStock>()("OutOfStock", {
  sku: S.String,
}) {}

const Inventory = Restate.service({
  name: "Inventory",
  handlers: {
    reserve: Restate.handler(
      { input: S.String, output: S.Number, error: OutOfStock },
      (sku) => Effect.fail(new OutOfStock({ sku })),
    ),
  },
})

const reserve = Restate.ingress.client(Inventory)
  .reserve("sku-1")
  .pipe(Effect.catchTag("OutOfStock", (error) => Effect.succeed(error.sku)))
```

A handler can also fail with a `TerminalFailure` directly to choose the status code. Failures of any kind are terminal: Restate does not retry them. Only defects are retried.

<details>
<summary>How the error travels</summary>

The encoded error is carried in the `message` of a `TerminalFailure` (code 500) under an `effect-restate/error` marker, so it survives Restate's wire format. Callers that hold the handler definition decode it; anyone else, such as a non-Effect client, sees an ordinary failed invocation with that message.
</details>

### Serving

`Restate.serve(definitions, { port? })` starts an HTTP/2 endpoint for the definitions and stays up until the surrounding scope closes. It returns `{ port }`, which matters when you pass `port: 0` to pick a free one.

```ts
const main = Restate.serve([Greeter, Counter, Signup]).pipe(
  Effect.andThen(Effect.never),
  Effect.provide(Mailer.layer),
  Effect.scoped,
  Effect.provide(Restate.layer()),
)
```

- Whatever your handlers need beyond the Restate contexts (here a `Mailer`) is provided to `serve` and captured there.
- The library only opens the endpoint. Registering it with a Restate server (CLI, admin API, or your deployment tooling) is up to you.
- `ServeError` covers a port that cannot be bound or a definition that cannot be served.

### Cancellation

Cancelling an invocation interrupts the handler's fiber, so `Effect.ensuring` and `Effect.acquireRelease` finalizers run, and they may still use Restate operations to compensate. The caller sees a `TerminalFailure` with code 409.

```ts
const slow = Restate.service({
  name: "Slow",
  handlers: {
    wait: Restate.handler({}, () =>
      Restate.sleep("1 hour").pipe(
        Effect.ensuring(Effect.log("cleaning up")),
      ),
    ),
  },
})

const cancelIt = (invocationId: string) => Restate.ingress.cancel(invocationId)
```

The invocation id comes from `Restate.invocationId` inside the handler or `handle.invocationId` on a `SendHandle`. From inside a handler use `Restate.cancel(id)`. Cancelling an already finished invocation fails with `IngressError`.

### Rules

Restate re-runs handler code to resume an invocation, and the journal only lines up if the code does the same operations in the same order each time.

- **Do not run Restate operations concurrently.** Operations are journaled in the order they are issued, so two fibers issuing them (`Effect.all(..., { concurrency })`, `Effect.forkChild`) make that order unpredictable. Run them one after another. Concurrency inside a single `Restate.run` closure is fine.
- **Use `Restate.sleep` and `Restate.now`, not Effect's clock.** `Effect.sleep` is not durable and `Date.now()` or `Clock.currentTimeMillis` return a different value on replay. Likewise use `Restate.random` and `Restate.uuid` instead of `Math.random()` and `crypto.randomUUID()`. Anything else non-deterministic belongs inside `Restate.run`.
- **No Restate context inside `Restate.run`.** The closure is not part of the journal. Using any Restate operation there (calls, state, `sleep`, a nested `run`) is a defect. Other services still work.
- **Typed failures are final, defects are retried.** Use a typed error for outcomes the caller should see, and let unexpected bugs die.

### In-memory differences

`Restate.layerInMemory` runs the same handlers and shares the same test suite as the live layer, with a few deliberate gaps:

- **No journaling or replay.** Handlers run once, in-process. `Restate.run` executes its effect directly.
- **No retries.** A defect inside a handler or a `Restate.run` closure surfaces to the caller as a die instead of being retried, and `retry` is ignored.
- **Handler `options` are ignored**, and there is no HTTP endpoint (`serve` reports port `0`) or deployment.
- **Ingress to an unknown service or handler** fails with `IngressError`, as the server's 404 does. A durable call to one from inside a handler is a defect.

## Errors

Tagged errors you can `Effect.catchTag` on:

| Error             | Raised by                                                                                              |
| ----------------- | ------------------------------------------------------------------------------------------------------ |
| `TerminalFailure` | any call or `attach` whose handler failed without an `error` schema, cancelled invocations (409), rejected awakeables and workflow promises, completing a workflow promise twice (409), `Restate.run` retries running out |
| `IngressError`    | every `Restate.ingress.*` operation: server unreachable, unknown service or handler, unknown invocation or awakeable |
| `NotAttachable`   | `Restate.ingress.result` on a send without an `idempotencyKey`                                         |
| `ServeError`      | `Restate.serve`                                                                                        |

`TerminalFailure` has `message` and `code`; `IngressError` and `ServeError` carry the underlying `cause`; `NotAttachable` carries the `invocationId`. Your own `error` schemas appear in the error channel next to these.

## Development

`pnpm test` runs the in-memory suite, no Docker required. `pnpm test:real` runs the same shared tests against a real Restate server booted in Docker through testcontainers; it needs a running Docker daemon and is not part of `pnpm test`. With OrbStack, point it at the socket:

```bash
DOCKER_HOST=unix:///Users/<you>/.orbstack/run/docker.sock pnpm test:real
```

`pnpm run check:lint` and `pnpm run check:type` cover Biome and the type check.

## Status

Pre-1.0: the first release is 0.1.0, and the surface may change as it meets more production use. The in-memory layer follows the live one but is not a full simulation, see [In-memory differences](#in-memory-differences).

## License

Apache-2.0

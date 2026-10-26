import { it } from "@effect/vitest";
import {
	Cause,
	Deferred,
	Effect,
	Exit,
	Fiber,
	Schema as S,
	type Scope,
} from "effect";
import { TestClock } from "effect/testing";
import { expect } from "vitest";
import { Restate, ServeError } from "../src/index.js";
import { registerSharedRestateTests } from "./shared-suite.js";

const settle = Effect.forEach(Array.from({ length: 5 }), () => Effect.yieldNow);

it.layer(Restate.layerInMemory())("Restate (in-memory)", (it) => {
	registerSharedRestateTests(it);

	it.effect("a delayed send waits for the clock", () =>
		Effect.gen(function* () {
			const ran = yield* Deferred.make<string>();
			const Later = Restate.service({
				name: "LaterClock",
				handlers: {
					record: Restate.handler({ input: S.String }, (value) =>
						Deferred.succeed(ran, value),
					),
				},
			});
			yield* Restate.serve([Later]);

			yield* Restate.ingress.sendClient(Later).record("x", { delay: "1 hour" });
			yield* settle;
			expect(yield* Deferred.isDone(ran)).toBe(false);

			yield* TestClock.adjust("59 minutes");
			yield* settle;
			expect(yield* Deferred.isDone(ran)).toBe(false);

			yield* TestClock.adjust("1 minute");
			expect(yield* Deferred.await(ran)).toBe("x");
		}),
	);

	it.effect("Restate.sleep waits for the clock", () =>
		Effect.gen(function* () {
			const Napper = Restate.service({
				name: "Napper",
				handlers: {
					nap: Restate.handler({ output: S.String }, () =>
						Restate.sleep("10 minutes").pipe(Effect.as("rested")),
					),
				},
			});
			yield* Restate.serve([Napper]);

			const call = yield* Effect.forkChild(
				Restate.ingress.client(Napper).nap(),
			);
			yield* settle;
			expect(call.pollUnsafe()).toBeUndefined();

			yield* TestClock.adjust("10 minutes");
			expect(yield* Fiber.join(call)).toBe("rested");
		}),
	);

	it.effect("Restate.now follows the Effect Clock", () =>
		Effect.gen(function* () {
			const Clocked = Restate.service({
				name: "Clocked",
				handlers: {
					now: Restate.handler({ output: S.Number }, () => Restate.now),
				},
			});
			yield* Restate.serve([Clocked]);

			const clocked = Restate.ingress.client(Clocked);
			const before = yield* clocked.now();
			yield* TestClock.adjust("5 seconds");
			expect((yield* clocked.now()) - before).toBe(5000);
		}),
	);

	it.effect("a handler defect reaches the caller as a defect", () =>
		Effect.gen(function* () {
			const Broken = Restate.service({
				name: "Broken",
				handlers: {
					crash: Restate.handler({}, () => Effect.die("kaboom")),
				},
			});
			yield* Restate.serve([Broken]);

			const exit = yield* Effect.exit(Restate.ingress.client(Broken).crash());
			expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true);
		}),
	);

	it.effect("a durable call to a service that is not served is a defect", () =>
		Effect.gen(function* () {
			const Missing = Restate.service({
				name: "Missing",
				handlers: { nothing: Restate.handler({}, () => Effect.void) },
			});
			const Caller = Restate.service({
				name: "Caller",
				handlers: {
					go: Restate.handler({}, () => Restate.client(Missing).nothing()),
				},
			});
			yield* Restate.serve([Caller]);

			const exit = yield* Effect.exit(Restate.ingress.client(Caller).go());
			expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true);
		}),
	);

	it.effect("a service name can only be served once at a time", () =>
		Effect.gen(function* () {
			const Twice = Restate.service({
				name: "Twice",
				handlers: { nothing: Restate.handler({}, () => Effect.void) },
			});
			yield* Restate.serve([Twice]);

			const error = yield* Restate.serve([Twice]).pipe(Effect.flip);
			expect(error).toBeInstanceOf(ServeError);
		}),
	);

	it.effect("exclusive handlers only serialize per key", () =>
		Effect.gen(function* () {
			const entered = yield* Deferred.make<void>();
			const release = yield* Deferred.make<void>();
			const Locked = Restate.object({
				name: "Locked",
				handlers: {
					hold: Restate.handler({}, () =>
						Deferred.succeed(entered, undefined).pipe(
							Effect.andThen(Deferred.await(release)),
						),
					),
					quick: Restate.handler({ output: S.String }, () =>
						Effect.succeed("done"),
					),
				},
			});
			yield* Restate.serve([Locked]);

			const holding = yield* Effect.forkChild(
				Restate.ingress.client(Locked, "a").hold(),
			);
			yield* Deferred.await(entered);

			expect(yield* Restate.ingress.client(Locked, "b").quick()).toBe("done");
			const queued = yield* Effect.forkChild(
				Restate.ingress.client(Locked, "a").quick(),
			);
			yield* settle;
			expect(queued.pollUnsafe()).toBeUndefined();

			yield* Deferred.succeed(release, undefined);
			yield* Fiber.join(holding);
			expect(yield* Fiber.join(queued)).toBe("done");
		}),
	);

	it.effect("cancelling a caller cancels the invocation it is waiting on", () =>
		Effect.gen(function* () {
			const started = yield* Deferred.make<void>();
			const childFinalized = yield* Deferred.make<void>();
			const Child = Restate.service({
				name: "CancelChild",
				handlers: {
					wait: Restate.handler({}, () =>
						Deferred.succeed(started, undefined).pipe(
							Effect.andThen(Restate.sleep("1 hour")),
							Effect.ensuring(Deferred.succeed(childFinalized, undefined)),
						),
					),
				},
			});
			const Parent = Restate.service({
				name: "CancelParent",
				handlers: {
					run: Restate.handler({ output: S.Number }, () =>
						Restate.client(Child).wait().pipe(Effect.as(1)),
					),
				},
			});
			yield* Restate.serve([Child, Parent]);

			const parent = Restate.ingress.sendClient(Parent);
			const handle = yield* parent.run({
				idempotencyKey: "cancel-parent",
			});
			yield* Deferred.await(started);
			yield* Restate.ingress.cancel(handle.invocationId);

			const error = yield* Restate.ingress.result(handle).pipe(Effect.flip);
			expect(error).toMatchObject({ code: 409 });
			yield* Deferred.await(childFinalized);
		}),
	);

	it.effect("Restate services cannot be used inside Restate.run", () =>
		Effect.gen(function* () {
			const Nested = Restate.service({
				name: "Nested",
				handlers: {
					go: Restate.handler({ output: S.String }, () =>
						Restate.run(
							"inner",
							Restate.sleep("1 second").pipe(Effect.as("x")),
							{
								output: S.String,
							},
						),
					),
				},
			});
			yield* Restate.serve([Nested]);

			const exit = yield* Effect.exit(Restate.ingress.client(Nested).go());
			expect(Exit.isFailure(exit)).toBe(true);
			expect(Exit.isFailure(exit) && Cause.pretty(exit.cause)).toContain(
				"cannot be used inside Restate.run",
			);
		}),
	);

	it.effect(
		"input that does not match the schema is a 400 terminal failure",
		() =>
			Effect.gen(function* () {
				const Typed = Restate.service({
					name: "Typed",
					handlers: {
						echo: Restate.handler({ input: S.Number, output: S.Number }, (n) =>
							Effect.succeed(n),
						),
					},
				});
				yield* Restate.serve([Typed]);

				const restate = yield* Restate;
				const error = yield* restate.ingress
					.call({ service: "Typed", handler: "echo" }, '"not a number"', {})
					.pipe(Effect.flip);
				expect(error).toMatchObject({ _tag: "TerminalFailure", code: 400 });
			}),
	);
});

it.effect("definitions reject handler kinds that do not fit", () =>
	Effect.sync(() => {
		expect(() =>
			Restate.service({
				name: "BadService",
				// @ts-expect-error services have no shared handlers
				handlers: { a: Restate.shared({}, () => Effect.void) },
			}),
		).toThrow("services have no shared handlers");
		expect(() =>
			Restate.workflow({
				name: "NoRun",
				// @ts-expect-error workflows need a run handler
				handlers: { a: Restate.shared({}, () => Effect.void) },
			}),
		).toThrow("must have a 'run' handler");
		expect(() =>
			Restate.workflow({
				name: "BadWorkflow",
				handlers: {
					run: Restate.handler({}, () => Effect.void),
					// @ts-expect-error reserved name
					workflowSubmit: Restate.shared({}, () => Effect.void),
				},
			}),
		).toThrow("reserved name");
	}),
);

it.effect(
	"handlers that need a context their kind does not provide leak into serve",
	() =>
		Effect.sync(() => {
			const Counter = Restate.object({
				name: "LeakCounter",
				handlers: {
					peek: Restate.shared({}, () => Restate.state("n", S.Number).set(1)),
				},
			});
			// @ts-expect-error a shared handler cannot write state: ObjectContext stays required
			const serve: Effect.Effect<unknown, unknown, Scope.Scope | Restate> =
				Restate.serve([Counter]);
			expect(serve).toBeDefined();
		}),
);

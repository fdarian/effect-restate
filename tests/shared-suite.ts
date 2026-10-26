import type { Vitest } from "@effect/vitest";
import {
	Context,
	Deferred,
	Effect,
	Fiber,
	Layer,
	Option,
	Ref,
	Schema as S,
	Schedule,
	type Scope,
} from "effect";
import { TestClock } from "effect/testing";
import { expect } from "vitest";
import { Restate, TerminalFailure } from "../src/index.js";

class Mailer extends Context.Service<Mailer>()("Mailer", {
	make: Effect.succeed({
		send: (to: string) => Effect.succeed(`id-for-${to}`),
	}),
}) {
	static readonly layer = Layer.effect(this, this.make);
}

class GreetError extends S.TaggedError<GreetError>()("GreetError", {
	reason: S.String,
}) {}

const uid = () => crypto.randomUUID().slice(0, 8);

/**
 * Tests that hold under both `Restate.layerInMemory()` and a real Restate
 * server. They use real time only (short delays), unique service names and
 * unique keys, so they can share one server.
 */
export function registerSharedRestateTests(it: Vitest.MethodsNonLive<Restate>) {
	/** `it.effect` runs on the TestClock; these tests need the real one. */
	const test = (
		name: string,
		body: () => Effect.Effect<void, unknown, Restate | Scope.Scope>,
	) => it.effect(name, () => TestClock.withLive(body()), { timeout: 30_000 });

	const eventually = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
		effect.pipe(
			Effect.retry(Schedule.spaced("25 millis")),
			Effect.timeout("10 seconds"),
		);

	test("service call round-trips input and output through schemas", () =>
		Effect.gen(function* () {
			const Greeter = Restate.service({
				name: `Greeter${uid()}`,
				handlers: {
					greet: Restate.handler(
						{
							input: S.Struct({ name: S.String, at: S.Date }),
							output: S.Struct({ greeting: S.String, year: S.Number }),
						},
						(input) =>
							Effect.succeed({
								greeting: `Hello ${input.name}`,
								year: input.at.getUTCFullYear(),
							}),
					),
					ping: Restate.handler({}, () => Effect.succeed("pong")),
				},
			});
			yield* Restate.serve([Greeter]);
			const greeter = Restate.ingress.client(Greeter);
			expect(
				yield* greeter.greet({
					name: "Ada",
					at: new Date("2020-06-01T00:00:00.000Z"),
				}),
			).toEqual({ greeting: "Hello Ada", year: 2020 });
			expect(yield* greeter.ping()).toBe("pong");
		}));

	test("a typed error with an error schema round-trips to the caller", () =>
		Effect.gen(function* () {
			const Strict = Restate.service({
				name: `Strict${uid()}`,
				handlers: {
					greet: Restate.handler(
						{ input: S.String, output: S.String, error: GreetError },
						(name) =>
							name === ""
								? Effect.fail(new GreetError({ reason: "empty" }))
								: Effect.succeed(`hi ${name}`),
					),
				},
			});
			const Wrapper = Restate.service({
				name: `Wrapper${uid()}`,
				handlers: {
					greet: Restate.handler({ output: S.String }, () =>
						Restate.client(Strict)
							.greet("")
							.pipe(
								Effect.catchTag("GreetError", (error) =>
									Effect.succeed(`caught:${error.reason}`),
								),
							),
					),
				},
			});
			yield* Restate.serve([Strict, Wrapper]);

			const error = yield* Restate.ingress
				.client(Strict)
				.greet("")
				.pipe(Effect.flip);
			expect(error).toBeInstanceOf(GreetError);
			if (error instanceof GreetError) expect(error.reason).toBe("empty");

			expect(yield* Restate.ingress.client(Wrapper).greet()).toBe(
				"caught:empty",
			);
		}));

	test("failures without an error schema reach the caller as TerminalFailure", () =>
		Effect.gen(function* () {
			const Plain = Restate.service({
				name: `Plain${uid()}`,
				handlers: {
					fail: Restate.handler({}, () => Effect.fail(new Error("boom"))),
					teapot: Restate.handler({}, () =>
						Effect.fail(new TerminalFailure({ message: "short", code: 418 })),
					),
				},
			});
			yield* Restate.serve([Plain]);
			const plain = Restate.ingress.client(Plain);

			const boom = yield* plain.fail().pipe(Effect.flip);
			expect(boom).toBeInstanceOf(TerminalFailure);
			expect(boom).toMatchObject({ message: "boom", code: 500 });

			const teapot = yield* plain.teapot().pipe(Effect.flip);
			expect(teapot).toMatchObject({ message: "short", code: 418 });
		}));

	test("object state persists across calls per key and exclusive handlers serialize", () =>
		Effect.gen(function* () {
			const count = Restate.state("count", S.Number);
			const Counter = Restate.object({
				name: `Counter${uid()}`,
				handlers: {
					add: Restate.handler(
						{ input: S.Number, output: S.Number },
						(amount) =>
							Effect.gen(function* () {
								const current = Option.getOrElse(yield* count.get, () => 0);
								yield* Restate.sleep("10 millis");
								yield* count.set(current + amount);
								return current + amount;
							}),
					),
					get: Restate.shared({ output: S.Number }, () =>
						Effect.map(
							count.get,
							Option.getOrElse(() => 0),
						),
					),
					whoAmI: Restate.shared({ output: S.String }, () => Restate.objectKey),
					keys: Restate.shared(
						{ output: S.Array(S.String) },
						() => Restate.stateKeys,
					),
					reset: Restate.handler({}, () => Restate.clearAllState),
				},
			});
			yield* Restate.serve([Counter]);
			const key = uid();
			const counter = Restate.ingress.client(Counter, key);

			yield* Effect.all(
				Array.from({ length: 8 }, () => counter.add(1)),
				{ concurrency: "unbounded" },
			);
			expect(yield* counter.get()).toBe(8);
			expect(yield* counter.whoAmI()).toBe(key);
			expect(yield* counter.keys()).toEqual(["count"]);

			expect(yield* Restate.ingress.client(Counter, uid()).get()).toBe(0);

			yield* counter.reset();
			expect(yield* counter.get()).toBe(0);
			expect(yield* counter.keys()).toEqual([]);
		}));

	test("shared handlers run while an exclusive handler holds the key", () =>
		Effect.gen(function* () {
			const phase = Restate.state("phase", S.String);
			const Gate = Restate.object({
				name: `Gate${uid()}`,
				handlers: {
					hold: Restate.handler({}, () =>
						Effect.gen(function* () {
							yield* phase.set("holding");
							yield* Restate.sleep("600 millis");
							yield* phase.set("done");
						}),
					),
					phase: Restate.shared({ output: S.String }, () =>
						Effect.map(
							phase.get,
							Option.getOrElse(() => "idle"),
						),
					),
				},
			});
			yield* Restate.serve([Gate]);
			const key = uid();
			const send = Restate.ingress.sendClient(Gate, key);
			const gate = Restate.ingress.client(Gate, key);

			yield* send.hold();
			yield* eventually(
				gate
					.phase()
					.pipe(
						Effect.flatMap((value) =>
							value === "holding" ? Effect.void : Effect.fail(value),
						),
					),
			);
			yield* eventually(
				gate
					.phase()
					.pipe(
						Effect.flatMap((value) =>
							value === "done" ? Effect.void : Effect.fail(value),
						),
					),
			);
		}));

	test("workflow runs once per key, durable promises are resolved by shared handlers", () =>
		Effect.gen(function* () {
			const runs = yield* Ref.make(0);
			const Signup = Restate.workflow({
				name: `Signup${uid()}`,
				handlers: {
					run: Restate.handler({ input: S.String, output: S.String }, (email) =>
						Effect.gen(function* () {
							yield* Ref.update(runs, (n) => n + 1);
							const decision = yield* Restate.promise("approval", S.String)
								.await;
							return `${email}:${decision}`;
						}),
					),
					approve: Restate.shared({ input: S.String }, (decision) =>
						Restate.promise("approval", S.String).resolve(decision),
					),
				},
			});
			yield* Restate.serve([Signup]);
			const signup = Restate.ingress.workflowClient(Signup, uid());

			const first = yield* signup.submit("ada@example.com");
			const second = yield* signup.submit("ada@example.com");
			expect(second.invocationId).toBe(first.invocationId);
			expect(first.attachable).toBe(true);
			expect(Option.isNone(yield* signup.output)).toBe(true);

			yield* signup.approve("yes");
			expect(yield* signup.attach).toBe("ada@example.com:yes");
			expect(yield* Restate.ingress.result(first)).toBe("ada@example.com:yes");
			expect(yield* signup.output).toEqual(Option.some("ada@example.com:yes"));
			expect(yield* Ref.get(runs)).toBe(1);
		}));

	test("a durable promise completes once: the second completion fails with a TerminalFailure the handler can recover from", () =>
		Effect.gen(function* () {
			const promise = Restate.promise("answer", S.String);
			const Oracle = Restate.workflow({
				name: `Oracle${uid()}`,
				handlers: {
					run: Restate.handler({ output: S.String }, () => promise.await),
					answer: Restate.shared({ input: S.String }, (value) =>
						promise.resolve(value),
					),
					answerRecovering: Restate.shared(
						{ input: S.String, output: S.Number },
						(value) =>
							promise.resolve(value).pipe(
								Effect.as(0),
								Effect.catchTag("TerminalFailure", (failure) =>
									Effect.succeed(failure.code),
								),
							),
					),
					peek: Restate.shared(
						{ output: S.Option(S.String) },
						() => promise.peek,
					),
				},
			});
			yield* Restate.serve([Oracle]);
			const oracle = Restate.ingress.workflowClient(Oracle, uid());

			yield* oracle.submit();
			expect(yield* oracle.peek()).toEqual(Option.none());
			yield* oracle.answer("first");
			const second = yield* oracle.answer("second").pipe(Effect.flip);
			expect(second).toMatchObject({ _tag: "TerminalFailure", code: 409 });
			expect(yield* oracle.answerRecovering("third")).toBe(409);
			expect(yield* oracle.attach).toBe("first");
			expect(yield* oracle.peek()).toEqual(Option.some("first"));
		}));

	test("peeking a rejected durable promise fails with the rejection, which the handler can recover from", () =>
		Effect.gen(function* () {
			const promise = Restate.promise("answer", S.String);
			const Oracle = Restate.workflow({
				name: `Denied${uid()}`,
				handlers: {
					run: Restate.handler({ output: S.String }, () => promise.await),
					deny: Restate.shared({}, () => promise.reject("denied")),
					peek: Restate.shared(
						{ output: S.Option(S.String) },
						() => promise.peek,
					),
					peekRecovered: Restate.shared({ output: S.String }, () =>
						promise.peek.pipe(
							Effect.as("pending"),
							Effect.catchTag("TerminalFailure", (failure) =>
								Effect.succeed(`recovered:${failure.message}`),
							),
						),
					),
				},
			});
			yield* Restate.serve([Oracle]);
			const oracle = Restate.ingress.workflowClient(Oracle, uid());

			yield* oracle.submit();
			yield* oracle.deny();
			expect(yield* oracle.peek().pipe(Effect.flip)).toMatchObject({
				_tag: "TerminalFailure",
				message: "denied",
			});
			expect(yield* oracle.peekRecovered()).toBe("recovered:denied");
			expect(yield* oracle.attach.pipe(Effect.flip)).toMatchObject({
				message: "denied",
			});
		}));

	test("a workflow that was never submitted has neither output nor result", () =>
		Effect.gen(function* () {
			const Idle = Restate.workflow({
				name: `Idle${uid()}`,
				handlers: {
					run: Restate.handler({ output: S.String }, () => Effect.succeed("x")),
				},
			});
			yield* Restate.serve([Idle]);
			const idle = Restate.ingress.workflowClient(Idle, uid());

			expect((yield* idle.output.pipe(Effect.flip))._tag).toBe("IngressError");
			expect(yield* idle.attach.pipe(Effect.flip)).toMatchObject({
				_tag: "TerminalFailure",
				code: 404,
			});
		}));

	test("calling an unknown service or handler through ingress is an ingress error", () =>
		Effect.gen(function* () {
			const Known = Restate.service({
				name: `Known${uid()}`,
				handlers: {
					go: Restate.handler({ output: S.String }, () => Effect.succeed("x")),
				},
			});
			const Missing = Restate.service({
				name: `Missing${uid()}`,
				handlers: {
					go: Restate.handler({ output: S.String }, () => Effect.succeed("x")),
				},
			});
			const Wrong = Restate.service({
				name: Known.name,
				handlers: {
					other: Restate.handler({ output: S.String }, () =>
						Effect.succeed("x"),
					),
				},
			});
			yield* Restate.serve([Known]);

			const unknownService = yield* Restate.ingress
				.client(Missing)
				.go()
				.pipe(Effect.flip);
			expect(unknownService._tag).toBe("IngressError");
			const unknownHandler = yield* Restate.ingress
				.client(Wrong)
				.other()
				.pipe(Effect.flip);
			expect(unknownHandler._tag).toBe("IngressError");
			const unknownSend = yield* Restate.ingress
				.sendClient(Missing)
				.go()
				.pipe(Effect.flip);
			expect(unknownSend._tag).toBe("IngressError");
		}));

	test("cancelling a finished invocation is an ingress error", () =>
		Effect.gen(function* () {
			const Quick = Restate.service({
				name: `Quick${uid()}`,
				handlers: {
					go: Restate.handler({ output: S.String }, () =>
						Effect.succeed("done"),
					),
				},
			});
			yield* Restate.serve([Quick]);

			const handle = yield* Restate.ingress
				.sendClient(Quick)
				.go({ idempotencyKey: uid() });
			expect(yield* Restate.ingress.result(handle)).toBe("done");
			const error = yield* Restate.ingress
				.cancel(handle.invocationId)
				.pipe(Effect.flip);
			expect(error._tag).toBe("IngressError");
		}));

	test("an awakeable created in a handler is resolved through ingress", () =>
		Effect.gen(function* () {
			const ids = yield* Deferred.make<string>();
			const Waiter = Restate.service({
				name: `Waiter${uid()}`,
				handlers: {
					wait: Restate.handler({ output: S.String }, () =>
						Effect.gen(function* () {
							const awakeable = yield* Restate.awakeable(S.String);
							yield* Deferred.succeed(ids, awakeable.id);
							return yield* awakeable.await;
						}),
					),
				},
			});
			yield* Restate.serve([Waiter]);
			const waiter = Restate.ingress.client(Waiter);

			const resolved = yield* Effect.forkChild(waiter.wait());
			yield* Restate.ingress.resolveAwakeable(
				yield* Deferred.await(ids),
				S.String,
				"pong",
			);
			expect(yield* Fiber.join(resolved)).toBe("pong");
		}));

	test("a rejected awakeable fails the awaiting handler with a TerminalFailure", () =>
		Effect.gen(function* () {
			const ids = yield* Deferred.make<string>();
			const Waiter = Restate.service({
				name: `Rejected${uid()}`,
				handlers: {
					wait: Restate.handler({ output: S.String }, () =>
						Effect.gen(function* () {
							const awakeable = yield* Restate.awakeable(S.String);
							yield* Deferred.succeed(ids, awakeable.id);
							return yield* awakeable.await;
						}),
					),
				},
			});
			yield* Restate.serve([Waiter]);

			const call = yield* Effect.forkChild(
				Restate.ingress.client(Waiter).wait().pipe(Effect.flip),
			);
			yield* Restate.ingress.rejectAwakeable(
				yield* Deferred.await(ids),
				"nope",
			);
			expect(yield* Fiber.join(call)).toMatchObject({ message: "nope" });
		}));

	test("handlers call other services durably", () =>
		Effect.gen(function* () {
			const Greeter = Restate.service({
				name: `Greeter${uid()}`,
				handlers: {
					greet: Restate.handler(
						{ input: S.String, output: S.String },
						(name) => Effect.succeed(`Hello ${name}`),
					),
				},
			});
			const Notes = Restate.object({
				name: `Notes${uid()}`,
				handlers: {
					append: Restate.handler(
						{ input: S.String, output: S.Number },
						(line) =>
							Effect.gen(function* () {
								const lines = Restate.state("lines", S.Number);
								const next = Option.getOrElse(yield* lines.get, () => 0) + 1;
								yield* lines.set(next);
								return next + line.length;
							}),
					),
				},
			});
			const Relay = Restate.service({
				name: `Relay${uid()}`,
				handlers: {
					run: Restate.handler({ input: S.String, output: S.String }, (name) =>
						Effect.gen(function* () {
							const greeting = yield* Restate.client(Greeter).greet(name);
							const size = yield* Restate.client(Notes, "shared-key").append(
								greeting,
							);
							return `${greeting}/${size}`;
						}),
					),
				},
			});
			yield* Restate.serve([Greeter, Notes, Relay]);

			expect(yield* Restate.ingress.client(Relay).run("Ada")).toBe(
				"Hello Ada/10",
			);
		}));

	test("a delayed send runs after the delay", () =>
		Effect.gen(function* () {
			const ran = yield* Deferred.make<number>();
			const Later = Restate.service({
				name: `Later${uid()}`,
				handlers: {
					record: Restate.handler({ input: S.String }, () =>
						Deferred.succeed(ran, Date.now()),
					),
				},
			});
			yield* Restate.serve([Later]);
			const sentAt = Date.now();

			yield* Restate.ingress
				.sendClient(Later)
				.record("x", { delay: "400 millis" });
			const ranAt = yield* Deferred.await(ran).pipe(
				Effect.timeout("10 seconds"),
			);
			expect(ranAt - sentAt).toBeGreaterThanOrEqual(350);
		}));

	test("a handler can send to another service", () =>
		Effect.gen(function* () {
			const received = yield* Deferred.make<string>();
			const Inbox = Restate.service({
				name: `Inbox${uid()}`,
				handlers: {
					deliver: Restate.handler({ input: S.String }, (message) =>
						Deferred.succeed(received, message),
					),
				},
			});
			const Outbox = Restate.service({
				name: `Outbox${uid()}`,
				handlers: {
					post: Restate.handler(
						{ input: S.String, output: S.String },
						(message) => Restate.sendClient(Inbox).deliver(message),
					),
				},
			});
			yield* Restate.serve([Inbox, Outbox]);

			const invocationId = yield* Restate.ingress.client(Outbox).post("hello");
			expect(invocationId.length).toBeGreaterThan(0);
			expect(
				yield* Deferred.await(received).pipe(Effect.timeout("10 seconds")),
			).toBe("hello");
		}));

	test("an idempotent send returns the same invocation, runs once, and its result is attachable", () =>
		Effect.gen(function* () {
			const executions = yield* Ref.make(0);
			const Once = Restate.service({
				name: `Once${uid()}`,
				handlers: {
					compute: Restate.handler({ input: S.Number, output: S.Number }, (n) =>
						Ref.update(executions, (count) => count + 1).pipe(Effect.as(n * 2)),
					),
				},
			});
			yield* Restate.serve([Once]);
			const send = Restate.ingress.sendClient(Once);
			const idempotencyKey = uid();

			const first = yield* send.compute(21, { idempotencyKey });
			const second = yield* send.compute(21, { idempotencyKey });
			expect(second.invocationId).toBe(first.invocationId);
			expect(first.attachable).toBe(true);
			expect(yield* Restate.ingress.result(first)).toBe(42);
			expect(yield* Ref.get(executions)).toBe(1);

			const anonymous = yield* send.compute(1);
			expect(anonymous.attachable).toBe(false);
			const error = yield* Restate.ingress.result(anonymous).pipe(Effect.flip);
			expect(error._tag).toBe("NotAttachable");
		}));

	test("Restate.run journals a result and turns a typed failure into a typed error", () =>
		Effect.gen(function* () {
			const Steps = Restate.service({
				name: `Steps${uid()}`,
				handlers: {
					go: Restate.handler({ output: S.String }, () =>
						Effect.gen(function* () {
							const value = yield* Restate.run("compute", Effect.succeed(21), {
								output: S.Number,
							});
							const failure = yield* Restate.run(
								"risky",
								Effect.fail(new GreetError({ reason: "nope" })),
								{ error: GreetError },
							).pipe(Effect.flip);
							return `${value * 2}:${failure.reason}`;
						}),
					),
				},
			});
			yield* Restate.serve([Steps]);

			expect(yield* Restate.ingress.client(Steps).go()).toBe("42:nope");
		}));

	test("cancelling an invocation runs its finalizers and fails the caller with 409", () =>
		Effect.gen(function* () {
			const started = yield* Deferred.make<string>();
			const finalized = yield* Deferred.make<void>();
			const Slow = Restate.service({
				name: `Slow${uid()}`,
				handlers: {
					wait: Restate.handler({ output: S.String }, () =>
						Effect.gen(function* () {
							yield* Deferred.succeed(started, yield* Restate.invocationId);
							yield* Restate.sleep("1 hour");
							return "unreachable";
						}).pipe(Effect.ensuring(Deferred.succeed(finalized, undefined))),
					),
				},
			});
			yield* Restate.serve([Slow]);

			const call = yield* Effect.forkChild(
				Restate.ingress.client(Slow).wait().pipe(Effect.flip),
			);
			yield* Restate.ingress.cancel(yield* Deferred.await(started));

			expect(yield* Fiber.join(call)).toMatchObject({ code: 409 });
			yield* Deferred.await(finalized).pipe(Effect.timeout("10 seconds"));
		}));

	test("handler requirements beyond the Restate contexts are satisfied at serve time", () =>
		Effect.gen(function* () {
			const Welcome = Restate.service({
				name: `Welcome${uid()}`,
				handlers: {
					send: Restate.handler({ input: S.String, output: S.String }, (to) =>
						Effect.gen(function* () {
							const mailer = yield* Mailer;
							return yield* mailer.send(to);
						}),
					),
				},
			});
			yield* Restate.serve([Welcome]).pipe(Effect.provide(Mailer.layer));

			expect(yield* Restate.ingress.client(Welcome).send("ada")).toBe(
				"id-for-ada",
			);
		}));
}

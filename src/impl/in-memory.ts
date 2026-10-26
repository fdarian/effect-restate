import {
	Cause,
	Clock,
	Context,
	Deferred,
	Duration,
	Effect,
	Exit,
	Fiber,
	Layer,
	Option,
	Random,
	Semaphore,
} from "effect";
import {
	type CallTarget,
	ObjectContext,
	type ObjectContextShape,
	RestateContext,
	type RestateContextShape,
	SharedObjectContext,
	type SharedObjectContextShape,
	WorkflowContext,
	type WorkflowContextShape,
} from "../core/context.js";
import type { AnyDefinition } from "../core/definition.js";
import { IngressError, ServeError, TerminalFailure } from "../core/errors.js";
import type { AnyHandler } from "../core/handler.js";
import { executeHandler } from "../core/invoke.js";
import {
	type IngressDriver,
	type RestateService,
	RestateTag,
	type SendReceipt,
} from "../core/restate.js";

type Invocation = {
	readonly id: string;
	readonly fiber: Fiber.Fiber<string, TerminalFailure>;
	cancelled: boolean;
};

/** Everything Restate keeps per object or workflow key. */
type Entity = {
	readonly state: Map<string, string>;
	readonly lock: Semaphore.Semaphore;
	readonly promises: Map<string, Deferred.Deferred<string, TerminalFailure>>;
	run: Invocation | undefined;
};

type Served = {
	readonly definition: AnyDefinition;
	readonly services: Context.Context<never>;
};

type InvokeOptions = {
	readonly delay?: Duration.Input;
	readonly idempotencyKey?: string;
};

const cancelledFailure = () =>
	new TerminalFailure({ message: "Cancelled", code: 409 });

const unknownId = (kind: string, id: string) =>
	new IngressError({ cause: new Error(`Unknown ${kind} '${id}'`) });

export const make = Effect.gen(function* () {
	// Invocation fibers are forked into the layer scope, so sends die with the
	// layer instead of leaking past it.
	const scope = yield* Effect.scope;

	const served = new Map<string, Served>();
	const entities = new Map<string, Entity>();
	const invocations = new Map<string, Invocation>();
	const idempotent = new Map<string, Invocation>();
	const awakeables = new Map<
		string,
		Deferred.Deferred<string, TerminalFailure>
	>();

	const entityOf = (service: string, key: string) => {
		const id = `${service}\u0000${key}`;
		const existing = entities.get(id);
		if (existing !== undefined) return existing;
		const created: Entity = {
			state: new Map(),
			lock: Semaphore.makeUnsafe(1),
			promises: new Map(),
			run: undefined,
		};
		entities.set(id, created);
		return created;
	};

	const awaitInvocation = (invocation: Invocation) =>
		Fiber.await(invocation.fiber).pipe(
			Effect.flatMap((exit) =>
				invocation.cancelled &&
				Exit.isFailure(exit) &&
				Cause.hasInterruptsOnly(exit.cause)
					? Effect.fail(cancelledFailure())
					: exit,
			),
		);

	const cancelInvocation = (invocation: Invocation) =>
		Effect.suspend(() => {
			invocation.cancelled = true;
			return Effect.forkIn(Fiber.interrupt(invocation.fiber), scope);
		}).pipe(Effect.asVoid, Effect.uninterruptible);

	const contextsFor = (
		served: Served,
		handler: AnyHandler,
		handlerName: string,
		id: string,
		entity: Entity,
		key: string,
	) => {
		const restateContext: RestateContextShape = {
			invocationId: () => Effect.succeed(id),
			run: (action) => action,
			sleep: (duration) => Effect.sleep(duration),
			awakeable: () =>
				Effect.sync(() => {
					const awakeableId = `awk_${crypto.randomUUID()}`;
					const deferred = Deferred.makeUnsafe<string, TerminalFailure>();
					awakeables.set(awakeableId, deferred);
					return { id: awakeableId, await: Deferred.await(deferred) };
				}),
			resolveAwakeable: (id, value) =>
				Effect.suspend(() => {
					const deferred = awakeables.get(id);
					return deferred === undefined
						? Effect.die(new Error(`Unknown awakeable '${id}'`))
						: Deferred.succeed(deferred, value).pipe(Effect.asVoid);
				}),
			rejectAwakeable: (id, reason) =>
				Effect.suspend(() => {
					const deferred = awakeables.get(id);
					return deferred === undefined
						? Effect.die(new Error(`Unknown awakeable '${id}'`))
						: Deferred.fail(
								deferred,
								new TerminalFailure({ message: reason, code: 500 }),
							).pipe(Effect.asVoid);
				}),
			call: (target, parameter, options) =>
				invoke(target, parameter, options).pipe(
					Effect.flatMap((child) =>
						awaitInvocation(child.invocation).pipe(
							Effect.onInterrupt(() => cancelInvocation(child.invocation)),
						),
					),
				),
			send: (target, parameter, options) =>
				invoke(target, parameter, options).pipe(
					Effect.map((child) => child.invocation.id),
				),
			cancel: (id) =>
				Effect.suspend(() => {
					const target = invocations.get(id);
					return target === undefined
						? Effect.die(new Error(`Unknown invocation '${id}'`))
						: cancelInvocation(target);
				}),
			random: () => Random.next,
			uuid: () => Effect.sync(() => crypto.randomUUID()),
			now: () => Clock.currentTimeMillis,
		};
		const sharedObjectContext: SharedObjectContextShape = {
			key: () => Effect.succeed(key),
			getState: (name) =>
				Effect.sync(() => Option.fromNullishOr(entity.state.get(name))),
			stateKeys: () => Effect.sync(() => Array.from(entity.state.keys())),
		};
		const objectContext: ObjectContextShape = {
			setState: (name, value) =>
				Effect.sync(() => {
					entity.state.set(name, value);
				}),
			clearState: (name) =>
				Effect.sync(() => {
					entity.state.delete(name);
				}),
			clearAllState: () =>
				Effect.sync(() => {
					entity.state.clear();
				}),
		};
		const promise = (name: string) => {
			const existing = entity.promises.get(name);
			if (existing !== undefined) return existing;
			const created = Deferred.makeUnsafe<string, TerminalFailure>();
			entity.promises.set(name, created);
			return created;
		};
		const workflowContext: WorkflowContextShape = {
			promiseGet: (name) => Effect.suspend(() => Deferred.await(promise(name))),
			promisePeek: (name) =>
				Effect.suspend(() => Deferred.poll(promise(name))).pipe(
					Effect.flatMap(
						Option.match({
							onNone: () => Effect.succeed(Option.none<string>()),
							onSome: (outcome) => Effect.map(outcome, Option.some),
						}),
					),
				),
			promiseResolve: (name, value) =>
				Effect.suspend(() =>
					Deferred.succeed(promise(name), value).pipe(Effect.asVoid),
				),
			promiseReject: (name, reason) =>
				Effect.suspend(() =>
					Deferred.fail(
						promise(name),
						new TerminalFailure({ message: reason, code: 500 }),
					).pipe(Effect.asVoid),
				),
		};

		const base = Context.make(RestateContext, restateContext);
		const definition = served.definition;
		if (definition.kind === "service") return base;
		const withShared = base.pipe(
			Context.add(SharedObjectContext, sharedObjectContext),
		);
		if (definition.kind === "object") {
			return handler.kind === "handler"
				? withShared.pipe(Context.add(ObjectContext, objectContext))
				: withShared;
		}
		const withWorkflow = withShared.pipe(
			Context.add(WorkflowContext, workflowContext),
		);
		return handlerName === "run"
			? withWorkflow.pipe(Context.add(ObjectContext, objectContext))
			: withWorkflow;
	};

	const invoke = (
		target: CallTarget,
		parameter: string,
		options: InvokeOptions,
	) =>
		Effect.gen(function* () {
			const entry = served.get(target.service);
			if (entry === undefined) {
				return yield* Effect.die(
					new Error(
						`Unknown service '${target.service}': make sure it is served`,
					),
				);
			}
			const definition = entry.definition;
			const handler = definition.handlers[target.handler];
			if (handler === undefined) {
				return yield* Effect.die(
					new Error(`Unknown handler '${target.service}/${target.handler}'`),
				);
			}
			if (definition.kind === "service" && target.key !== undefined) {
				return yield* Effect.die(
					new Error(`Service '${target.service}' takes no key`),
				);
			}
			if (definition.kind !== "service" && target.key === undefined) {
				return yield* Effect.die(
					new Error(`'${target.service}' is keyed: a key is required`),
				);
			}
			const key = target.key ?? "";
			const entity = entityOf(target.service, key);

			const isWorkflowRun =
				definition.kind === "workflow" && target.handler === "run";
			if (isWorkflowRun && entity.run !== undefined) {
				return { invocation: entity.run, accepted: false };
			}
			const idempotencyId =
				options.idempotencyKey === undefined
					? undefined
					: [target.service, key, target.handler, options.idempotencyKey].join(
							"\u0000",
						);
			if (idempotencyId !== undefined) {
				const existing = idempotent.get(idempotencyId);
				if (existing !== undefined) {
					return { invocation: existing, accepted: false };
				}
			}

			const id = `inv_${crypto.randomUUID()}`;
			const ready = yield* Deferred.make<void>();
			const context = Context.merge(
				entry.services,
				contextsFor(entry, handler, target.handler, id, entity, key),
			);
			const exclusive =
				definition.kind === "object" && handler.kind === "handler";
			const execute = executeHandler(handler, parameter).pipe(
				Effect.provideContext(context),
			);
			const delayed =
				options.delay === undefined
					? execute
					: Effect.sleep(Duration.fromInputUnsafe(options.delay)).pipe(
							Effect.andThen(execute),
						);
			const fiber = yield* Effect.forkIn(
				Deferred.await(ready).pipe(
					Effect.andThen(
						exclusive ? Semaphore.withPermit(entity.lock, delayed) : delayed,
					),
					Effect.interruptible,
				),
				scope,
			);
			const invocation: Invocation = { id, fiber, cancelled: false };
			invocations.set(id, invocation);
			if (idempotencyId !== undefined)
				idempotent.set(idempotencyId, invocation);
			if (isWorkflowRun) entity.run = invocation;
			yield* Deferred.succeed(ready, undefined);
			return { invocation, accepted: true };
		}).pipe(Effect.uninterruptible);

	const receipt = (
		invocation: Invocation,
		attachable: boolean,
	): SendReceipt => ({ invocationId: invocation.id, attachable });

	const ingress: IngressDriver = {
		call: (target, parameter, options) =>
			invoke(target, parameter, options).pipe(
				Effect.flatMap((child) => awaitInvocation(child.invocation)),
			),
		send: (target, parameter, options) =>
			invoke(target, parameter, options).pipe(
				Effect.map((child) =>
					receipt(child.invocation, options.idempotencyKey !== undefined),
				),
			),
		attach: (invocationId) =>
			Effect.suspend(
				(): Effect.Effect<string, TerminalFailure | IngressError> => {
					const invocation = invocations.get(invocationId);
					return invocation === undefined
						? Effect.fail(unknownId("invocation", invocationId))
						: awaitInvocation(invocation);
				},
			),
		workflowSubmit: (target, parameter, options) =>
			invoke(target, parameter, options).pipe(
				Effect.map((child) => receipt(child.invocation, true)),
			),
		workflowAttach: (service, key) =>
			Effect.suspend(
				(): Effect.Effect<string, TerminalFailure | IngressError> => {
					const run = entityOf(service, key).run;
					return run === undefined
						? Effect.fail(unknownId("workflow", `${service}/${key}`))
						: awaitInvocation(run);
				},
			),
		workflowOutput: (service, key) =>
			Effect.suspend(() => {
				const run = entityOf(service, key).run;
				if (run === undefined || run.fiber.pollUnsafe() === undefined) {
					return Effect.succeed(Option.none<string>());
				}
				return awaitInvocation(run).pipe(Effect.map(Option.some));
			}),
		resolveAwakeable: (id, value) =>
			Effect.suspend(() => {
				const deferred = awakeables.get(id);
				return deferred === undefined
					? Effect.fail(unknownId("awakeable", id))
					: Deferred.succeed(deferred, value).pipe(Effect.asVoid);
			}),
		rejectAwakeable: (id, reason) =>
			Effect.suspend(() => {
				const deferred = awakeables.get(id);
				return deferred === undefined
					? Effect.fail(unknownId("awakeable", id))
					: Deferred.fail(
							deferred,
							new TerminalFailure({ message: reason, code: 500 }),
						).pipe(Effect.asVoid);
			}),
		cancel: (invocationId) =>
			Effect.suspend(() => {
				const invocation = invocations.get(invocationId);
				return invocation === undefined
					? Effect.fail(unknownId("invocation", invocationId))
					: cancelInvocation(invocation);
			}),
	};

	const serve: RestateService["serve"] = (definitions) =>
		Effect.gen(function* () {
			const services = yield* Effect.context<never>();
			const names: Array<string> = [];
			for (const definition of definitions) {
				if (served.has(definition.name)) {
					return yield* new ServeError({
						cause: new Error(`Service '${definition.name}' is already served`),
					});
				}
				served.set(definition.name, { definition, services });
				names.push(definition.name);
			}
			yield* Effect.addFinalizer(() =>
				Effect.sync(() => {
					for (const name of names) served.delete(name);
				}),
			);
			return { port: 0 };
		}) as never;

	return { serve, ingress } satisfies RestateService;
});

export const layer = Layer.effect(RestateTag, make);

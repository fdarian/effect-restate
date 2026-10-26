import { Context, Duration, Effect, Option } from "effect";
import {
	type AnySchema,
	decodeValue,
	encodeFailure,
	encodeValue,
	failFromTerminal,
} from "./codec.js";
import {
	ObjectContext,
	type ObjectContextShape,
	RestateContext,
	type RestateContextShape,
	type RetryOptions,
	SharedObjectContext,
	type SharedObjectContextShape,
	WorkflowContext,
	type WorkflowContextShape,
} from "./context.js";
import { TerminalFailure } from "./errors.js";

const forbidden = <S>(label: string) =>
	new Proxy(
		{},
		{
			get: () => () =>
				Effect.die(
					new Error(
						`${label} cannot be used inside Restate.run: the closure is not part of the journal`,
					),
				),
		},
	) as S;

const insideRun = Context.make(
	RestateContext,
	forbidden<RestateContextShape>("RestateContext"),
).pipe(
	Context.add(
		SharedObjectContext,
		forbidden<SharedObjectContextShape>("SharedObjectContext"),
	),
	Context.add(ObjectContext, forbidden<ObjectContextShape>("ObjectContext")),
	Context.add(
		WorkflowContext,
		forbidden<WorkflowContextShape>("WorkflowContext"),
	),
);

type RunOutput<A> = { readonly output?: AnySchema & { readonly Type: A } };

export type RunOptions<A> = RunOutput<A> & {
	readonly retry?: RetryOptions;
};

/**
 * Durable side effect. The result is journaled and returned as is on replay.
 * A typed failure is journaled too and never retried; it needs an `error`
 * schema so it can be reconstructed. A defect or interruption inside `effect`
 * is retried according to `retry`; once retries run out the invocation dies
 * with a `TerminalFailure`.
 *
 * Restate services cannot be used inside `effect`; other services are
 * available as in the surrounding handler.
 */
export function run<A, R>(
	name: string,
	effect: Effect.Effect<A, never, R>,
	options?: RunOptions<A>,
): Effect.Effect<A, never, R | RestateContext>;
export function run<A, ES extends AnySchema, R>(
	name: string,
	effect: Effect.Effect<A, ES["Type"], R>,
	options: RunOptions<A> & { readonly error: ES },
): Effect.Effect<A, ES["Type"], R | RestateContext>;
export function run(
	name: string,
	effect: Effect.Effect<unknown, unknown, unknown>,
	options?: RunOptions<unknown> & { readonly error?: AnySchema },
): Effect.Effect<unknown, unknown, unknown> {
	return Effect.gen(function* () {
		const ctx = yield* RestateContext;
		const services = yield* Effect.context<never>();
		const sandboxed = Effect.provideContext(
			effect as Effect.Effect<unknown, unknown, never>,
			Context.merge(services, insideRun),
		);
		const action = sandboxed.pipe(
			Effect.catch((error) =>
				encodeFailure(options?.error, error).pipe(
					Effect.orDie,
					Effect.flatMap(Effect.fail),
				),
			),
			Effect.flatMap((value) =>
				encodeValue(options?.output, value).pipe(Effect.orDie),
			),
		);
		const text = yield* ctx
			.run(action, {
				...(options?.retry === undefined ? {} : { retry: options.retry }),
				name,
			})
			.pipe(
				Effect.catch((failure) =>
					failFromTerminal(options?.error, failure).pipe(
						Effect.catch((error) =>
							error instanceof TerminalFailure
								? Effect.die(error)
								: Effect.fail(error),
						),
					),
				),
			);
		return yield* decodeValue(options?.output, text).pipe(Effect.orDie);
	});
}

export const sleep = (duration: Duration.Input, name?: string) =>
	Effect.flatMap(RestateContext, (ctx) =>
		ctx.sleep(Duration.fromInputUnsafe(duration), name),
	);

export const random = Effect.flatMap(RestateContext, (ctx) => ctx.random());
export const uuid = Effect.flatMap(RestateContext, (ctx) => ctx.uuid());
/** Durable `Date.now()`: replays return the journaled value. */
export const now = Effect.flatMap(RestateContext, (ctx) => ctx.now());
export const invocationId = Effect.flatMap(RestateContext, (ctx) =>
	ctx.invocationId(),
);

export const cancel = (id: string) =>
	Effect.flatMap(RestateContext, (ctx) => ctx.cancel(id));

export const objectKey = Effect.flatMap(SharedObjectContext, (ctx) =>
	ctx.key(),
);
export const stateKeys = Effect.flatMap(SharedObjectContext, (ctx) =>
	ctx.stateKeys(),
);
export const clearAllState = Effect.flatMap(ObjectContext, (ctx) =>
	ctx.clearAllState(),
);

/** Stored values that no longer decode are a defect of the deployment. */
export const state = <S extends AnySchema>(name: string, schema: S) => ({
	get: Effect.flatMap(SharedObjectContext, (ctx) => ctx.getState(name)).pipe(
		Effect.flatMap((stored) =>
			Option.isNone(stored)
				? Effect.succeed(Option.none<S["Type"]>())
				: decodeValue(schema, stored.value).pipe(
						Effect.map((value) => Option.some(value as S["Type"])),
						Effect.orDie,
					),
		),
	),
	set: (value: S["Type"]) =>
		encodeValue(schema, value).pipe(
			Effect.orDie,
			Effect.flatMap((text) =>
				Effect.flatMap(ObjectContext, (ctx) => ctx.setState(name, text)),
			),
		),
	clear: Effect.flatMap(ObjectContext, (ctx) => ctx.clearState(name)),
});

export const awakeable = <S extends AnySchema>(schema: S) =>
	Effect.flatMap(RestateContext, (ctx) => ctx.awakeable()).pipe(
		Effect.map((handle) => ({
			id: handle.id,
			await: handle.await.pipe(
				Effect.flatMap((text) =>
					decodeValue(schema, text).pipe(
						Effect.map((value) => value as S["Type"]),
						Effect.orDie,
					),
				),
			),
		})),
	);

export const resolveAwakeable = <S extends AnySchema>(
	id: string,
	schema: S,
	value: S["Type"],
) =>
	encodeValue(schema, value).pipe(
		Effect.orDie,
		Effect.flatMap((text) =>
			Effect.flatMap(RestateContext, (ctx) => ctx.resolveAwakeable(id, text)),
		),
	);

export const rejectAwakeable = (id: string, reason: string) =>
	Effect.flatMap(RestateContext, (ctx) => ctx.rejectAwakeable(id, reason));

/**
 * Durable promise of the current workflow. `peek` dies if the promise was
 * rejected.
 */
export const promise = <S extends AnySchema>(name: string, schema: S) => {
	const decode = (text: string) =>
		decodeValue(schema, text).pipe(
			Effect.map((value) => value as S["Type"]),
			Effect.orDie,
		);
	return {
		await: Effect.flatMap(WorkflowContext, (ctx) => ctx.promiseGet(name)).pipe(
			Effect.flatMap(decode),
		),
		peek: Effect.flatMap(WorkflowContext, (ctx) => ctx.promisePeek(name)).pipe(
			Effect.orDie,
			Effect.flatMap((stored) =>
				Option.isNone(stored)
					? Effect.succeed(Option.none<S["Type"]>())
					: decode(stored.value).pipe(Effect.map(Option.some)),
			),
		),
		resolve: (value: S["Type"]) =>
			encodeValue(schema, value).pipe(
				Effect.orDie,
				Effect.flatMap((text) =>
					Effect.flatMap(WorkflowContext, (ctx) =>
						ctx.promiseResolve(name, text),
					),
				),
			),
		reject: (reason: string) =>
			Effect.flatMap(WorkflowContext, (ctx) => ctx.promiseReject(name, reason)),
	};
};

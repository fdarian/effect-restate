import * as http2 from "node:http2";
import type { AddressInfo } from "node:net";
import type * as restate from "@restatedev/restate-sdk";
import {
	Cause,
	Config,
	Context,
	Duration,
	Effect,
	Exit,
	FiberSet,
	Layer,
	Option,
	Result,
	Schema,
} from "effect";
import { FetchHttpClient, HttpClient, HttpClientResponse } from "effect/http";
import {
	type CallTarget,
	ObjectContext,
	type ObjectContextShape,
	RestateContext,
	type RestateContextShape,
	type RetryOptions,
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
} from "../core/restate.js";

type Sdk = typeof restate;
type Clients = typeof import("@restatedev/restate-sdk-clients");
type ClientOpts<I, O> = import("@restatedev/restate-sdk-clients").Opts<I, O>;
type ClientSendOpts<I> = import("@restatedev/restate-sdk-clients").SendOpts<I>;

const DEFAULT_INGRESS_URL = "http://localhost:8080";
const DEFAULT_ADMIN_URL = "http://localhost:9070";
const DEFAULT_PORT = 9080;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/**
 * Hands our JSON text to Restate untouched, so the codec in core owns
 * encoding. It mirrors `serde.json`: same content type, and the empty text is
 * the empty body.
 */
const textSerde: restate.Serde<string> = {
	contentType: "application/json",
	serialize: (text) => textEncoder.encode(text),
	deserialize: (bytes) => textDecoder.decode(bytes),
};

const toMillis = (input: Duration.Input) =>
	Duration.toMillis(Duration.fromInputUnsafe(input));

const toTerminalFailure = (error: restate.TerminalError) =>
	new TerminalFailure({ message: error.message, code: error.code });

const asError = (defect: unknown) =>
	defect instanceof Error ? defect : new Error(String(defect));

/**
 * Per-invocation state. `cancelled` tells an interruption caused by a Restate
 * cancellation from one caused by the attempt ending.
 */
type Invocation = { cancelled: boolean };

/**
 * Awaits a RestatePromise as an Effect. Exactly one `.then` is attached per
 * promise, and only when the effect runs, so ctx operations are issued and
 * awaited one at a time in program order. A `CancelledError` rejection
 * interrupts the fiber: the attempt is still alive, so finalizers may run
 * compensating ctx operations.
 */
const makeAwait =
	(sdk: Sdk, invocation: Invocation) =>
	<A>(start: () => PromiseLike<A>): Effect.Effect<A, TerminalFailure> =>
		Effect.callback<A, TerminalFailure>((resume) => {
			const promise = (() => {
				try {
					return start();
				} catch (defect) {
					resume(Effect.die(defect));
					return undefined;
				}
			})();
			if (promise === undefined) return;
			promise.then(
				(value) => resume(Effect.succeed(value)),
				(error: unknown) => {
					if (error instanceof sdk.CancelledError) {
						invocation.cancelled = true;
						resume(Effect.interrupt);
					} else if (error instanceof sdk.TerminalError) {
						resume(Effect.fail(toTerminalFailure(error)));
					} else {
						resume(Effect.die(error));
					}
				},
			);
		});

/** The error a handler or `ctx.run` closure throws to the SDK for an `Exit`. */
const thrownFor = (
	sdk: Sdk,
	cause: Cause.Cause<TerminalFailure>,
	invocation: Invocation,
) => {
	const failure = Cause.findError(cause);
	if (Result.isSuccess(failure)) {
		return new sdk.TerminalError(failure.success.message, {
			errorCode: failure.success.code,
		});
	}
	const defect = Cause.findDefect(cause);
	if (Result.isSuccess(defect)) {
		if (defect.success instanceof sdk.TerminalError) return defect.success;
		return asError(defect.success);
	}
	return invocation.cancelled
		? new sdk.CancelledError()
		: new Error("The invocation attempt ended before the handler finished");
};

const runOptionsOf = (retry: RetryOptions | undefined) => ({
	serde: textSerde,
	...(retry?.maxAttempts === undefined
		? {}
		: { maxRetryAttempts: retry.maxAttempts }),
	...(retry?.maxDuration === undefined
		? {}
		: { maxRetryDuration: toMillis(retry.maxDuration) }),
	...(retry?.initialInterval === undefined
		? {}
		: { initialRetryInterval: toMillis(retry.initialInterval) }),
	...(retry?.maxInterval === undefined
		? {}
		: { maxRetryInterval: toMillis(retry.maxInterval) }),
	...(retry?.factor === undefined ? {} : { retryIntervalFactor: retry.factor }),
});

const restateContextFor = (
	sdk: Sdk,
	ctx: restate.Context,
	invocation: Invocation,
): RestateContextShape => {
	const wait = makeAwait(sdk, invocation);
	const callTarget = (target: CallTarget) => ({
		service: target.service,
		method: target.handler,
		...(target.key === undefined ? {} : { key: target.key }),
		inputSerde: textSerde,
	});
	return {
		invocationId: () => Effect.sync(() => ctx.request().id),
		run: (action, options) =>
			wait(() =>
				ctx.run(
					options.name,
					async () => {
						const exit = await Effect.runPromiseExit(action, {
							signal: ctx.request().attemptCompletedSignal,
						});
						if (Exit.isSuccess(exit)) return exit.value;
						throw thrownFor(sdk, exit.cause, { cancelled: false });
					},
					runOptionsOf(options.retry),
				),
			),
		sleep: (duration, name) =>
			wait(() => ctx.sleep(Duration.toMillis(duration), name)).pipe(
				Effect.orDie,
			),
		awakeable: () =>
			Effect.sync(() => {
				const awakeable = ctx.awakeable<string>(textSerde);
				return {
					id: awakeable.id,
					await: wait(() => awakeable.promise),
				};
			}),
		resolveAwakeable: (id, value) =>
			Effect.sync(() => ctx.resolveAwakeable(id, value, textSerde)),
		rejectAwakeable: (id, reason) =>
			Effect.sync(() => ctx.rejectAwakeable(id, reason)),
		call: (target, parameter, options) =>
			wait(() =>
				ctx.genericCall<string, string>({
					...callTarget(target),
					outputSerde: textSerde,
					parameter,
					...(options.idempotencyKey === undefined
						? {}
						: { idempotencyKey: options.idempotencyKey }),
				}),
			),
		send: (target, parameter, options) =>
			wait(
				() =>
					ctx.genericSend<string>({
						...callTarget(target),
						parameter,
						...(options.delay === undefined
							? {}
							: { delay: toMillis(options.delay) }),
						...(options.idempotencyKey === undefined
							? {}
							: { idempotencyKey: options.idempotencyKey }),
					}).invocationId,
			).pipe(Effect.orDie),
		cancel: (invocationId) =>
			Effect.sync(() => ctx.invocation(invocationId as never).cancel()),
		random: () => Effect.sync(() => ctx.rand.random()),
		uuid: () => Effect.sync(() => ctx.rand.uuidv4()),
		now: () => wait(() => ctx.date.now()).pipe(Effect.orDie),
	};
};

const sharedObjectContextFor = (
	sdk: Sdk,
	ctx: restate.ObjectSharedContext,
	invocation: Invocation,
): SharedObjectContextShape => {
	const wait = makeAwait(sdk, invocation);
	return {
		key: () => Effect.sync(() => ctx.key),
		getState: (name) =>
			wait(() => ctx.get<string>(name, textSerde)).pipe(
				Effect.map(Option.fromNullishOr),
				Effect.orDie,
			),
		stateKeys: () => wait(() => ctx.stateKeys()).pipe(Effect.orDie),
	};
};

const objectContextFor = (ctx: restate.ObjectContext): ObjectContextShape => ({
	setState: (name, value) => Effect.sync(() => ctx.set(name, value, textSerde)),
	clearState: (name) => Effect.sync(() => ctx.clear(name)),
	clearAllState: () => Effect.sync(() => ctx.clearAll()),
});

const workflowContextFor = (
	sdk: Sdk,
	ctx: restate.WorkflowSharedContext,
	invocation: Invocation,
): WorkflowContextShape => {
	const wait = makeAwait(sdk, invocation);
	return {
		promiseGet: (name) =>
			wait(() => ctx.promise<string>(name, textSerde).get()),
		promisePeek: (name) =>
			wait(() => ctx.promise<string>(name, textSerde).peek()).pipe(
				Effect.map(Option.fromUndefinedOr),
			),
		promiseResolve: (name, value) =>
			wait(() => ctx.promise<string>(name, textSerde).resolve(value)),
		promiseReject: (name, reason) =>
			wait(() => ctx.promise<string>(name, textSerde).reject(reason)),
	};
};

/** Only the context services the handler's kind provides are included. */
const contextsFor = (
	sdk: Sdk,
	definition: AnyDefinition,
	handler: AnyHandler,
	handlerName: string,
	ctx: restate.Context,
	invocation: Invocation,
) => {
	const base = Context.make(
		RestateContext,
		restateContextFor(sdk, ctx, invocation),
	);
	if (definition.kind === "service") return base;
	const withShared = base.pipe(
		Context.add(
			SharedObjectContext,
			sharedObjectContextFor(
				sdk,
				ctx as restate.ObjectSharedContext,
				invocation,
			),
		),
	);
	const exclusive =
		definition.kind === "workflow"
			? handlerName === "run"
			: handler.kind === "handler";
	const withState = exclusive
		? withShared.pipe(
				Context.add(
					ObjectContext,
					objectContextFor(ctx as restate.ObjectContext),
				),
			)
		: withShared;
	return definition.kind === "workflow"
		? withState.pipe(
				Context.add(
					WorkflowContext,
					workflowContextFor(
						sdk,
						ctx as restate.WorkflowSharedContext,
						invocation,
					),
				),
			)
		: withState;
};

type RunEffect = <A>(
	effect: Effect.Effect<A>,
	options: { readonly signal: AbortSignal },
) => Promise<A>;

const sdkHandlerFor = (
	sdk: Sdk,
	run: RunEffect,
	definition: AnyDefinition,
	handlerName: string,
	handler: AnyHandler,
) => {
	const invoke = async (ctx: restate.Context, input: string) => {
		const invocation: Invocation = { cancelled: false };
		const contexts = contextsFor(
			sdk,
			definition,
			handler,
			handlerName,
			ctx,
			invocation,
		);
		const exit = await run(
			Effect.exit(
				executeHandler(handler, input).pipe(Effect.provideContext(contexts)),
			),
			{ signal: ctx.request().attemptCompletedSignal },
		);
		if (Exit.isSuccess(exit)) return exit.value;
		throw thrownFor(sdk, exit.cause, invocation);
	};
	const options = {
		...handler.options,
		input: textSerde,
		output: textSerde,
		serde: textSerde,
	};
	// The SDK's handler constructors type the context per kind; `invoke`
	// narrows it itself, so the boundary is cast.
	const fn = invoke as never;
	if (definition.kind === "service") {
		return sdk.createServiceHandler(options, fn);
	}
	if (definition.kind === "object") {
		return handler.kind === "handler"
			? sdk.createObjectHandler(options, fn)
			: sdk.createObjectSharedHandler(options, fn);
	}
	return handlerName === "run"
		? sdk.createWorkflowHandler(options, fn)
		: sdk.createWorkflowSharedHandler(options, fn);
};

const sdkDefinitionFor = (
	sdk: Sdk,
	run: RunEffect,
	definition: AnyDefinition,
) => {
	const handlers = Object.fromEntries(
		Object.entries(definition.handlers).map(([name, handler]) => [
			name,
			sdkHandlerFor(sdk, run, definition, name, handler),
		]),
	);
	const spec = { name: definition.name, handlers } as never;
	if (definition.kind === "service") return sdk.service(spec);
	return definition.kind === "object" ? sdk.object(spec) : sdk.workflow(spec);
};

const terminalBody = Schema.fromJsonString(
	Schema.Struct({
		message: Schema.String,
		code: Schema.optional(Schema.Number),
		source: Schema.optional(Schema.String),
	}),
);

/**
 * A terminal failure of the invocation comes back as an HTTP error carrying
 * `{ code, message, source: "invocation" }`. The server's own errors (unknown
 * service, malformed ids) carry `source: "ingress"` and are infrastructure
 * problems instead.
 */
const toIngressFailure = (clients: Clients, cause: unknown) => {
	if (cause instanceof clients.HttpCallError) {
		const body = Schema.decodeUnknownOption(terminalBody)(cause.responseText);
		if (Option.isSome(body) && body.value.source === "invocation") {
			return new TerminalFailure({
				message: body.value.message,
				code: body.value.code ?? cause.status,
			});
		}
	}
	return new IngressError({ cause });
};

type WorkflowIngress = {
	readonly workflowSubmit: (
		parameter: string,
		options: ClientSendOpts<string>,
	) => Promise<{ readonly invocationId: string }>;
	readonly workflowAttach: (
		options: ClientOpts<void, string>,
	) => Promise<string>;
	readonly workflowOutput: (
		options: ClientOpts<void, string>,
	) => Promise<{ readonly ready: boolean; readonly result: string }>;
};

export const make = Effect.gen(function* () {
	const ingressUrl = yield* Config.String("RESTATE_INGRESS_URL").pipe(
		Config.withDefault(DEFAULT_INGRESS_URL),
	);
	const adminUrl = yield* Config.String("RESTATE_ADMIN_URL").pipe(
		Config.withDefault(DEFAULT_ADMIN_URL),
	);
	const identityKeys = yield* Config.String("RESTATE_IDENTITY_KEYS").pipe(
		Config.option,
		Effect.map(
			Option.map((keys) =>
				keys
					.split(",")
					.map((key) => key.trim())
					.filter((key) => key !== ""),
			),
		),
	);
	const defaultPort = yield* Config.Port("PORT").pipe(
		Config.withDefault(DEFAULT_PORT),
	);

	const sdk = yield* Effect.promise(() => import("@restatedev/restate-sdk"));
	const clients = yield* Effect.promise(
		() => import("@restatedev/restate-sdk-clients"),
	);
	const httpClient = yield* HttpClient.HttpClient;

	const connection = clients.connect({ url: ingressUrl });
	const callOpts = (options: { readonly idempotencyKey?: string }) =>
		clients.Opts.from<string, string>({
			input: textSerde,
			output: textSerde,
			...(options.idempotencyKey === undefined
				? {}
				: { idempotencyKey: options.idempotencyKey }),
		});
	const sendOpts = (options: {
		readonly delay?: Duration.Input;
		readonly idempotencyKey?: string;
	}) =>
		clients.SendOpts.from<string>({
			input: textSerde,
			...(options.delay === undefined
				? {}
				: { delay: toMillis(options.delay) }),
			...(options.idempotencyKey === undefined
				? {}
				: { idempotencyKey: options.idempotencyKey }),
		});

	const attempt = <A>(use: () => Promise<A>) =>
		Effect.tryPromise({
			try: use,
			catch: (cause) => toIngressFailure(clients, cause),
		});
	const attemptPlain = <A>(use: () => Promise<A>) =>
		Effect.tryPromise({
			try: use,
			catch: (cause) => new IngressError({ cause }),
		});

	const workflow = (service: string, key: string) =>
		connection.workflowClient(
			{ name: service } as never,
			key,
		) as never as WorkflowIngress;
	const workflowCallOpts = clients.Opts.from<void, string>({
		output: textSerde,
	});

	const ingress: IngressDriver = {
		call: (target, parameter, options) =>
			attempt(() =>
				connection.call<string, string>({
					service: target.service,
					handler: target.handler,
					parameter,
					...(target.key === undefined ? {} : { key: target.key }),
					opts: callOpts(options),
				}),
			),
		send: (target, parameter, options) =>
			attemptPlain(() =>
				connection.send<string>({
					service: target.service,
					handler: target.handler,
					parameter,
					...(target.key === undefined ? {} : { key: target.key }),
					opts: sendOpts(options),
				}),
			).pipe(
				Effect.map((send) => ({
					invocationId: send.invocationId,
					attachable: send.attachable,
				})),
			),
		attach: (invocationId) =>
			attempt(() =>
				connection.result<string>(
					{ invocationId, status: "Accepted", attachable: true },
					textSerde,
				),
			),
		workflowSubmit: (target, parameter, options) =>
			attemptPlain(() =>
				workflow(target.service, target.key).workflowSubmit(
					parameter,
					sendOpts(options),
				),
			).pipe(
				Effect.map((submission) => ({
					invocationId: submission.invocationId,
					attachable: true,
				})),
			),
		workflowAttach: (service, key) =>
			attempt(() => workflow(service, key).workflowAttach(workflowCallOpts)),
		workflowOutput: (service, key) =>
			attempt(() =>
				workflow(service, key).workflowOutput(workflowCallOpts),
			).pipe(
				Effect.map((output) =>
					output.ready ? Option.some(output.result) : Option.none(),
				),
			),
		resolveAwakeable: (id, value) =>
			attemptPlain(() =>
				connection.resolveAwakeable<string>(id, value, textSerde),
			),
		rejectAwakeable: (id, reason) =>
			attemptPlain(() => connection.rejectAwakeable(id, reason)),
		cancel: (invocationId) =>
			httpClient
				.patch(
					`${adminUrl}/invocations/${encodeURIComponent(invocationId)}/cancel`,
				)
				.pipe(
					Effect.flatMap(HttpClientResponse.filterStatusOk),
					Effect.asVoid,
					Effect.mapError((cause) => new IngressError({ cause })),
				),
	};

	const serve: RestateService["serve"] = (definitions, options) =>
		Effect.gen(function* () {
			const run = yield* FiberSet.makeRuntimePromise<never>();
			const services = yield* Effect.try({
				try: () =>
					definitions.map((definition) =>
						sdkDefinitionFor(sdk, run as RunEffect, definition),
					),
				catch: (cause) => new ServeError({ cause }),
			});
			const server = http2.createServer(
				sdk.createEndpointHandler({
					services,
					...(Option.isSome(identityKeys)
						? { identityKeys: identityKeys.value }
						: {}),
				}),
			);
			// Restate keeps its sessions open, which would stall `server.close`.
			const sessions = new Set<http2.ServerHttp2Session>();
			server.on("session", (session) => {
				sessions.add(session);
				session.once("close", () => sessions.delete(session));
			});
			yield* Effect.acquireRelease(
				Effect.callback<void, ServeError>((resume) => {
					server.once("error", (cause) =>
						resume(Effect.fail(new ServeError({ cause }))),
					);
					server.listen(options?.port ?? defaultPort, () =>
						resume(Effect.void),
					);
				}),
				() =>
					Effect.callback<void>((resume) => {
						server.close(() => resume(Effect.void));
						for (const session of sessions) session.destroy();
					}),
			);
			return { port: (server.address() as AddressInfo).port };
		}) as never;

	return { serve, ingress } satisfies RestateService;
});

export const layer = Layer.effect(RestateTag, make).pipe(
	Layer.provide(FetchHttpClient.layer),
);

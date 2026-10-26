import { type Duration, Effect } from "effect";
import { type CallTarget, RestateContext } from "./context.js";
import type {
	AnyObjectDef,
	AnyServiceDef,
	AnyWorkflowDef,
} from "./definition.js";
import type { TerminalFailure } from "./errors.js";
import type { AnyHandler } from "./handler.js";
import { encodeInput, resolveCall } from "./invoke.js";

export type CallOptions = { readonly idempotencyKey?: string };
export type SendOptions = {
	readonly delay?: Duration.Input;
	readonly idempotencyKey?: string;
};

/** Handlers without input take their options as the only argument. */
export type ClientArgs<H extends AnyHandler, Opts> = [H["types"]["I"]] extends [
	never,
]
	? [options?: Opts]
	: [input: H["types"]["I"], options?: Opts];

type HandlerMap = Readonly<Record<string, AnyHandler>>;

type CallFn<H extends AnyHandler, Extra, R> = (
	...args: ClientArgs<H, CallOptions>
) => Effect.Effect<
	H["types"]["O"],
	H["types"]["E"] | TerminalFailure | Extra,
	R
>;

type SendFn<H extends AnyHandler, R> = (
	...args: ClientArgs<H, SendOptions>
) => Effect.Effect<string, never, R>;

export type CallClient<H extends HandlerMap, Extra, R> = {
	readonly [N in keyof H]: CallFn<H[N], Extra, R>;
};

export type SendClient<H extends HandlerMap, R> = {
	readonly [N in keyof H]: SendFn<H[N], R>;
};

export type KeyArgs<D> = D extends AnyObjectDef ? [key: string] : [];

export type WorkflowCallClient<
	H extends AnyWorkflowDef["handlers"],
	Extra,
	R,
> = {
	readonly run: CallFn<H["run"], Extra, R>;
	readonly submit: SendFn<H["run"], R>;
} & CallClient<Omit<H, "run">, Extra, R>;

export const splitArgs = <Opts>(
	handler: AnyHandler,
	args: ReadonlyArray<unknown>,
) =>
	handler.input === undefined
		? { input: undefined, options: args[0] as Opts | undefined }
		: { input: args[0], options: args[1] as Opts | undefined };

export const callOptions = (options: CallOptions | undefined) =>
	options?.idempotencyKey === undefined
		? {}
		: { idempotencyKey: options.idempotencyKey };

export const sendOptions = (options: SendOptions | undefined) => ({
	...(options?.delay === undefined ? {} : { delay: options.delay }),
	...(options?.idempotencyKey === undefined
		? {}
		: { idempotencyKey: options.idempotencyKey }),
});

export const buildMethods = (
	handlers: HandlerMap,
	method: (
		name: string,
		handler: AnyHandler,
	) => (...args: ReadonlyArray<unknown>) => unknown,
) =>
	Object.fromEntries(
		Object.keys(handlers).flatMap((name) => {
			const handler = handlers[name];
			return handler === undefined ? [] : [[name, method(name, handler)]];
		}),
	);

export const requireKey = (
	def: AnyObjectDef | AnyWorkflowDef,
	key: string | undefined,
) => {
	if (key === undefined) {
		throw new Error(`'${def.name}' is keyed: a key is required`);
	}
	return key;
};

const durableCall =
	(target: CallTarget, handler: AnyHandler) =>
	(...args: ReadonlyArray<unknown>) => {
		const call = splitArgs<CallOptions>(handler, args);
		return resolveCall(
			handler,
			encodeInput(handler, call.input).pipe(
				Effect.flatMap((text) =>
					Effect.flatMap(RestateContext, (ctx) =>
						ctx.call(target, text, callOptions(call.options)),
					),
				),
			),
		);
	};

const durableSend =
	(target: CallTarget, handler: AnyHandler) =>
	(...args: ReadonlyArray<unknown>) => {
		const call = splitArgs<SendOptions>(handler, args);
		return encodeInput(handler, call.input).pipe(
			Effect.flatMap((text) =>
				Effect.flatMap(RestateContext, (ctx) =>
					ctx.send(target, text, sendOptions(call.options)),
				),
			),
		);
	};

/** Journaled request-response calls from inside a handler. */
export const client = <D extends AnyServiceDef | AnyObjectDef>(
	def: D,
	...key: KeyArgs<D>
): CallClient<D["handlers"], never, RestateContext> => {
	const objectKey =
		def.kind === "object" ? requireKey(def, key[0] as string) : undefined;
	return buildMethods(def.handlers, (name, handler) =>
		durableCall(
			{
				service: def.name,
				handler: name,
				...(objectKey === undefined ? {} : { key: objectKey }),
			},
			handler,
		),
	) as never;
};

/** Journaled one-way sends from inside a handler; resolves to the invocation id. */
export const sendClient = <D extends AnyServiceDef | AnyObjectDef>(
	def: D,
	...key: KeyArgs<D>
): SendClient<D["handlers"], RestateContext> => {
	const objectKey =
		def.kind === "object" ? requireKey(def, key[0] as string) : undefined;
	return buildMethods(def.handlers, (name, handler) =>
		durableSend(
			{
				service: def.name,
				handler: name,
				...(objectKey === undefined ? {} : { key: objectKey }),
			},
			handler,
		),
	) as never;
};

/** `run` waits for the workflow, `submit` starts it; shared handlers are plain calls. */
export const workflowClient = <D extends AnyWorkflowDef>(
	def: D,
	key: string,
): WorkflowCallClient<D["handlers"], never, RestateContext> => {
	const shared = buildMethods(def.handlers, (name, handler) =>
		durableCall({ service: def.name, handler: name, key }, handler),
	);
	const run = def.handlers.run;
	const target = { service: def.name, handler: "run", key };
	return {
		...shared,
		run: durableCall(target, run),
		submit: durableSend(target, run),
	} as never;
};

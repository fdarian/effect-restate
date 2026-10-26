import { Effect, Option } from "effect";
import {
	buildMethods,
	type CallClient,
	type CallOptions,
	type ClientArgs,
	callOptions,
	type KeyArgs,
	requireKey,
	type SendOptions,
	sendOptions,
	splitArgs,
} from "./client.js";
import { type AnySchema, encodeValue, failFromTerminal } from "./codec.js";
import type { CallTarget } from "./context.js";
import type {
	AnyObjectDef,
	AnyServiceDef,
	AnyWorkflowDef,
} from "./definition.js";
import { type IngressError, NotAttachable, TerminalFailure } from "./errors.js";
import type { AnyHandler, HandlerDef, HandlerKind } from "./handler.js";
import { decodeOutput, encodeInput, resolveCall } from "./invoke.js";
import {
	type IngressDriver,
	type RestateService,
	RestateTag,
} from "./restate.js";

/** Handle to an accepted send; `result` awaits its outcome when attachable. */
export type SendHandle<O, E> = {
	readonly invocationId: string;
	/** Only sends with an idempotency key keep their result, workflows always do. */
	readonly attachable: boolean;
	readonly handler: HandlerDef<HandlerKind, unknown, O, E, unknown>;
};

type IngressSendFn<H extends AnyHandler> = (
	...args: ClientArgs<H, SendOptions>
) => Effect.Effect<
	SendHandle<H["types"]["O"], H["types"]["E"]>,
	IngressError,
	RestateService
>;

type IngressSendClient<H extends Readonly<Record<string, AnyHandler>>> = {
	readonly [N in keyof H]: IngressSendFn<H[N]>;
};

type IngressWorkflowClient<H extends AnyWorkflowDef["handlers"]> = {
	readonly submit: IngressSendFn<H["run"]>;
	readonly attach: Effect.Effect<
		H["run"]["types"]["O"],
		H["run"]["types"]["E"] | TerminalFailure | IngressError,
		RestateService
	>;
	readonly output: Effect.Effect<
		Option.Option<H["run"]["types"]["O"]>,
		H["run"]["types"]["E"] | TerminalFailure | IngressError,
		RestateService
	>;
} & CallClient<Omit<H, "run">, IngressError, RestateService>;

const withIngress = <A, E>(
	use: (ingress: IngressDriver) => Effect.Effect<A, E>,
) => Effect.flatMap(RestateTag, (restate) => use(restate.ingress));

const targetOf = (
	def: AnyServiceDef | AnyObjectDef | AnyWorkflowDef,
	handler: string,
	key: string | undefined,
): CallTarget => ({
	service: def.name,
	handler,
	...(key === undefined ? {} : { key }),
});

const ingressCall =
	(target: CallTarget, handler: AnyHandler) =>
	(...args: ReadonlyArray<unknown>) => {
		const call = splitArgs<CallOptions>(handler, args);
		return resolveCall(
			handler,
			encodeInput(handler, call.input).pipe(
				Effect.flatMap((text) =>
					withIngress((ingress) =>
						ingress.call(target, text, callOptions(call.options)),
					),
				),
			),
		);
	};

const handleOf = (
	handler: AnyHandler,
	receipt: { readonly invocationId: string; readonly attachable: boolean },
): SendHandle<unknown, unknown> => ({
	invocationId: receipt.invocationId,
	attachable: receipt.attachable,
	handler,
});

const ingressSend =
	(target: CallTarget, handler: AnyHandler) =>
	(...args: ReadonlyArray<unknown>) => {
		const call = splitArgs<SendOptions>(handler, args);
		return encodeInput(handler, call.input).pipe(
			Effect.flatMap((text) =>
				withIngress((ingress) =>
					ingress.send(target, text, sendOptions(call.options)),
				),
			),
			Effect.map((receipt) => handleOf(handler, receipt)),
		);
	};

/** Request-response calls from outside any handler. */
export const client = <D extends AnyServiceDef | AnyObjectDef>(
	def: D,
	...key: KeyArgs<D>
): CallClient<D["handlers"], IngressError, RestateService> => {
	const objectKey =
		def.kind === "object" ? requireKey(def, key[0] as string) : undefined;
	return buildMethods(def.handlers, (name, handler) =>
		ingressCall(targetOf(def, name, objectKey), handler),
	) as never;
};

export const sendClient = <D extends AnyServiceDef | AnyObjectDef>(
	def: D,
	...key: KeyArgs<D>
): IngressSendClient<D["handlers"]> => {
	const objectKey =
		def.kind === "object" ? requireKey(def, key[0] as string) : undefined;
	return buildMethods(def.handlers, (name, handler) =>
		ingressSend(targetOf(def, name, objectKey), handler),
	) as never;
};

/** Outcome of a send. Fails with `NotAttachable` unless the send kept its result. */
export const result = <O, E>(
	handle: SendHandle<O, E>,
): Effect.Effect<
	O,
	E | TerminalFailure | IngressError | NotAttachable,
	RestateService
> => {
	if (!handle.attachable) {
		return Effect.fail(
			new NotAttachable({ invocationId: handle.invocationId }),
		);
	}
	return resolveCall(
		handle.handler,
		withIngress((ingress) => ingress.attach(handle.invocationId)),
	) as never;
};

export const workflowClient = <D extends AnyWorkflowDef>(
	def: D,
	key: string,
): IngressWorkflowClient<D["handlers"]> => {
	const run = def.handlers.run;
	const target = targetOf(def, "run", key);
	const shared = buildMethods(def.handlers, (name, handler) =>
		ingressCall(targetOf(def, name, key), handler),
	);
	return {
		...shared,
		submit: (...args: ReadonlyArray<unknown>) => {
			const call = splitArgs<SendOptions>(run, args);
			return encodeInput(run, call.input).pipe(
				Effect.flatMap((text) =>
					withIngress((ingress) =>
						ingress.workflowSubmit(
							{ ...target, key },
							text,
							sendOptions(call.options),
						),
					),
				),
				Effect.map((receipt) => handleOf(run, receipt)),
			);
		},
		attach: resolveCall(
			run,
			withIngress((ingress) => ingress.workflowAttach(def.name, key)),
		),
		output: withIngress((ingress) =>
			ingress.workflowOutput(def.name, key),
		).pipe(
			Effect.catch((error) =>
				error instanceof TerminalFailure
					? failFromTerminal(run.error, error)
					: Effect.fail(error),
			),
			Effect.flatMap((output) =>
				Option.isNone(output)
					? Effect.succeed(output)
					: decodeOutput(run, output.value).pipe(Effect.map(Option.some)),
			),
		),
	} as never;
};

export const resolveAwakeable = <S extends AnySchema>(
	id: string,
	schema: S,
	value: S["Type"],
) =>
	encodeValue(schema, value).pipe(
		Effect.orDie,
		Effect.flatMap((text) =>
			withIngress((ingress) => ingress.resolveAwakeable(id, text)),
		),
	);

export const rejectAwakeable = (id: string, reason: string) =>
	withIngress((ingress) => ingress.rejectAwakeable(id, reason));

/** Cancels an invocation; its caller observes a `TerminalFailure` with code 409. */
export const cancel = (invocationId: string) =>
	withIngress((ingress) => ingress.cancel(invocationId));

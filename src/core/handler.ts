import type * as restate from "@restatedev/restate-sdk";
import type { Effect } from "effect";
import type { AnySchema } from "./codec.js";

export type HandlerKind = "handler" | "shared";

/** SDK handler options passed through by the live implementation only. */
export type HandlerSdkOptions = Omit<
	restate.ObjectHandlerOpts<unknown, unknown>,
	"input" | "output" | "accept" | "serde" | "setOutputContentTypeIfEmpty"
>;

type HandlerOpts<IS, OS, ES> = {
	readonly input?: IS;
	readonly output?: OS;
	readonly error?: ES;
	readonly options?: HandlerSdkOptions;
};

/** `never` marks a handler without input, so clients take no argument. */
type InputOf<IS> = IS extends AnySchema ? IS["Type"] : never;
type OutputOf<OS> = OS extends AnySchema ? OS["Type"] : unknown;
type ErrorOf<ES> = ES extends AnySchema ? ES["Type"] : never;
type FnInput<IS> = IS extends AnySchema ? IS["Type"] : undefined;
type FnError<ES> = ES extends AnySchema ? ES["Type"] : unknown;

/**
 * All type parameters are covariant phantoms: the stored function is untyped,
 * the parameters only carry what clients and `serve` need to know.
 */
export class HandlerDef<out K extends HandlerKind, out I, out O, out E, out R> {
	readonly _tag = "HandlerDef";
	declare readonly types: {
		readonly I: I;
		readonly O: O;
		readonly E: E;
		readonly R: R;
	};

	constructor(
		readonly kind: K,
		readonly input: AnySchema | undefined,
		readonly output: AnySchema | undefined,
		readonly error: AnySchema | undefined,
		readonly options: HandlerSdkOptions | undefined,
		readonly fn: (input: unknown) => Effect.Effect<unknown, unknown, R>,
	) {}
}

export type AnyHandler<K extends HandlerKind = HandlerKind> = HandlerDef<
	K,
	unknown,
	unknown,
	unknown,
	unknown
>;

const make = <K extends HandlerKind>(
	kind: K,
	opts: HandlerOpts<
		AnySchema | undefined,
		AnySchema | undefined,
		AnySchema | undefined
	>,
	fn: (input: never) => Effect.Effect<unknown, unknown, unknown>,
) =>
	new HandlerDef<K, unknown, unknown, unknown, unknown>(
		kind,
		opts.input,
		opts.output,
		opts.error,
		opts.options,
		fn as (input: unknown) => Effect.Effect<unknown, unknown, unknown>,
	);

/**
 * Exclusive handler of an object, `run` handler of a workflow, or plain
 * handler of a service. Without `input` the handler takes no input; without
 * `output` the output is `unknown`, passed through as JSON; without `error`
 * failures reach callers as `TerminalFailure` carrying a message.
 */
export const handler = <
	IS extends AnySchema | undefined = undefined,
	OS extends AnySchema | undefined = undefined,
	ES extends AnySchema | undefined = undefined,
	R = never,
>(
	opts: HandlerOpts<IS, OS, ES>,
	fn: (input: FnInput<IS>) => Effect.Effect<OutputOf<OS>, FnError<ES>, R>,
): HandlerDef<"handler", InputOf<IS>, OutputOf<OS>, ErrorOf<ES>, R> =>
	make("handler", opts, fn as never) as never;

/** Shared handler of an object, or a non-`run` handler of a workflow. */
export const shared = <
	IS extends AnySchema | undefined = undefined,
	OS extends AnySchema | undefined = undefined,
	ES extends AnySchema | undefined = undefined,
	R = never,
>(
	opts: HandlerOpts<IS, OS, ES>,
	fn: (input: FnInput<IS>) => Effect.Effect<OutputOf<OS>, FnError<ES>, R>,
): HandlerDef<"shared", InputOf<IS>, OutputOf<OS>, ErrorOf<ES>, R> =>
	make("shared", opts, fn as never) as never;

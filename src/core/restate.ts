import { Context, type Effect, type Option, type Scope } from "effect";
import type { CallOptions, SendOptions } from "./client.js";
import type { CallTarget } from "./context.js";
import type { AnyDefinition, Requirements } from "./definition.js";
import type { IngressError, ServeError, TerminalFailure } from "./errors.js";

export type ServeOptions = { readonly port?: number };
export type ServeResult = { readonly port: number };

export type SendReceipt = {
	readonly invocationId: string;
	readonly attachable: boolean;
};

/** Ingress operations on JSON text; the typed clients in `ingress.ts` sit on top. */
export type IngressDriver = {
	readonly call: (
		target: CallTarget,
		parameter: string,
		options: CallOptions,
	) => Effect.Effect<string, TerminalFailure | IngressError>;
	readonly send: (
		target: CallTarget,
		parameter: string,
		options: SendOptions,
	) => Effect.Effect<SendReceipt, IngressError>;
	readonly attach: (
		invocationId: string,
	) => Effect.Effect<string, TerminalFailure | IngressError>;
	readonly workflowSubmit: (
		target: CallTarget & { readonly key: string },
		parameter: string,
		options: SendOptions,
	) => Effect.Effect<SendReceipt, IngressError>;
	readonly workflowAttach: (
		service: string,
		key: string,
	) => Effect.Effect<string, TerminalFailure | IngressError>;
	readonly workflowOutput: (
		service: string,
		key: string,
	) => Effect.Effect<Option.Option<string>, TerminalFailure | IngressError>;
	readonly resolveAwakeable: (
		id: string,
		value: string,
	) => Effect.Effect<void, IngressError>;
	readonly rejectAwakeable: (
		id: string,
		reason: string,
	) => Effect.Effect<void, IngressError>;
	readonly cancel: (invocationId: string) => Effect.Effect<void, IngressError>;
};

export type RestateService = {
	/**
	 * Serves the definitions until the scope closes. Handler requirements are
	 * captured here: whatever the handlers need beyond the Restate contexts
	 * must be provided to this effect.
	 */
	readonly serve: <const D extends ReadonlyArray<AnyDefinition>>(
		definitions: D,
		options?: ServeOptions,
	) => Effect.Effect<
		ServeResult,
		ServeError,
		Scope.Scope | Requirements<D[number]>
	>;
	readonly ingress: IngressDriver;
};

export const RestateTag = Context.Service<RestateService>(
	"effect-restate/Restate",
);

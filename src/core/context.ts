import { Context, type Duration, type Effect, type Option } from "effect";
import type { TerminalFailure } from "./errors.js";

/**
 * The context shapes are the seam between the user-facing operations in
 * `ops.ts`/`client.ts` and an implementation. They work on JSON text (the wire
 * format) so schema encoding, error encoding and decoding live once in core
 * and both implementations share them.
 *
 * Every member is a function so a handler's `Restate.run` closure can swap in
 * a stand-in that dies on any use.
 */

export type CallTarget = {
	readonly service: string;
	readonly handler: string;
	readonly key?: string;
};

export type RetryOptions = {
	readonly maxAttempts?: number;
	readonly maxDuration?: Duration.Input;
	readonly initialInterval?: Duration.Input;
	readonly maxInterval?: Duration.Input;
	readonly factor?: number;
};

export type AwakeableHandle = {
	readonly id: string;
	readonly await: Effect.Effect<string, TerminalFailure>;
};

export type RestateContextShape = {
	readonly invocationId: () => Effect.Effect<string>;
	/**
	 * Runs `action` durably. Typed failures of `action` are `TerminalFailure`s
	 * and are journaled; defects are not and may be retried by the runtime.
	 */
	readonly run: (
		action: Effect.Effect<string, TerminalFailure>,
		options: { readonly name: string; readonly retry?: RetryOptions },
	) => Effect.Effect<string, TerminalFailure>;
	readonly sleep: (
		duration: Duration.Duration,
		name?: string,
	) => Effect.Effect<void>;
	readonly awakeable: () => Effect.Effect<AwakeableHandle>;
	readonly resolveAwakeable: (id: string, value: string) => Effect.Effect<void>;
	readonly rejectAwakeable: (id: string, reason: string) => Effect.Effect<void>;
	readonly call: (
		target: CallTarget,
		parameter: string,
		options: { readonly idempotencyKey?: string },
	) => Effect.Effect<string, TerminalFailure>;
	readonly send: (
		target: CallTarget,
		parameter: string,
		options: {
			readonly delay?: Duration.Input;
			readonly idempotencyKey?: string;
		},
	) => Effect.Effect<string>;
	readonly cancel: (invocationId: string) => Effect.Effect<void>;
	readonly random: () => Effect.Effect<number>;
	readonly uuid: () => Effect.Effect<string>;
	readonly now: () => Effect.Effect<number>;
};

export type SharedObjectContextShape = {
	readonly key: () => Effect.Effect<string>;
	readonly getState: (name: string) => Effect.Effect<Option.Option<string>>;
	readonly stateKeys: () => Effect.Effect<ReadonlyArray<string>>;
};

export type ObjectContextShape = {
	readonly setState: (name: string, value: string) => Effect.Effect<void>;
	readonly clearState: (name: string) => Effect.Effect<void>;
	readonly clearAllState: () => Effect.Effect<void>;
};

export type WorkflowContextShape = {
	readonly promiseGet: (name: string) => Effect.Effect<string, TerminalFailure>;
	readonly promisePeek: (
		name: string,
	) => Effect.Effect<Option.Option<string>, TerminalFailure>;
	readonly promiseResolve: (
		name: string,
		value: string,
	) => Effect.Effect<void, TerminalFailure>;
	readonly promiseReject: (
		name: string,
		reason: string,
	) => Effect.Effect<void, TerminalFailure>;
};

export class RestateContext extends Context.Service<
	RestateContext,
	RestateContextShape
>()("effect-restate/RestateContext") {}

export class SharedObjectContext extends Context.Service<
	SharedObjectContext,
	SharedObjectContextShape
>()("effect-restate/SharedObjectContext") {}

export class ObjectContext extends Context.Service<
	ObjectContext,
	ObjectContextShape
>()("effect-restate/ObjectContext") {}

export class WorkflowContext extends Context.Service<
	WorkflowContext,
	WorkflowContextShape
>()("effect-restate/WorkflowContext") {}

export type ContextTag =
	| RestateContext
	| SharedObjectContext
	| ObjectContext
	| WorkflowContext;

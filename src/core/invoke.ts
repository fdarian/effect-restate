import { Effect } from "effect";
import {
	decodeValue,
	encodeFailure,
	encodeValue,
	failFromTerminal,
} from "./codec.js";
import { TerminalFailure } from "./errors.js";
import type { AnyHandler } from "./handler.js";

/**
 * Schema-aware halves of calling and serving a handler. Everything here works
 * on JSON text, which is what both implementations move across their boundary.
 */

/** Encoding the caller's own input only fails when it breaks its types. */
export const encodeInput = (handler: AnyHandler, input: unknown) =>
	handler.input === undefined
		? Effect.succeed("")
		: encodeValue(handler.input, input).pipe(Effect.orDie);

export const decodeOutput = (handler: AnyHandler, text: string) =>
	decodeValue(handler.output, text).pipe(Effect.orDie);

/** Turns the text result of a call into the handler's typed result. */
export const resolveCall = <X, R>(
	handler: AnyHandler,
	result: Effect.Effect<string, TerminalFailure | X, R>,
): Effect.Effect<unknown, unknown, R> =>
	result.pipe(
		Effect.catch((error) =>
			error instanceof TerminalFailure
				? failFromTerminal(handler.error, error)
				: Effect.fail(error),
		),
		Effect.flatMap((text) => decodeOutput(handler, text)),
	);

/**
 * Runs a handler on the wire format: input text in, output text or
 * `TerminalFailure` out. Typed failures become terminal failures; defects and
 * interruption pass through for the implementation to map. The handler's
 * services are resolved from the fiber's context, so the caller provides them.
 */
export const executeHandler = (
	handler: AnyHandler,
	inputText: string,
): Effect.Effect<string, TerminalFailure> => {
	const input =
		handler.input === undefined
			? Effect.void
			: decodeValue(handler.input, inputText).pipe(
					Effect.mapError(
						(error) =>
							new TerminalFailure({
								message: `Failed to deserialize input: ${error.message}`,
								code: 400,
							}),
					),
				);
	return input.pipe(
		Effect.flatMap(
			(decoded) =>
				handler.fn(decoded) as Effect.Effect<unknown, unknown, never>,
		),
		Effect.catch((error) =>
			encodeFailure(handler.error, error).pipe(
				Effect.orDie,
				Effect.flatMap(Effect.fail),
			),
		),
		Effect.flatMap((output) =>
			encodeValue(handler.output, output).pipe(Effect.orDie),
		),
	);
};

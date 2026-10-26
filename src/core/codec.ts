import { Effect, Option, Schema } from "effect";
import { TerminalFailure } from "./errors.js";

export type AnySchema = Schema.Codec<unknown, unknown>;

const ERROR_MARKER = "effect-restate/error";

const jsonValueCodecs = new WeakMap<AnySchema, Schema.Codec<unknown>>();
const jsonCodecs = new WeakMap<AnySchema, Schema.Codec<unknown, string>>();
const errorEnvelopes = new WeakMap<AnySchema, Schema.Codec<unknown, string>>();

const jsonValueCodec = (schema: AnySchema) => {
	const cached = jsonValueCodecs.get(schema);
	if (cached !== undefined) return cached;
	const codec: Schema.Codec<unknown> = Schema.toCodecJson(schema);
	jsonValueCodecs.set(schema, codec);
	return codec;
};

const jsonCodec = (schema: AnySchema) => {
	const cached = jsonCodecs.get(schema);
	if (cached !== undefined) return cached;
	const codec: Schema.Codec<unknown, string> = Schema.fromJsonString(
		Schema.toCodecJson(schema),
	);
	jsonCodecs.set(schema, codec);
	return codec;
};

const errorEnvelope = (schema: AnySchema) => {
	const cached = errorEnvelopes.get(schema);
	if (cached !== undefined) return cached;
	const envelope: Schema.Codec<unknown, string> = Schema.fromJsonString(
		Schema.Struct({ [ERROR_MARKER]: Schema.toCodecJson(schema) }),
	);
	errorEnvelopes.set(schema, envelope);
	return envelope;
};

const unknownJson = Schema.fromJsonString(Schema.Unknown);
const markedPayload = Schema.fromJsonString(
	Schema.Struct({ [ERROR_MARKER]: Schema.Unknown }),
);

/** `undefined` is the empty body, like the SDK's JSON serde. */
export const encodeValue = (
	schema: AnySchema | undefined,
	value: unknown,
): Effect.Effect<string, Schema.SchemaError> => {
	if (value === undefined) return Effect.succeed("");
	return Schema.encodeUnknownEffect(
		schema === undefined ? unknownJson : jsonCodec(schema),
	)(value);
};

export const decodeValue = (
	schema: AnySchema | undefined,
	text: string,
): Effect.Effect<unknown, Schema.SchemaError> => {
	if (text === "") {
		return schema === undefined
			? Effect.void
			: Schema.decodeUnknownEffect(schema)(undefined);
	}
	return Schema.decodeUnknownEffect(
		schema === undefined ? unknownJson : jsonCodec(schema),
	)(text);
};

const messageOf = (error: unknown) => {
	if (typeof error === "object" && error !== null) {
		if ("message" in error && typeof error.message === "string") {
			return error.message;
		}
		if ("_tag" in error && typeof error._tag === "string") return error._tag;
	}
	return String(error);
};

/**
 * Turns a typed failure into the terminal failure that crosses the wire. With
 * an `error` schema the encoded error rides in the message under a marker so
 * the caller can decode it back. A `TerminalFailure` raised by the handler
 * passes through as is, keeping its code.
 */
export const encodeFailure = (
	schema: AnySchema | undefined,
	error: unknown,
): Effect.Effect<TerminalFailure, Schema.SchemaError> => {
	if (error instanceof TerminalFailure) return Effect.succeed(error);
	if (schema === undefined) {
		return Effect.succeed(
			new TerminalFailure({ message: messageOf(error), code: 500 }),
		);
	}
	return Schema.encodeUnknownEffect(errorEnvelope(schema))({
		[ERROR_MARKER]: error,
	}).pipe(Effect.map((message) => new TerminalFailure({ message, code: 500 })));
};

/**
 * Fails with the typed error carried by `failure` when the target declares an
 * `error` schema and the failure carries the marker; otherwise with the
 * `TerminalFailure` itself.
 */
export const failFromTerminal = <E>(
	schema: AnySchema | undefined,
	failure: TerminalFailure,
): Effect.Effect<never, E | TerminalFailure> => {
	if (schema === undefined) return Effect.fail(failure);
	const payload = Schema.decodeUnknownOption(markedPayload)(failure.message);
	if (Option.isNone(payload)) return Effect.fail(failure);
	return Schema.decodeUnknownEffect(jsonValueCodec(schema))(
		payload.value[ERROR_MARKER],
	).pipe(
		Effect.matchEffect({
			onFailure: (error) =>
				Effect.fail(
					new TerminalFailure({
						message: `Failed to decode the error of a failed invocation: ${error.message}`,
						code: failure.code,
					}),
				),
			onSuccess: (decoded) => Effect.fail(decoded as E),
		}),
	);
};

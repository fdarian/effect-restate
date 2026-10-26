import { Schema } from "effect";

export class TerminalFailure extends Schema.TaggedError<TerminalFailure>()(
	"TerminalFailure",
	{ message: Schema.String, code: Schema.Number },
) {}

export class IngressError extends Schema.TaggedError<IngressError>()(
	"IngressError",
	{ cause: Schema.Defect() },
) {}

export class ServeError extends Schema.TaggedError<ServeError>()("ServeError", {
	cause: Schema.Defect(),
}) {}

export class NotAttachable extends Schema.TaggedError<NotAttachable>()(
	"NotAttachable",
	{ invocationId: Schema.String },
) {}

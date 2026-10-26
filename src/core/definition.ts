import type {
	ObjectContext,
	RestateContext,
	SharedObjectContext,
	WorkflowContext,
} from "./context.js";
import type { AnyHandler, HandlerKind } from "./handler.js";

export type ServiceHandlers = Readonly<Record<string, AnyHandler<"handler">>>;
export type ObjectHandlers = Readonly<Record<string, AnyHandler>>;
export type WorkflowHandlers = Readonly<{ run: AnyHandler<"handler"> }> &
	Readonly<Record<string, AnyHandler>>;

const WORKFLOW_RESERVED = [
	"workflowSubmit",
	"workflowAttach",
	"workflowOutput",
	// Names of the workflow client's own methods.
	"submit",
	"attach",
	"output",
] as const;

export class ServiceDef<out N extends string, out H> {
	readonly kind = "service";
	constructor(
		readonly name: N,
		readonly handlers: H,
	) {}
}

export class ObjectDef<out N extends string, out H> {
	readonly kind = "object";
	constructor(
		readonly name: N,
		readonly handlers: H,
	) {}
}

export class WorkflowDef<out N extends string, out H> {
	readonly kind = "workflow";
	constructor(
		readonly name: N,
		readonly handlers: H,
	) {}
}

export type AnyServiceDef = ServiceDef<string, ServiceHandlers>;
export type AnyObjectDef = ObjectDef<string, ObjectHandlers>;
export type AnyWorkflowDef = WorkflowDef<string, WorkflowHandlers>;
export type AnyDefinition = AnyServiceDef | AnyObjectDef | AnyWorkflowDef;

type ValidWorkflowHandlers<H> = {
	readonly [K in keyof H]: K extends "run"
		? AnyHandler<"handler">
		: K extends (typeof WORKFLOW_RESERVED)[number]
			? never
			: AnyHandler<"shared">;
};

const validate = (
	name: string,
	handlers: Readonly<Record<string, AnyHandler>>,
	allowed: (handlerName: string, kind: HandlerKind) => string | undefined,
) => {
	for (const handlerName of Object.keys(handlers)) {
		const handler = handlers[handlerName];
		if (handler === undefined) continue;
		const problem = allowed(handlerName, handler.kind);
		if (problem !== undefined) {
			throw new Error(`Invalid handler '${name}/${handlerName}': ${problem}`);
		}
	}
};

export const service = <
	const N extends string,
	H extends ServiceHandlers,
>(definition: {
	readonly name: N;
	readonly handlers: H;
}): ServiceDef<N, H> => {
	validate(definition.name, definition.handlers, (_, kind) =>
		kind === "shared" ? "services have no shared handlers" : undefined,
	);
	return new ServiceDef(definition.name, definition.handlers);
};

export const object = <
	const N extends string,
	H extends ObjectHandlers,
>(definition: {
	readonly name: N;
	readonly handlers: H;
}): ObjectDef<N, H> => new ObjectDef(definition.name, definition.handlers);

export const workflow = <
	const N extends string,
	H extends WorkflowHandlers,
>(definition: {
	readonly name: N;
	readonly handlers: H & ValidWorkflowHandlers<H>;
}): WorkflowDef<N, H> => {
	if (definition.handlers.run === undefined) {
		throw new Error(`Workflow '${definition.name}' must have a 'run' handler`);
	}
	validate(definition.name, definition.handlers, (handlerName, kind) => {
		if (handlerName === "run") {
			return kind === "handler" ? undefined : "'run' must not be shared";
		}
		if ((WORKFLOW_RESERVED as ReadonlyArray<string>).includes(handlerName)) {
			return "reserved name";
		}
		return kind === "shared"
			? undefined
			: "workflow handlers other than 'run' must be shared";
	});
	return new WorkflowDef(definition.name, definition.handlers);
};

type ProvidedBy<
	Kind extends string,
	Handler extends HandlerKind,
	Name extends string,
> = Kind extends "service"
	? RestateContext
	: Kind extends "object"
		? Handler extends "handler"
			? RestateContext | SharedObjectContext | ObjectContext
			: RestateContext | SharedObjectContext
		: Name extends "run"
			? RestateContext | SharedObjectContext | ObjectContext | WorkflowContext
			: RestateContext | SharedObjectContext | WorkflowContext;

/**
 * What `serve` still needs from the environment: each handler's requirements
 * minus the context services the runtime provides for that kind of handler.
 * Using a service a kind does not provide (e.g. writing state from a shared
 * handler) therefore surfaces here as an unsatisfied requirement.
 */
export type Requirements<D extends AnyDefinition> = D extends AnyDefinition
	? {
			[N in keyof D["handlers"] & string]: Exclude<
				D["handlers"][N]["types"]["R"],
				ProvidedBy<D["kind"], D["handlers"][N]["kind"], N>
			>;
		}[keyof D["handlers"] & string]
	: never;

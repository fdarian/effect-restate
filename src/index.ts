import { Effect } from "effect";
import * as client from "./core/client.js";
import type { AnyDefinition } from "./core/definition.js";
import * as definition from "./core/definition.js";
import * as handlers from "./core/handler.js";
import * as ingress from "./core/ingress.js";
import * as ops from "./core/ops.js";
import {
	type RestateService,
	RestateTag,
	type ServeOptions,
} from "./core/restate.js";
import * as inMemory from "./impl/in-memory.js";

export type { CallOptions, SendOptions } from "./core/client.js";
export {
	ObjectContext,
	RestateContext,
	type RetryOptions,
	SharedObjectContext,
	WorkflowContext,
} from "./core/context.js";
export {
	type AnyDefinition,
	ObjectDef,
	type Requirements,
	ServiceDef,
	WorkflowDef,
} from "./core/definition.js";
export {
	IngressError,
	NotAttachable,
	ServeError,
	TerminalFailure,
} from "./core/errors.js";
export type { HandlerDef, HandlerSdkOptions } from "./core/handler.js";
export type { SendHandle } from "./core/ingress.js";
export type { RunOptions } from "./core/ops.js";
export type { ServeOptions, ServeResult } from "./core/restate.js";

export type Restate = RestateService;

const serve = <const D extends ReadonlyArray<AnyDefinition>>(
	definitions: D,
	options?: ServeOptions,
) =>
	Effect.flatMap(RestateTag, (restate) => restate.serve(definitions, options));

const api = {
	layerInMemory: () => inMemory.layer,
	service: definition.service,
	object: definition.object,
	workflow: definition.workflow,
	handler: handlers.handler,
	shared: handlers.shared,
	serve,
	run: ops.run,
	sleep: ops.sleep,
	state: ops.state,
	stateKeys: ops.stateKeys,
	clearAllState: ops.clearAllState,
	objectKey: ops.objectKey,
	awakeable: ops.awakeable,
	resolveAwakeable: ops.resolveAwakeable,
	rejectAwakeable: ops.rejectAwakeable,
	promise: ops.promise,
	random: ops.random,
	uuid: ops.uuid,
	now: ops.now,
	invocationId: ops.invocationId,
	cancel: ops.cancel,
	client: client.client,
	sendClient: client.sendClient,
	workflowClient: client.workflowClient,
	ingress: {
		client: ingress.client,
		sendClient: ingress.sendClient,
		workflowClient: ingress.workflowClient,
		result: ingress.result,
		resolveAwakeable: ingress.resolveAwakeable,
		rejectAwakeable: ingress.rejectAwakeable,
		cancel: ingress.cancel,
	},
};

/** The service tag, carrying the module's whole API. */
export const Restate = Object.assign(RestateTag, api);

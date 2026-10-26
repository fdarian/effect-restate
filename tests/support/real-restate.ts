import { ConfigProvider, Context, Effect, Layer } from "effect";
import {
	FetchHttpClient,
	HttpClient,
	HttpClientRequest,
	HttpClientResponse,
} from "effect/http";
import { GenericContainer, Wait } from "testcontainers";
import { Restate } from "../../src/index.js";

/** Pinned so the suite does not drift with `latest`. */
const RESTATE_IMAGE = "docker.io/restatedev/restate:1.8.1";
const INGRESS_PORT = 8080;
const ADMIN_PORT = 9070;

class TestRestate extends Context.Service<TestRestate>()("TestRestate", {
	make: Effect.gen(function* () {
		const container = yield* Effect.acquireRelease(
			Effect.promise(() =>
				new GenericContainer(RESTATE_IMAGE)
					.withExposedPorts(INGRESS_PORT, ADMIN_PORT)
					// Lets the server reach the endpoints our `serve` opens on the host.
					.withExtraHosts([
						{ host: "host.docker.internal", ipAddress: "host-gateway" },
					])
					.withWaitStrategy(
						Wait.forAll([
							Wait.forHttp("/restate/health", INGRESS_PORT).forStatusCode(200),
							Wait.forHttp("/health", ADMIN_PORT).forStatusCode(200),
						]),
					)
					.withStartupTimeout(120_000)
					.start(),
			),
			(started) => Effect.promise(() => started.stop()),
		);
		const host = container.getHost();
		return {
			ingressUrl: `http://${host}:${container.getMappedPort(INGRESS_PORT)}`,
			adminUrl: `http://${host}:${container.getMappedPort(ADMIN_PORT)}`,
		};
	}),
}) {
	static readonly layer = Layer.effect(this, this.make);
}

/**
 * Deployment registration is operations work, not part of the library: the
 * live `serve` only opens the endpoint. Every `serve` is followed by
 * registering that endpoint with the server under test.
 */
const registeringServe =
	(
		adminUrl: string,
		live: Restate,
		httpClient: HttpClient.HttpClient,
	): Restate["serve"] =>
	(definitions, options) =>
		live.serve(definitions, { port: 0, ...options }).pipe(
			Effect.tap((served) =>
				httpClient
					.execute(
						HttpClientRequest.post(`${adminUrl}/deployments`).pipe(
							HttpClientRequest.bodyJsonUnsafe({
								uri: `http://host.docker.internal:${served.port}`,
							}),
						),
					)
					.pipe(
						Effect.flatMap(HttpClientResponse.filterStatusOk),
						Effect.orDie,
					),
			),
		) as never;

/**
 * A `Layer<Restate>` backed by a real Restate server running in Docker via
 * testcontainers. Requires a reachable Docker daemon; the container is torn
 * down when the layer's scope closes.
 */
export const layerRealRestate: Layer.Layer<Restate> = Layer.unwrap(
	Effect.gen(function* () {
		const server = yield* TestRestate;
		const configProvider = ConfigProvider.fromUnknown({
			RESTATE_INGRESS_URL: server.ingressUrl,
			RESTATE_ADMIN_URL: server.adminUrl,
		});
		const live = Restate.layer().pipe(
			Layer.provide(ConfigProvider.layer(configProvider)),
			Layer.orDie,
		);
		return Layer.effect(
			Restate,
			Effect.gen(function* () {
				const restate = yield* Restate;
				const httpClient = yield* HttpClient.HttpClient;
				return {
					...restate,
					serve: registeringServe(server.adminUrl, restate, httpClient),
				};
			}),
		).pipe(Layer.provide(live), Layer.provide(FetchHttpClient.layer));
	}),
).pipe(Layer.provide(TestRestate.layer));

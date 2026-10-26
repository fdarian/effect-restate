import { describe, it } from "@effect/vitest";
import { registerSharedRestateTests } from "./shared-suite.js";
import { layerRealRestate } from "./support/real-restate.js";

// Opt-in: `pnpm test:real` sets RESTATE_TEST_REAL=1. Wrapping the whole
// it.layer(...) block in describe.skipIf keeps a plain `pnpm test` run from
// ever touching Docker, since vitest does not run a skipped suite's hooks.
describe.skipIf(process.env.RESTATE_TEST_REAL !== "1")(
	"Restate (real server)",
	() => {
		it.layer(layerRealRestate, { timeout: 180_000 })((it) => {
			registerSharedRestateTests(it);
		});
	},
);

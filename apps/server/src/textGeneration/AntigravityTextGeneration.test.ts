// @effect-diagnostics nodeBuiltinImport:off
import { it } from "@effect/vitest";
import { AntigravitySettings, ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect } from "vite-plus/test";

import { makeAntigravityTextGeneration } from "./AntigravityTextGeneration.ts";

const decodeAntigravitySettings = Schema.decodeSync(AntigravitySettings);

async function makeFakeAgy(baseDir: string, body: string): Promise<string> {
  const binaryPath = NodePath.join(baseDir, "agy");
  await NodeFSP.writeFile(binaryPath, body, "utf8");
  await NodeFSP.chmod(binaryPath, 0o755);
  return binaryPath;
}

function threadTitleInput(model = "gemini-3.7-flash-low") {
  return {
    cwd: process.cwd(),
    message: "Implement Antigravity support",
    attachments: [],
    modelSelection: {
      instanceId: ProviderInstanceId.make("antigravity"),
      model,
    },
  };
}

describe("AntigravityTextGeneration", () => {
  it.effect("extracts structured JSON from fake agy --print output", () =>
    Effect.gen(function* () {
      const baseDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "antig-text-")),
      );
      try {
        const binaryPath = yield* Effect.promise(() =>
          makeFakeAgy(
            baseDir,
            `#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' 'Here is JSON: {"title":"Antigravity support"}'
`,
          ),
        );
        const settings = decodeAntigravitySettings({
          binaryPath,
          brainPath: NodePath.join(baseDir, "brain"),
        });
        const textGeneration = yield* makeAntigravityTextGeneration(settings);

        const result = yield* textGeneration.generateThreadTitle(threadTitleInput());

        expect(result.title).toBe("Antigravity support");
      } finally {
        yield* Effect.promise(() => NodeFSP.rm(baseDir, { recursive: true, force: true }));
      }
    }),
  );

  it.effect("extracts JSON from CLI output split across writes", () =>
    Effect.gen(function* () {
      const baseDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "antig-text-partial-")),
      );
      try {
        const binaryPath = yield* Effect.promise(() =>
          makeFakeAgy(
            baseDir,
            `#!/usr/bin/env bash
set -euo pipefail
printf '%s' '{"title":"Partial'
sleep 0.1
printf '%s\\n' ' title"}'
`,
          ),
        );
        const settings = decodeAntigravitySettings({
          binaryPath,
          brainPath: NodePath.join(baseDir, "brain"),
        });
        const textGeneration = yield* makeAntigravityTextGeneration(settings);

        const result = yield* textGeneration.generateThreadTitle(threadTitleInput());

        expect(result.title).toBe("Partial title");
      } finally {
        yield* Effect.promise(() => NodeFSP.rm(baseDir, { recursive: true, force: true }));
      }
    }),
  );

  it.effect("returns TextGenerationError when CLI returns invalid JSON", () =>
    Effect.gen(function* () {
      const baseDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "antig-text-invalid-")),
      );
      try {
        const binaryPath = yield* Effect.promise(() =>
          makeFakeAgy(
            baseDir,
            `#!/usr/bin/env bash
printf '%s\\n' 'not-json'
`,
          ),
        );
        const settings = decodeAntigravitySettings({
          binaryPath,
          brainPath: NodePath.join(baseDir, "brain"),
        });
        const textGeneration = yield* makeAntigravityTextGeneration(settings);

        const result = yield* Effect.exit(textGeneration.generateThreadTitle(threadTitleInput()));

        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure") {
          expect(String(result.cause)).toContain("Antigravity CLI returned invalid JSON");
        }
      } finally {
        yield* Effect.promise(() => NodeFSP.rm(baseDir, { recursive: true, force: true }));
      }
    }),
  );
});

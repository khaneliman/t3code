// @effect-diagnostics nodeBuiltinImport:off
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { AntigravitySettings, ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect } from "vite-plus/test";

import {
  buildAntigravityProviderModels,
  checkAntigravityProviderStatus,
  parseAntigravityLanguageServerCmdline,
  parseAntigravityModelLabel,
  parseAntigravityModelsOutput,
  parseLinuxTcpListenPortsForInodes,
  resolveAntigravityBrainPath,
  resolveAntigravityBinaryPath,
  resolveAntigravityHomePath,
  resolveAntigravityModelId,
  resolveAntigravitySettingsPath,
} from "./AntigravityProvider.ts";

const decodeAntigravitySettings = Schema.decodeSync(AntigravitySettings);

async function makeFakeAgy(script: string): Promise<string> {
  const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "agy-test-"));
  const binaryPath = NodePath.join(dir, "agy");
  await NodeFSP.writeFile(binaryPath, script, "utf8");
  await NodeFSP.chmod(binaryPath, 0o755);
  return binaryPath;
}

describe("AntigravityProvider model helpers", () => {
  it("groups model labels into base models with reasoning options", () => {
    const models = buildAntigravityProviderModels({
      models: [
        { id: "gemini-3.7-flash-medium", label: "Gemini 3.7 Flash (Medium)" },
        { id: "gemini-3.7-flash-high", label: "Gemini 3.7 Flash (High)" },
        { id: "gemini-3.7-flash-low", label: "Gemini 3.7 Flash (Low)" },
        { id: "gemini-3.1-pro-low", label: "Gemini 3.1 Pro (Low)" },
        { id: "gemini-3.1-pro-high", label: "Gemini 3.1 Pro (High)" },
      ],
    });

    expect(models.map((model) => model.name)).toEqual(["Gemini 3.7 Flash", "Gemini 3.1 Pro"]);
    const flash = models.find((model) => model.name === "Gemini 3.7 Flash");
    expect(flash?.slug).toBe("gemini-3.7-flash-medium");
    expect(flash?.capabilities?.optionDescriptors).toEqual([
      {
        id: "reasoningEffort",
        label: "Reasoning",
        type: "select",
        currentValue: "gemini-3.7-flash-medium",
        options: [
          { id: "gemini-3.7-flash-low", label: "Low" },
          { id: "gemini-3.7-flash-medium", label: "Medium", isDefault: true },
          { id: "gemini-3.7-flash-high", label: "High" },
        ],
      },
    ]);
  });

  it("resolves concrete model labels from model and reasoning selections", () => {
    expect(parseAntigravityModelLabel("Gemini 3.5 Flash (High)")).toEqual({
      baseName: "Gemini 3.5 Flash",
      reasoningEffort: "high",
    });
    expect(
      resolveAntigravityModelId({
        instanceId: ProviderInstanceId.make("antigravity"),
        model: "Gemini 3.5 Flash (Medium)",
        options: [{ id: "reasoningEffort", value: "high" }],
      }),
    ).toBe("gemini-3.5-flash-high");
    expect(
      resolveAntigravityModelId({
        instanceId: ProviderInstanceId.make("antigravity"),
        model: "gemini-3.7-flash-medium",
        options: [{ id: "reasoningEffort", value: "gemini-3.7-flash-high" }],
      }),
    ).toBe("gemini-3.7-flash-high");
    expect(
      resolveAntigravityModelId({
        instanceId: ProviderInstanceId.make("antigravity"),
        model: "pro",
      }),
    ).toBe("gemini-3.1-pro-high");
    expect(
      parseAntigravityModelsOutput(
        "\ngemini-3.7-flash-medium\tGemini 3.7 Flash (Medium)\n\nClaude Sonnet\n",
      ),
    ).toEqual([
      { id: "gemini-3.7-flash-medium", label: "Gemini 3.7 Flash (Medium)" },
      { id: "Claude Sonnet", label: "Claude Sonnet" },
    ]);
  });

  it("resolves default Antigravity paths and agy command", () => {
    const settings = decodeAntigravitySettings({});
    expect(resolveAntigravityBinaryPath(settings)).toBe("agy");
    expect(resolveAntigravityHomePath(settings)).toContain(".gemini/antigravity-cli");
    expect(resolveAntigravityBrainPath(settings)).toContain(".gemini/antigravity-cli/brain");
    expect(resolveAntigravitySettingsPath(settings)).toContain(
      ".gemini/antigravity-cli/settings.json",
    );
  });
});

describe("AntigravityProvider daemon discovery helpers", () => {
  it("extracts CSRF token from language_server cmdline", () => {
    expect(
      parseAntigravityLanguageServerCmdline([
        "/opt/antigravity/resources/bin/language_server",
        "--standalone",
        "--csrf_token",
        "token-123",
      ]),
    ).toEqual({ csrfToken: "token-123" });
    expect(parseAntigravityLanguageServerCmdline(["node", "server.js"])).toBeUndefined();
  });

  it("extracts sorted loopback listen ports for matching inodes", () => {
    const tcp = [
      "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode",
      "   0: 0100007F:89CD 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 12345 1 0000000000000000 100 0 0 10 0",
      "   1: 00000000:1F90 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 99999 1 0000000000000000 100 0 0 10 0",
      "   2: 0100007F:A77B 00000000:0000 01 00000000:00000000 00:00000000 00000000  1000        0 67890 1 0000000000000000 100 0 0 10 0",
    ].join("\n");
    expect(parseLinuxTcpListenPortsForInodes(tcp, new Set(["12345", "67890"]))).toEqual([35277]);
  });
});

describe("AntigravityProvider status probe", () => {
  it.effect("returns disabled snapshot without probing CLI", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkAntigravityProviderStatus(
        decodeAntigravitySettings({ enabled: false }),
      ).pipe(Effect.provide(NodeServices.layer));
      expect(snapshot.status).toBe("disabled");
      expect(snapshot.enabled).toBe(false);
    }),
  );

  it.effect("reports missing binary", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkAntigravityProviderStatus(
        decodeAntigravitySettings({ binaryPath: "/tmp/t3-missing-agy" }),
      ).pipe(Effect.provide(NodeServices.layer));
      expect(snapshot.status).toBe("error");
      expect(snapshot.installed).toBe(false);
    }),
  );

  it.effect("reports CLI ready after version and models succeed without daemon", () =>
    Effect.gen(function* () {
      const binaryPath = yield* Effect.promise(() =>
        makeFakeAgy(`#!/usr/bin/env bash
if [ "$1" = "--version" ]; then echo "agy 1.0.14"; exit 0; fi
if [ "$1" = "models" ]; then printf "gemini-3.7-flash-medium\tGemini 3.7 Flash (Medium)\n"; exit 0; fi
if [ "$1" = "agentapi" ]; then echo "daemon missing" >&2; exit 1; fi
exit 2
`),
      );
      const snapshot = yield* checkAntigravityProviderStatus(
        decodeAntigravitySettings({
          binaryPath,
        }),
      ).pipe(Effect.provide(NodeServices.layer));
      expect(snapshot.status).toBe("ready");
      expect(snapshot.installed).toBe(true);
      expect(snapshot.auth.label).toBe("Antigravity CLI");
      expect(snapshot.models.map((model) => model.slug)).toContain("gemini-3.7-flash-medium");
      expect(snapshot.models.map((model) => model.name)).toContain("Gemini 3.7 Flash");
    }),
  );

  it.effect("reports ready when fake daemon probe succeeds", () =>
    Effect.gen(function* () {
      const binaryPath = yield* Effect.promise(() =>
        makeFakeAgy(`#!/usr/bin/env bash
if [ "$1" = "--version" ]; then echo "agy 1.0.14"; exit 0; fi
if [ "$1" = "models" ]; then printf "gemini-3.7-flash-medium\\tGemini 3.7 Flash (Medium)\\ngemini-3.7-flash-low\\tGemini 3.7 Flash (Low)\\n"; exit 0; fi
if [ "$1" = "agentapi" ]; then echo "trajectory not found: __t3_probe__" >&2; exit 1; fi
exit 2
`),
      );
      const snapshot = yield* checkAntigravityProviderStatus(
        decodeAntigravitySettings({
          binaryPath,
          languageServerAddress: "http://127.0.0.1:39999",
        }),
      ).pipe(Effect.provide(NodeServices.layer));
      expect(snapshot.status).toBe("ready");
      expect(snapshot.version).toBe("1.0.14");
      expect(snapshot.auth.status).toBe("authenticated");
    }),
  );
});

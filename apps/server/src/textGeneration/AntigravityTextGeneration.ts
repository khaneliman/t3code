// @effect-diagnostics nodeBuiltinImport:off
import {
  type AntigravitySettings,
  type ModelSelection,
  TextGenerationError,
} from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { extractJsonObject } from "@t3tools/shared/schemaJson";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePerfHooks from "node:perf_hooks";
import * as NodeTimersPromises from "node:timers/promises";

import {
  makeAntigravityEnvironment,
  resolveAntigravityBinaryPath,
  resolveAntigravityModelLabel,
  transcriptPathForConversation,
} from "../provider/Layers/AntigravityProvider.ts";
import { parseAntigravityTranscriptLine } from "../provider/Layers/AntigravityAdapter.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import * as TextGeneration from "./TextGeneration.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
  toJsonSchemaObject,
} from "./TextGenerationUtils.ts";

const ANTIGRAVITY_TEXT_GENERATION_TIMEOUT_MS = 180_000;

function runAgentApi(
  settings: AntigravitySettings,
  args: ReadonlyArray<string>,
  cwd: string,
  env: NodeJS.ProcessEnv,
): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = NodeChildProcess.execFile(
      resolveAntigravityBinaryPath(settings),
      ["agentapi", ...args],
      {
        cwd,
        env,
        timeout: 30_000,
        windowsHide: true,
      },
    );
    let stdout = "";
    let stderr = "";
    child.stdout?.on("data", (chunk) => {
      stdout += String(chunk);
    });
    child.stderr?.on("data", (chunk) => {
      stderr += String(chunk);
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolve(stdout);
      } else {
        reject(new Error(stderr.trim() || stdout.trim() || `agentapi exited with code ${code}`));
      }
    });
  });
}

function tryExtractCompleteJsonObject(text: string): string | undefined {
  const extracted = extractJsonObject(text);
  try {
    JSON.parse(extracted);
    return extracted;
  } catch {
    return undefined;
  }
}

async function waitForStructuredOutput(input: {
  readonly settings: AntigravitySettings;
  readonly conversationId: string;
  readonly startedAtMs: number;
}): Promise<string> {
  const transcriptPath = transcriptPathForConversation(input);
  let offset = 0;
  let carry = "";
  let lastContent = "";

  while (
    NodePerfHooks.performance.now() - input.startedAtMs <
    ANTIGRAVITY_TEXT_GENERATION_TIMEOUT_MS
  ) {
    try {
      const stat = await NodeFSP.stat(transcriptPath);
      if (stat.size < offset) {
        offset = 0;
        carry = "";
      }
      if (stat.size > offset) {
        const handle = await NodeFSP.open(transcriptPath, "r");
        try {
          const buffer = Buffer.alloc(stat.size - offset);
          await handle.read(buffer, 0, buffer.length, offset);
          offset = stat.size;
          const text = carry + buffer.toString("utf8");
          const lines = text.split(/\r?\n/g);
          carry = lines.pop() ?? "";
          for (const line of lines) {
            const record = parseAntigravityTranscriptLine(line);
            if (
              record?.source === "MODEL" &&
              typeof record.content === "string" &&
              record.content.trim().length > 0
            ) {
              lastContent = record.content;
              const json = tryExtractCompleteJsonObject(lastContent);
              if (json) return json;
            }
          }
        } finally {
          await handle.close();
        }
      }
    } catch {
      // Transcript is created asynchronously by the daemon.
    }
    await NodeTimersPromises.setTimeout(750);
  }

  throw new Error(
    lastContent
      ? "Timed out waiting for structured Antigravity output."
      : "Timed out waiting for Antigravity transcript output.",
  );
}

export const makeAntigravityTextGeneration = Effect.fn("makeAntigravityTextGeneration")(function* (
  settings: AntigravitySettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const platform = yield* HostProcessPlatform;
  const runJson = Effect.fn("AntigravityTextGeneration.runJson")(function* <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchema,
    modelSelection,
  }: {
    readonly operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle";
    readonly cwd: string;
    readonly prompt: string;
    readonly outputSchema: S;
    readonly modelSelection: ModelSelection;
  }): Effect.fn.Return<S["Type"], TextGenerationError, S["DecodingServices"]> {
    const schemaJson = JSON.stringify(toJsonSchemaObject(outputSchema));
    const fullPrompt = [
      `<T3_WORKSPACE_CONTEXT>\nCurrent working directory: ${cwd}\nWhen the user refers to "this folder", "here", or the current folder, use this directory.\n</T3_WORKSPACE_CONTEXT>`,
      prompt,
      `Return only a JSON object matching this JSON Schema:\n${schemaJson}`,
    ].join("\n\n");
    const startedAtMs = NodePerfHooks.performance.now();
    const modelLabel = resolveAntigravityModelLabel(modelSelection);
    const env = makeAntigravityEnvironment(settings, environment, platform);
    const stdout = yield* Effect.tryPromise({
      try: () => runAgentApi(settings, ["new-conversation", fullPrompt], cwd, env),
      catch: (cause) =>
        new TextGenerationError({
          operation,
          detail:
            cause instanceof Error ? cause.message : "Failed to start Antigravity text generation.",
          cause,
        }),
    });

    const parsed = yield* Effect.try({
      try: () =>
        JSON.parse(stdout) as { response?: { newConversation?: { conversationId?: string } } },
      catch: (cause) =>
        new TextGenerationError({
          operation,
          detail: "Antigravity agentapi returned invalid JSON.",
          cause,
        }),
    });
    const conversationId = parsed.response?.newConversation?.conversationId;
    if (!conversationId) {
      return yield* new TextGenerationError({
        operation,
        detail: "Antigravity agentapi response did not include a conversation id.",
      });
    }

    const json = yield* Effect.tryPromise({
      try: () => waitForStructuredOutput({ settings, conversationId, startedAtMs }),
      catch: (cause) =>
        new TextGenerationError({
          operation,
          detail: cause instanceof Error ? cause.message : "Failed to read Antigravity output.",
          cause,
        }),
    });
    return yield* Schema.decodeEffect(Schema.fromJsonString(outputSchema))(json).pipe(
      Effect.mapError(
        (cause) =>
          new TextGenerationError({
            operation,
            detail: `Antigravity returned invalid structured output${modelLabel ? ` using ${modelLabel}` : ""}.`,
            cause,
          }),
      ),
    );
  });

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("AntigravityTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
      });

      const generated = yield* runJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("AntigravityTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt(input);
      const generated = yield* runJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        title: sanitizePrTitle(generated.title),
        body: generated.body.trim(),
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("AntigravityTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt(input);
      const generated = yield* runJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return { branch: sanitizeBranchFragment(generated.branch) };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("AntigravityTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt(input);
      const generated = yield* runJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return { title: sanitizeThreadTitle(generated.title) };
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});

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

import {
  makeAntigravityEnvironment,
  resolveAntigravityBinaryPath,
  resolveAntigravityCliModelAlias,
  resolveAntigravityModelLabel,
} from "../provider/Layers/AntigravityProvider.ts";
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
const ANTIGRAVITY_TEXT_GENERATION_PRINT_TIMEOUT = "3m0s";

function runAgyPrint(
  settings: AntigravitySettings,
  args: ReadonlyArray<string>,
  cwd: string,
  env: NodeJS.ProcessEnv,
): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = NodeChildProcess.execFile(resolveAntigravityBinaryPath(settings), [...args], {
      cwd,
      env,
      timeout: ANTIGRAVITY_TEXT_GENERATION_TIMEOUT_MS,
      maxBuffer: 20 * 1024 * 1024,
      windowsHide: true,
    });
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
        reject(new Error(stderr.trim() || stdout.trim() || `agy exited with code ${code}`));
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
    const modelLabel = resolveAntigravityModelLabel(modelSelection);
    const modelAlias = resolveAntigravityCliModelAlias(modelSelection);
    const env = makeAntigravityEnvironment(settings, environment, platform);
    const stdout = yield* Effect.tryPromise({
      try: () =>
        runAgyPrint(
          settings,
          [
            ...(modelAlias ? ["--model", modelAlias] : []),
            "--print-timeout",
            ANTIGRAVITY_TEXT_GENERATION_PRINT_TIMEOUT,
            "--print",
            fullPrompt,
          ],
          cwd,
          env,
        ),
      catch: (cause) =>
        new TextGenerationError({
          operation,
          detail:
            cause instanceof Error ? cause.message : "Failed to start Antigravity text generation.",
          cause,
        }),
    });

    const json = tryExtractCompleteJsonObject(stdout);
    if (!json) {
      return yield* new TextGenerationError({
        operation,
        detail: "Antigravity CLI returned invalid JSON.",
      });
    }
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

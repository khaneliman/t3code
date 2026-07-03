// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalTimers:off
// @effect-diagnostics runEffectInsideEffect:off
import {
  type AntigravitySettings,
  EventId,
  type ProviderApprovalDecision,
  ProviderDriverKind,
  type ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ProviderTurnStartResult,
  type ProviderUserInputAnswers,
  RuntimeItemId,
  RuntimeRequestId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTimers from "node:timers";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import type { AntigravityAdapterShape } from "../Services/AntigravityAdapter.ts";
import {
  type AntigravityDaemonEndpoint,
  antigravityLanguageServerRpc,
  makeAntigravityEnvironment,
  resolveAntigravityBinaryPath,
  resolveAntigravityCliModelAlias,
  resolveAntigravityHomePath,
  resolveAntigravityModelLabel,
  resolveAntigravitySettingsPath,
  transcriptPathForConversation,
} from "./AntigravityProvider.ts";

const PROVIDER = ProviderDriverKind.make("antigravity");
const AGENTAPI_TIMEOUT_MS = 30_000;
const CLI_PRINT_TIMEOUT_MS = 30 * 60 * 1_000;
const TRANSCRIPT_POLL_MS = 500;
const GATE_POLL_MS = 750;
const INTERRUPTED_AGENTAPI_RESULT = "__t3_antigravity_agentapi_interrupted__";
const isProviderAdapterRequestError = Schema.is(ProviderAdapterRequestError);
let nextEventSequence = 0;

const AgentApiNewConversationResponse = Schema.Struct({
  response: Schema.Struct({
    newConversation: Schema.Struct({
      conversationId: Schema.String,
      prompt: Schema.optional(Schema.String),
    }),
  }),
});
const decodeNewConversationResponse = Schema.decodeUnknownSync(AgentApiNewConversationResponse);

export interface AntigravityAdapterLiveOptions {
  readonly instanceId?: ProviderInstanceId;
  readonly environment?: NodeJS.ProcessEnv;
  readonly runAgentApi?: (
    binaryPath: string,
    args: ReadonlyArray<string>,
    options: { readonly cwd: string; readonly env: NodeJS.ProcessEnv },
  ) => Promise<string>;
}

export interface AntigravityTranscriptRecord {
  readonly step_index?: number;
  readonly source?: string;
  readonly type?: string;
  readonly status?: string;
  readonly created_at?: string;
  readonly content?: string;
  readonly error?: string;
  readonly thinking?: string;
  readonly tool_calls?: ReadonlyArray<{
    readonly name?: string;
    readonly args?: Record<string, unknown>;
  }>;
}

type AntigravityToolCall = NonNullable<AntigravityTranscriptRecord["tool_calls"]>[number];
type AntigravityToolLifecycleStatus = "inProgress" | "completed" | "failed";
type AntigravityToolItemType =
  | "command_execution"
  | "file_change"
  | "mcp_tool_call"
  | "dynamic_tool_call"
  | "collab_agent_tool_call"
  | "web_search";

interface AntigravityToolDescriptor {
  readonly itemType: AntigravityToolItemType;
  readonly title: string;
  readonly kind: string;
}

interface SessionContext {
  session: ProviderSession;
  conversationId: string | undefined;
  readonly turns: Array<{ readonly id: TurnId; readonly items: Array<unknown> }>;
  activeTurnId: TurnId | undefined;
  readonly pendingGates: Map<string, PendingAntigravityGate>;
  readonly autoApprovedGates: Set<string>;
  poller: NodeJS.Timeout | undefined;
  gatePoller: NodeJS.Timeout | undefined;
  daemonEndpoint: AntigravityDaemonEndpoint | undefined;
  daemonEndpointResolved: boolean;
  agentApiCancel: (() => void) | undefined;
  pollOffset: number;
  pollCarry: string;
  readonly seenLines: Set<string>;
  readonly toolCallStepIndexes: Set<number>;
  stopped: boolean;
}

interface PendingAntigravityGate {
  readonly requestId: RuntimeRequestId;
  readonly trajectoryId: string;
  readonly stepIndex: number;
  readonly kind: "permission" | "filePermission";
  readonly requestType: "command_execution_approval" | "file_read_approval";
  readonly detail: string;
  readonly absolutePathUri: string | undefined;
}

interface CascadeTrajectoryStep {
  readonly status?: string;
  readonly metadata?: {
    readonly sourceTrajectoryStepInfo?: {
      readonly trajectoryId?: string;
      readonly stepIndex?: number;
    };
  };
  readonly requestedInteraction?: {
    readonly permission?: {
      readonly resource?: { readonly action?: string; readonly target?: string };
    };
    readonly filePermission?: { readonly absolutePathUri?: string };
  };
}

interface CascadeTrajectory {
  readonly trajectoryId?: string;
  readonly steps?: ReadonlyArray<CascadeTrajectoryStep>;
}

interface CascadeTrajectoryResponse {
  readonly trajectory?: CascadeTrajectory;
}

function eventId(prefix: string): EventId {
  nextEventSequence += 1;
  return EventId.make(`${prefix}-${process.pid}-${nextEventSequence}`);
}

function runtimeEventBase(input: {
  readonly threadId: ThreadId;
  readonly instanceId?: ProviderInstanceId | undefined;
  readonly turnId?: TurnId | undefined;
  readonly itemId?: RuntimeItemId | undefined;
  readonly requestId?: RuntimeRequestId | undefined;
  readonly createdAt?: string | undefined;
  readonly method?: string | undefined;
  readonly payload?: unknown;
  readonly rawSource?:
    | "antigravity.transcript"
    | "antigravity.agentapi"
    | "antigravity.cli"
    | undefined;
}): Omit<ProviderRuntimeEvent, "type" | "payload"> {
  return {
    eventId: eventId("antigravity"),
    provider: PROVIDER,
    ...(input.instanceId ? { providerInstanceId: input.instanceId } : {}),
    threadId: input.threadId,
    createdAt: input.createdAt ?? new Date().toISOString(),
    ...(input.turnId ? { turnId: input.turnId } : {}),
    ...(input.itemId ? { itemId: input.itemId } : {}),
    ...(input.requestId ? { requestId: input.requestId } : {}),
    raw: {
      source: input.rawSource ?? "antigravity.transcript",
      ...(input.method ? { method: input.method } : {}),
      payload: input.payload ?? {},
    },
  };
}

const currentTimestamp = Effect.map(DateTime.now, DateTime.formatIso);

function trimText(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function normalizeTranscriptType(value: string | undefined): string {
  return (value ?? "").trim().toUpperCase();
}

function normalizeTranscriptText(value: string): string {
  return value.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
}

function stripTranscriptEnvelope(value: string | undefined): string | undefined {
  const content = trimText(value);
  if (!content) return undefined;

  const lines = normalizeTranscriptText(content).split("\n");
  let index = 0;
  let strippedMetadata = false;
  while (index < lines.length) {
    const trimmed = lines[index]?.trim() ?? "";
    if (/^(Created At|Completed At):\s*\S+/iu.test(trimmed)) {
      strippedMetadata = true;
      index += 1;
      continue;
    }
    if (strippedMetadata && trimmed.length === 0) {
      index += 1;
      continue;
    }
    break;
  }

  const stripped = lines.slice(index).join("\n").trim();
  return stripped.length > 0 ? stripped : undefined;
}

function stripAntigravityToolIndent(value: string): string {
  return value
    .split("\n")
    .map((line) => line.replace(/^\t{1,4}/u, "").trimEnd())
    .join("\n")
    .trim();
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function parseJsonLineRecords(value: string | undefined): ReadonlyArray<Record<string, unknown>> {
  if (!value) return [];
  const parsed: Record<string, unknown>[] = [];
  for (const line of value.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) continue;
    try {
      const record = asRecord(JSON.parse(trimmed) as unknown);
      if (record) parsed.push(record);
    } catch {
      // Antigravity sometimes truncates tool output mid-object. Keep raw text.
    }
  }
  return parsed;
}

function antigravityToolDescriptor(method: string): AntigravityToolDescriptor | undefined {
  if (method.includes("RUN_COMMAND")) {
    return { itemType: "command_execution", title: "Ran command", kind: "execute" };
  }
  if (method.includes("VIEW_FILE")) {
    return { itemType: "dynamic_tool_call", title: "Read file", kind: "read" };
  }
  if (method.includes("GREP_SEARCH")) {
    return { itemType: "dynamic_tool_call", title: "Searched files", kind: "search" };
  }
  if (method.includes("LIST_DIRECTORY") || method.includes("LIST_DIR")) {
    return { itemType: "dynamic_tool_call", title: "Listed directory", kind: "list" };
  }
  if (method.includes("CODE_ACTION")) {
    return { itemType: "file_change", title: "Edited file", kind: "edit" };
  }
  if (method.includes("CHECKPOINT")) {
    return { itemType: "file_change", title: "Checkpoint captured", kind: "checkpoint" };
  }
  if (method.includes("SEARCH_WEB")) {
    return { itemType: "web_search", title: "Web search", kind: "web_search" };
  }
  if (method.includes("READ_URL_CONTENT")) {
    return { itemType: "web_search", title: "Read URL", kind: "read_url" };
  }
  if (method.includes("MCP_TOOL")) {
    return { itemType: "mcp_tool_call", title: "MCP tool", kind: "mcp" };
  }
  if (method.includes("INVOKE_SUBAGENT")) {
    return {
      itemType: "collab_agent_tool_call",
      title: "Subagent",
      kind: "collab_agent",
    };
  }
  if (method.includes("ASK_QUESTION")) {
    return { itemType: "dynamic_tool_call", title: "Asked question", kind: "question" };
  }
  if (method.includes("GENERIC")) {
    return { itemType: "dynamic_tool_call", title: "Tool update", kind: "generic" };
  }
  return undefined;
}

function isIgnoredTranscriptRecord(method: string, source: string): boolean {
  return (
    method.includes("SYSTEM_MESSAGE") ||
    method.includes("USER_INPUT") ||
    method.includes("CONVERSATION_HISTORY") ||
    method.includes("EPHEMERAL_MESSAGE") ||
    (source.length > 0 && source !== "MODEL" && !method.includes("ERROR"))
  );
}

function toolArgs(tool: AntigravityToolCall): Record<string, unknown> {
  return tool.args ?? {};
}

function quotedAntigravityArg(value: unknown): string | undefined {
  const text = trimText(value);
  if (!text) return undefined;
  if (text.length >= 2 && text.startsWith('"') && text.endsWith('"')) {
    return text.slice(1, -1);
  }
  return text;
}

function toolCommand(args: Record<string, unknown>): string | undefined {
  const command =
    quotedAntigravityArg(args.command) ??
    quotedAntigravityArg(args.Command) ??
    quotedAntigravityArg(args.command_line) ??
    quotedAntigravityArg(args.CommandLine);
  return command;
}

function toolPath(args: Record<string, unknown>): string | undefined {
  const target =
    quotedAntigravityArg(args.AbsolutePath) ??
    quotedAntigravityArg(args.TargetFile) ??
    quotedAntigravityArg(args.target_file) ??
    quotedAntigravityArg(args.file_path) ??
    quotedAntigravityArg(args.path);
  return target;
}

function toolDetail(tool: AntigravityToolCall): string | undefined {
  const args = toolArgs(tool);
  return toolCommand(args) ?? toolPath(args) ?? trimText(tool.name);
}

function toolTitle(tool: AntigravityToolCall): string {
  const name = trimText(tool.name);
  if (!name) return "Tool call";
  const normalized = name.toLowerCase().replace(/[\s-]+/g, "_");
  if (normalized === "list_dir" || normalized === "list_directory") return "Listed directory";
  if (normalized === "read_file") return "Read file";
  if (normalized === "write_to_file" || normalized === "write_file") return "Write file";
  if (normalized === "grep_search") return "Searched files";
  if (normalized === "run_command") return "Ran command";
  if (normalized === "view_file") return "Read file";
  return name;
}

function isTerminalResponseRecord(input: {
  readonly method: string;
  readonly status: string;
  readonly record: AntigravityTranscriptRecord;
}): boolean {
  return (
    input.status === "DONE" &&
    input.method.includes("FINAL_RESPONSE") &&
    !(input.record.tool_calls && input.record.tool_calls.length > 0)
  );
}

function lifecycleStatusForRecord(input: {
  readonly status: string;
  readonly rawOutput: Record<string, unknown> | undefined;
}): AntigravityToolLifecycleStatus {
  if (input.status === "RUNNING") return "inProgress";
  if (input.status === "ERROR") return "failed";
  const exitCode = input.rawOutput?.exitCode;
  if (typeof exitCode === "number" && exitCode > 0) return "failed";
  return "completed";
}

function itemEventTypeForStatus(status: AntigravityToolLifecycleStatus) {
  return status === "inProgress" ? "item.updated" : "item.completed";
}

function extractTranscriptFilePath(body: string | undefined): string | undefined {
  if (!body) return undefined;
  const fileUriMatch = /File Path:\s*`?file:\/\/([^`\s]+)`?/iu.exec(body);
  if (fileUriMatch?.[1]) return fileUriMatch[1];
  const targetMatch = /(?:tool to|to):\s*(?<path>\/\S+)/iu.exec(body);
  return trimText(targetMatch?.groups?.path)?.replace(/\.$/u, "");
}

function extractTaskDescription(body: string | undefined): string | undefined {
  if (!body) return undefined;
  return trimText(
    /Task Description:\s*(?<description>.*?)(?:\s+Task logs are available at:|$)/isu.exec(body)
      ?.groups?.description,
  );
}

function extractCommandRawOutput(body: string | undefined): Record<string, unknown> | undefined {
  if (!body) return undefined;
  const cleaned = stripAntigravityToolIndent(body);
  const lines = cleaned.split("\n");
  const output: Record<string, unknown> = {};
  const failed = /command failed with exit code:\s*(?<code>\d+)/iu.exec(cleaned);
  if (failed?.groups?.code) {
    output.exitCode = Number.parseInt(failed.groups.code, 10);
  } else if (/command completed successfully/iu.test(cleaned)) {
    output.exitCode = 0;
  }

  const sectionIndexes: Array<{ readonly key: "stdout" | "stderr"; readonly index: number }> = [];
  for (const [index, line] of lines.entries()) {
    const trimmed = line.trim();
    if (trimmed === "Output:" || trimmed === "Stdout:") {
      sectionIndexes.push({ key: "stdout", index });
    } else if (trimmed === "Stderr:") {
      sectionIndexes.push({ key: "stderr", index });
    }
  }

  for (const [sectionIndex, section] of sectionIndexes.entries()) {
    const next = sectionIndexes[sectionIndex + 1]?.index ?? lines.length;
    const value = lines
      .slice(section.index + 1, next)
      .join("\n")
      .trim();
    if (value.length > 0) {
      output[section.key] = value;
    }
  }

  if (Object.keys(output).length === 0) {
    return cleaned ? { content: cleaned } : undefined;
  }
  return output;
}

function buildToolRawOutput(input: {
  readonly method: string;
  readonly body: string | undefined;
  readonly jsonLines: ReadonlyArray<Record<string, unknown>>;
}): Record<string, unknown> | undefined {
  const { method, body, jsonLines } = input;
  if (method.includes("RUN_COMMAND")) return extractCommandRawOutput(body);
  if (!body) return undefined;

  if (method.includes("GREP_SEARCH")) {
    const paths = new Set(
      jsonLines.map((entry) => trimText(entry.File)).filter((entry): entry is string => !!entry),
    );
    return {
      content: body,
      totalFiles: paths.size,
      ...(jsonLines.length > 0 ? { locations: jsonLines } : {}),
    };
  }

  if (method.includes("LIST_DIRECTORY")) {
    return {
      content: body,
      entries: jsonLines,
    };
  }

  return { content: body };
}

function extractChangedFilesFromToolOutput(input: {
  readonly method: string;
  readonly body: string | undefined;
  readonly jsonLines: ReadonlyArray<Record<string, unknown>>;
  readonly path: string | undefined;
}): ReadonlyArray<{ readonly path: string }> | undefined {
  const changed = new Set<string>();
  if (input.method.includes("CODE_ACTION") || input.method.includes("CHECKPOINT")) {
    if (input.path) changed.add(input.path);
    for (const line of input.body?.split("\n") ?? []) {
      const diffMatch = /^diff --git a\/(?<path>.+?) b\//u.exec(line.trim());
      if (diffMatch?.groups?.path) changed.add(diffMatch.groups.path);
    }
  }
  for (const entry of input.jsonLines) {
    const path = trimText(entry.File) ?? trimText(entry.path);
    if (path && (input.method.includes("CODE_ACTION") || input.method.includes("CHECKPOINT"))) {
      changed.add(path);
    }
  }
  return changed.size > 0 ? [...changed].map((path) => ({ path })) : undefined;
}

function toolDetailForTranscript(input: {
  readonly method: string;
  readonly body: string | undefined;
  readonly jsonLines: ReadonlyArray<Record<string, unknown>>;
  readonly path: string | undefined;
}): string | undefined {
  const { method, body, jsonLines, path } = input;
  if (method.includes("VIEW_FILE") || method.includes("CODE_ACTION")) return path;
  if (method.includes("GREP_SEARCH")) {
    if (body === "No results found") return "No results found";
    const files = new Set(
      jsonLines.map((entry) => trimText(entry.File)).filter((entry): entry is string => !!entry),
    );
    if (files.size > 0) return `${files.size.toLocaleString()} file${files.size === 1 ? "" : "s"}`;
  }
  if (method.includes("LIST_DIRECTORY") && jsonLines.length > 0) {
    return `${jsonLines.length.toLocaleString()} entr${jsonLines.length === 1 ? "y" : "ies"}`;
  }
  if (method.includes("GENERIC")) return extractTaskDescription(body);
  if (method.includes("CHECKPOINT")) {
    return body
      ?.split("\n")
      .find((line) => line.trim().length > 0)
      ?.trim();
  }
  return undefined;
}

function toolDataForTranscript(input: {
  readonly record: AntigravityTranscriptRecord;
  readonly method: string;
  readonly descriptor: AntigravityToolDescriptor;
  readonly body: string | undefined;
}): Record<string, unknown> {
  const jsonLines = parseJsonLineRecords(input.body);
  const path = extractTranscriptFilePath(input.body);
  const rawOutput = buildToolRawOutput({ method: input.method, body: input.body, jsonLines });
  const changes = extractChangedFilesFromToolOutput({
    method: input.method,
    body: input.body,
    jsonLines,
    path,
  });
  return {
    toolCallId: `antigravity-step-${input.record.step_index ?? "x"}`,
    kind: input.descriptor.kind,
    ...(path ? { path } : {}),
    ...(changes ? { changes } : {}),
    ...(rawOutput ? { rawOutput } : {}),
    ...(jsonLines.length > 0 ? { records: jsonLines } : {}),
  };
}

function toolDataForToolCall(input: {
  readonly record: AntigravityTranscriptRecord;
  readonly tool: AntigravityToolCall;
  readonly index: number;
}): Record<string, unknown> {
  const args = toolArgs(input.tool);
  const command = toolCommand(args);
  const path = toolPath(args);
  return {
    toolCallId: `antigravity-tool-${input.record.step_index ?? "x"}-${input.index}`,
    kind:
      trimText(input.tool.name)
        ?.toLowerCase()
        .replace(/[\s-]+/g, "_") ?? "tool",
    rawInput: args,
    ...(command ? { command } : {}),
    ...(path ? { path, changes: [{ path }] } : {}),
  };
}

function sanitizeAntigravityAssistantText(value: string | undefined): string | undefined {
  if (!value) return undefined;
  const kept: string[] = [];
  let droppingPermissionBlock = false;
  for (const line of value.split(/\r?\n/u)) {
    const trimmed = line.trim();
    if (!trimmed) {
      droppingPermissionBlock = false;
      kept.push(line);
      continue;
    }
    if (/^Created At:\s*\S+\s+Completed At:\s*\S+/iu.test(trimmed)) continue;
    if (/^You have read and write access to the following workspace\(s\):$/iu.test(trimmed)) {
      droppingPermissionBlock = true;
      continue;
    }
    if (/^Additionally, your current permission grants\b/iu.test(trimmed)) {
      droppingPermissionBlock = false;
      continue;
    }
    if (droppingPermissionBlock) {
      if (trimmed.startsWith("/") || /^[A-Z]:[\\/]/u.test(trimmed)) continue;
      droppingPermissionBlock = false;
    }
    if (/^(read_file|write_file|command|mcp)\([^)]+\):\s*(?:allowed|denied|ask)$/iu.test(trimmed)) {
      continue;
    }
    if (/^Browser initialized successfully\b/iu.test(trimmed)) continue;
    if (/^Workflow Status:/iu.test(trimmed)) continue;
    if (/^Workflow validation is now active\b/iu.test(trimmed)) continue;
    if (/^Content Priority Mode:/iu.test(trimmed)) continue;
    if (/^No tools to call\.?(?:\s+Waiting\b.*)?$/iu.test(trimmed)) continue;
    kept.push(line);
  }
  const sanitized = kept.join("\n").trim();
  return sanitized.length > 0 ? sanitized : undefined;
}

function isDuplicateConcreteToolRecord(
  record: AntigravityTranscriptRecord,
  priorToolCallStepIndexes: ReadonlySet<number>,
): boolean {
  if (typeof record.step_index !== "number" || !priorToolCallStepIndexes.has(record.step_index)) {
    return false;
  }
  const type = normalizeTranscriptType(record.type);
  return type.includes("LIST_DIRECTORY") || type.includes("LIST_DIR");
}

function agentApiTimeoutMessage(): string {
  return [
    `Antigravity agentapi did not finish within ${AGENTAPI_TIMEOUT_MS / 1_000}s.`,
    "It may be waiting for an external permission prompt in Antigravity.",
    "Open Antigravity to approve or deny the request, then retry this turn.",
  ].join(" ");
}

function agentApiFailureMessage(cause: unknown): string {
  const detail = isProviderAdapterRequestError(cause)
    ? cause.detail
    : cause instanceof Error
      ? cause.message
      : String(cause);
  const message = detail.trim();
  return message.includes("agentapi timed out") ? agentApiTimeoutMessage() : message;
}

export function parseAntigravityTranscriptLine(
  line: string,
): AntigravityTranscriptRecord | undefined {
  const trimmed = line.trim();
  if (!trimmed) return undefined;
  try {
    const parsed = JSON.parse(trimmed) as unknown;
    if (!parsed || typeof parsed !== "object") return undefined;
    return parsed as AntigravityTranscriptRecord;
  } catch {
    return undefined;
  }
}

export function mapAntigravityTranscriptRecordToRuntimeEvents(input: {
  readonly record: AntigravityTranscriptRecord;
  readonly threadId: ThreadId;
  readonly instanceId?: ProviderInstanceId;
  readonly turnId?: TurnId;
  readonly createdAt?: string;
}): ReadonlyArray<ProviderRuntimeEvent> {
  const { record, threadId, instanceId, turnId, createdAt } = input;
  const method = normalizeTranscriptType(record.type) || "TRANSCRIPT";
  const source = normalizeTranscriptType(record.source);
  const status = normalizeTranscriptType(record.status);
  const eventCreatedAt = trimText(record.created_at) ?? createdAt;
  const content = stripTranscriptEnvelope(record.content);
  const error = trimText(record.error) ?? (method.includes("ERROR") ? content : undefined);
  const completesTurn = isTerminalResponseRecord({ method, status, record });
  const events: ProviderRuntimeEvent[] = [];

  if (method.includes("ERROR") && error) {
    events.push({
      ...runtimeEventBase({
        threadId,
        instanceId,
        turnId,
        createdAt: eventCreatedAt,
        method,
        payload: record,
      }),
      type: "runtime.error",
      payload: { message: error, class: "provider_error", detail: record },
    });
    events.push({
      ...runtimeEventBase({
        threadId,
        instanceId,
        turnId,
        createdAt: eventCreatedAt,
        method,
        payload: record,
      }),
      type: "turn.completed",
      payload: { state: "failed", errorMessage: error },
    });
    return events;
  }

  if (isIgnoredTranscriptRecord(method, source)) return events;

  if (record.tool_calls && record.tool_calls.length > 0) {
    for (const [index, tool] of record.tool_calls.entries()) {
      const itemId = RuntimeItemId.make(
        `antigravity-tool-${record.step_index ?? "x"}-${index}-${tool.name ?? "tool"}`,
      );
      events.push({
        ...runtimeEventBase({
          threadId,
          instanceId,
          turnId,
          itemId,
          createdAt: eventCreatedAt,
          method,
          payload: record,
        }),
        type: "item.completed",
        payload: {
          itemType: "dynamic_tool_call",
          status: "completed",
          title: toolTitle(tool),
          ...(toolDetail(tool) ? { detail: toolDetail(tool) } : {}),
          data: toolDataForToolCall({ record, tool, index }),
        },
      });
    }
    return events;
  }

  const toolDescriptor = antigravityToolDescriptor(method);
  if (toolDescriptor) {
    const itemId = RuntimeItemId.make(`antigravity-step-${record.step_index ?? eventId("step")}`);
    const data = toolDataForTranscript({
      record,
      method,
      descriptor: toolDescriptor,
      body: content,
    });
    const rawOutput = asRecord(data.rawOutput);
    const lifecycleStatus = lifecycleStatusForRecord({ status, rawOutput });
    const jsonLines = parseJsonLineRecords(content);
    const detail = toolDetailForTranscript({
      method,
      body: content,
      jsonLines,
      path: trimText(data.path),
    });
    events.push({
      ...runtimeEventBase({
        threadId,
        instanceId,
        turnId,
        itemId,
        createdAt: eventCreatedAt,
        method,
        payload: record,
      }),
      type: itemEventTypeForStatus(lifecycleStatus),
      payload: {
        itemType: toolDescriptor.itemType,
        status: lifecycleStatus,
        title: toolDescriptor.title,
        ...(detail ? { detail } : {}),
        data,
      },
    });
    return events;
  }

  const assistantText = sanitizeAntigravityAssistantText(content);
  if (assistantText) {
    events.push({
      ...runtimeEventBase({
        threadId,
        instanceId,
        turnId,
        createdAt: eventCreatedAt,
        method,
        payload: record,
      }),
      type: "content.delta",
      payload: {
        streamKind: "assistant_text",
        delta: assistantText,
      },
    });
  }

  if (completesTurn) {
    events.push({
      ...runtimeEventBase({
        threadId,
        instanceId,
        turnId,
        createdAt: eventCreatedAt,
        method,
        payload: record,
      }),
      type: "turn.completed",
      payload: { state: "completed" },
    });
  }

  return events;
}

function runAgentApiDefault(
  binaryPath: string,
  args: ReadonlyArray<string>,
  options: { readonly cwd: string; readonly env: NodeJS.ProcessEnv },
  onChild?: (child: ReturnType<typeof NodeChildProcess.execFile>) => void,
): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = NodeChildProcess.execFile(binaryPath, [...args], {
      cwd: options.cwd,
      env: options.env,
      timeout: AGENTAPI_TIMEOUT_MS,
      windowsHide: true,
    });
    onChild?.(child);
    let stdout = "";
    let stderr = "";
    child.stdout?.on("data", (chunk) => {
      stdout += String(chunk);
    });
    child.stderr?.on("data", (chunk) => {
      stderr += String(chunk);
    });
    child.on("error", reject);
    child.on("close", (code, signal) => {
      if (code === 0) {
        resolve(stdout);
        return;
      }
      if (signal === "SIGTERM" || signal === "SIGKILL") {
        reject(new Error(`agentapi timed out after ${AGENTAPI_TIMEOUT_MS}ms`));
        return;
      }
      reject(new Error(stderr.trim() || stdout.trim() || `agentapi exited with code ${code}`));
    });
  });
}

function runAgyPrintDefault(
  binaryPath: string,
  args: ReadonlyArray<string>,
  options: { readonly cwd: string; readonly env: NodeJS.ProcessEnv },
  onChild?: (child: ReturnType<typeof NodeChildProcess.spawn>) => void,
  onStdout?: (chunk: string) => void,
): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = NodeChildProcess.spawn(binaryPath, [...args], {
      cwd: options.cwd,
      env: options.env,
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
    onChild?.(child);
    let stdout = "";
    let stderr = "";
    let timedOut = false;
    const timeout = NodeTimers.setTimeout(() => {
      timedOut = true;
      child.kill("SIGTERM");
      NodeTimers.setTimeout(() => {
        if (child.exitCode === null && !child.killed) child.kill("SIGKILL");
      }, 2_000).unref?.();
    }, CLI_PRINT_TIMEOUT_MS);
    timeout.unref?.();

    child.stdout.on("data", (chunk) => {
      const text = String(chunk);
      stdout += text;
      onStdout?.(text);
    });
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });
    child.on("error", (cause) => {
      NodeTimers.clearTimeout(timeout);
      reject(cause);
    });
    child.on("close", (code) => {
      NodeTimers.clearTimeout(timeout);
      if (code === 0) {
        resolve(stdout);
        return;
      }
      if (timedOut) {
        reject(new Error(`Antigravity CLI print timed out after ${CLI_PRINT_TIMEOUT_MS}ms`));
        return;
      }
      reject(new Error(stderr.trim() || stdout.trim() || `agy exited with code ${code}`));
    });
  });
}

async function ensureAntigravityCliSettings(input: {
  readonly settings: AntigravitySettings;
  readonly cwd: string;
  readonly modelLabel?: string;
}): Promise<void> {
  const { settings, cwd, modelLabel } = input;
  const settingsPath = resolveAntigravitySettingsPath(settings);
  await NodeFSP.mkdir(NodePath.dirname(settingsPath), { recursive: true });
  let parsed: Record<string, unknown> = {};
  try {
    const existing = JSON.parse(await NodeFSP.readFile(settingsPath, "utf8")) as unknown;
    if (existing && typeof existing === "object" && !Array.isArray(existing)) {
      parsed = { ...(existing as Record<string, unknown>) };
    }
  } catch {
    parsed = {};
  }

  const existingTrusted = Array.isArray(parsed.trustedWorkspaces) ? parsed.trustedWorkspaces : [];
  let changed = false;
  if (!existingTrusted.includes(cwd)) {
    parsed.trustedWorkspaces = [
      ...existingTrusted.filter((entry): entry is string => typeof entry === "string"),
      cwd,
    ];
    changed = true;
  }
  if (modelLabel && parsed.model !== modelLabel) {
    parsed.model = modelLabel;
    changed = true;
  }
  if (!changed) return;

  const tempPath = `${settingsPath}.${process.pid}.${Date.now()}.tmp`;
  await NodeFSP.writeFile(tempPath, `${JSON.stringify(parsed, null, 2)}\n`, "utf8");
  await NodeFSP.rename(tempPath, settingsPath);
}

function buildAgyPrintArgs(input: {
  readonly prompt: string;
  readonly conversationId?: string | undefined;
  readonly modelAlias?: string | undefined;
  readonly fullAccess: boolean;
}): ReadonlyArray<string> {
  return [
    ...(input.conversationId ? ["--conversation", input.conversationId] : []),
    ...(input.modelAlias ? ["--model", input.modelAlias] : []),
    ...(input.fullAccess ? ["--dangerously-skip-permissions"] : []),
    "--print-timeout",
    "30m0s",
    "--print",
    input.prompt,
  ];
}

async function readLastConversationIdForCwd(input: {
  readonly settings: AntigravitySettings;
  readonly cwd: string;
}): Promise<string | undefined> {
  const cachePath = NodePath.join(
    resolveAntigravityHomePath(input.settings),
    "cache",
    "last_conversations.json",
  );
  try {
    const parsed = JSON.parse(await NodeFSP.readFile(cachePath, "utf8")) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return undefined;
    const conversationId = (parsed as Record<string, unknown>)[input.cwd];
    return typeof conversationId === "string" && conversationId.trim().length > 0
      ? conversationId.trim()
      : undefined;
  } catch {
    return undefined;
  }
}

async function transcriptSizeForConversation(input: {
  readonly settings: AntigravitySettings;
  readonly conversationId: string;
}): Promise<number> {
  try {
    return (await NodeFSP.stat(transcriptPathForConversation(input))).size;
  } catch {
    return 0;
  }
}

function sendCascadeGateDecision(input: {
  readonly endpoint: AntigravityDaemonEndpoint;
  readonly conversationId: string;
  readonly gate: PendingAntigravityGate;
  readonly decision: ProviderApprovalDecision;
}): Promise<unknown> {
  const { endpoint, conversationId, gate, decision } = input;
  const allow = decision === "accept" || decision === "acceptForSession";
  const scope =
    decision === "acceptForSession" ? "PERMISSION_SCOPE_CONVERSATION" : "PERMISSION_SCOPE_ONCE";
  const decisionPayload =
    gate.kind === "filePermission"
      ? {
          filePermission: {
            allow,
            scope,
            ...(gate.absolutePathUri ? { absolutePathUri: gate.absolutePathUri } : {}),
          },
        }
      : { permission: { allow, scope } };

  return antigravityLanguageServerRpc({
    endpoint,
    method: "HandleCascadeUserInteraction",
    body: {
      cascadeId: conversationId,
      interaction: {
        trajectoryId: gate.trajectoryId,
        stepIndex: gate.stepIndex,
        ...decisionPayload,
      },
    },
  });
}

async function scanForNewPlanFile(cwd: string, turnStartTime: number): Promise<string | undefined> {
  const plansDir = NodePath.join(cwd, ".plans");
  try {
    const stats = await NodeFSP.stat(plansDir);
    if (!stats.isDirectory()) return undefined;
    const files = await NodeFSP.readdir(plansDir);
    let newestFile: { name: string; mtime: number } | undefined;
    for (const file of files) {
      if (!file.endsWith(".md")) continue;
      const filePath = NodePath.join(plansDir, file);
      try {
        const fileStat = await NodeFSP.stat(filePath);
        if (fileStat.mtimeMs >= turnStartTime - 2000) {
          if (!newestFile || fileStat.mtimeMs > newestFile.mtime) {
            newestFile = { name: file, mtime: fileStat.mtimeMs };
          }
        }
      } catch {
        // ignore
      }
    }
    if (newestFile) {
      const filePath = NodePath.join(plansDir, newestFile.name);
      return await NodeFSP.readFile(filePath, "utf8");
    }
  } catch {
    // ignore
  }
  return undefined;
}

function parseResumeCursor(raw: unknown): { readonly conversationId: string } | undefined {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return undefined;
  const conversationId = (raw as { readonly conversationId?: unknown }).conversationId;
  return typeof conversationId === "string" && conversationId.trim().length > 0
    ? { conversationId: conversationId.trim() }
    : undefined;
}

function appendTurnItem(context: SessionContext, turnId: TurnId | undefined, item: unknown): void {
  if (!turnId) return;
  const existing = context.turns.find((turn) => turn.id === turnId);
  if (existing) {
    existing.items.push(item);
    return;
  }
  context.turns.push({ id: turnId, items: [item] });
}

export const makeAntigravityAdapter = Effect.fn("makeAntigravityAdapter")(function* (
  settings: AntigravitySettings,
  options: AntigravityAdapterLiveOptions = {},
): Effect.fn.Return<AntigravityAdapterShape, never, ServerConfig | Crypto.Crypto> {
  const serverConfig = yield* Effect.service(ServerConfig);
  const crypto = yield* Crypto.Crypto;
  const platform = yield* HostProcessPlatform;
  const randomUUIDv4 = crypto.randomUUIDv4.pipe(
    Effect.mapError(
      (cause) =>
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "crypto/randomUUIDv4",
          detail: "Failed to generate Antigravity runtime identifier.",
          cause,
        }),
    ),
  );
  const sessionsRef = yield* Ref.make(new Map<ThreadId, SessionContext>());
  const eventQueue = yield* Queue.unbounded<ProviderRuntimeEvent>();
  const binaryPath = resolveAntigravityBinaryPath(settings);
  const baseEnv = options.environment ?? process.env;

  const emit = (event: ProviderRuntimeEvent): void => {
    Effect.runFork(Queue.offer(eventQueue, event));
  };

  const emitRuntimeWarning = (input: {
    readonly context: SessionContext;
    readonly message: string;
    readonly detail?: unknown;
    readonly method?: string;
  }): void => {
    emit({
      ...runtimeEventBase({
        threadId: input.context.session.threadId,
        ...(options.instanceId ? { instanceId: options.instanceId } : {}),
        ...(input.context.activeTurnId ? { turnId: input.context.activeTurnId } : {}),
        createdAt: input.context.session.updatedAt,
        method: input.method ?? "runtime.warning",
        rawSource: "antigravity.agentapi",
        payload: input.detail ?? {},
      }),
      type: "runtime.warning",
      payload: {
        message: input.message,
        ...(input.detail !== undefined ? { detail: input.detail } : {}),
      },
    });
  };

  const emitGateOpened = (context: SessionContext, gate: PendingAntigravityGate): void => {
    emit({
      ...runtimeEventBase({
        threadId: context.session.threadId,
        ...(options.instanceId ? { instanceId: options.instanceId } : {}),
        ...(context.activeTurnId ? { turnId: context.activeTurnId } : {}),
        requestId: gate.requestId,
        createdAt: context.session.updatedAt,
        method: "antigravity/cascade-permission-opened",
        payload: { trajectoryId: gate.trajectoryId, stepIndex: gate.stepIndex },
      }),
      type: "request.opened",
      payload: { requestType: gate.requestType, detail: gate.detail },
    });
  };

  const emitGateResolved = (
    context: SessionContext,
    gate: PendingAntigravityGate,
    decision: string,
  ): void => {
    emit({
      ...runtimeEventBase({
        threadId: context.session.threadId,
        ...(options.instanceId ? { instanceId: options.instanceId } : {}),
        ...(context.activeTurnId ? { turnId: context.activeTurnId } : {}),
        requestId: gate.requestId,
        createdAt: context.session.updatedAt,
        method: "antigravity/cascade-permission-resolved",
        payload: { trajectoryId: gate.trajectoryId, stepIndex: gate.stepIndex },
      }),
      type: "request.resolved",
      payload: { requestType: gate.requestType, decision },
    });
  };

  const endpointFor = (context: SessionContext): AntigravityDaemonEndpoint | undefined => {
    if (!context.daemonEndpointResolved) {
      context.daemonEndpointResolved = true;
      const env = makeAntigravityEnvironment(settings, baseEnv, platform);
      if (env.ANTIGRAVITY_LS_ADDRESS) {
        context.daemonEndpoint = {
          address: env.ANTIGRAVITY_LS_ADDRESS,
          csrfToken: env.ANTIGRAVITY_CSRF_TOKEN,
        };
      }
    }
    return context.daemonEndpoint;
  };

  const pollGates = async (context: SessionContext): Promise<void> => {
    if (context.stopped || !context.conversationId || !context.activeTurnId) return;
    const endpoint = endpointFor(context);
    if (!endpoint) return;
    const conversationId = context.conversationId;
    const autoApprove = context.session.runtimeMode === "full-access";
    let trajectory: CascadeTrajectory | undefined;
    try {
      const response = (await antigravityLanguageServerRpc({
        endpoint,
        method: "GetCascadeTrajectory",
        body: { cascadeId: conversationId },
      })) as CascadeTrajectoryResponse;
      trajectory = response.trajectory;
    } catch {
      return;
    }

    const seen = new Set<string>();
    const autoApproveQueue: PendingAntigravityGate[] = [];
    for (const [index, step] of (trajectory?.steps ?? []).entries()) {
      const interaction = step.requestedInteraction;
      if (step.status !== "CORTEX_STEP_STATUS_WAITING" || !interaction) continue;
      const info = step.metadata?.sourceTrajectoryStepInfo;
      const trajectoryId = trimText(info?.trajectoryId) ?? trimText(trajectory?.trajectoryId);
      const stepIndex = typeof info?.stepIndex === "number" ? info.stepIndex : index;
      if (!trajectoryId) continue;

      const id = `antigravity-approval:${trajectoryId}:${stepIndex}`;
      seen.add(id);
      if (context.pendingGates.has(id) || context.autoApprovedGates.has(id)) continue;

      const isFile = interaction.filePermission !== undefined;
      const gate: PendingAntigravityGate = {
        requestId: RuntimeRequestId.make(id),
        trajectoryId,
        stepIndex,
        kind: isFile ? "filePermission" : "permission",
        requestType: isFile ? "file_read_approval" : "command_execution_approval",
        detail:
          trimText(interaction.permission?.resource?.target) ??
          trimText(interaction.filePermission?.absolutePathUri) ??
          (isFile ? "Antigravity file access request" : "Antigravity command request"),
        absolutePathUri: isFile ? trimText(interaction.filePermission?.absolutePathUri) : undefined,
      };
      if (autoApprove) {
        context.autoApprovedGates.add(id);
        autoApproveQueue.push(gate);
      } else {
        context.pendingGates.set(id, gate);
        emitGateOpened(context, gate);
      }
    }

    for (const [id, gate] of context.pendingGates) {
      if (!seen.has(id)) {
        context.pendingGates.delete(id);
        emitGateResolved(context, gate, "external");
      }
    }
    for (const id of context.autoApprovedGates) {
      if (!seen.has(id)) context.autoApprovedGates.delete(id);
    }
    for (const gate of autoApproveQueue) {
      try {
        await sendCascadeGateDecision({ endpoint, conversationId, gate, decision: "accept" });
      } catch {
        context.autoApprovedGates.delete(gate.requestId);
        context.pendingGates.set(gate.requestId, gate);
        emitGateOpened(context, gate);
      }
    }
  };

  const startGatePoller = (context: SessionContext): void => {
    if (!context.conversationId || context.gatePoller) return;
    context.gatePoller = NodeTimers.setInterval(() => {
      void pollGates(context);
    }, GATE_POLL_MS);
    void pollGates(context);
  };

  const getContext = (threadId: ThreadId) =>
    Ref.get(sessionsRef).pipe(
      Effect.flatMap((sessions) => {
        const context = sessions.get(threadId);
        if (!context || context.stopped) {
          return Effect.fail(
            new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }),
          );
        }
        return Effect.succeed(context);
      }),
    );

  const reopenTurn = (context: SessionContext): void => {
    const turnId = TurnId.make(`antigravity-turn-${NodeCrypto.randomUUID()}`);
    const updatedAt = new Date().toISOString();
    context.activeTurnId = turnId;
    context.session = {
      ...context.session,
      status: "running",
      activeTurnId: turnId,
      updatedAt,
    };
    context.turns.push({ id: turnId, items: [] });
    emit({
      ...runtimeEventBase({
        threadId: context.session.threadId,
        ...(options.instanceId ? { instanceId: options.instanceId } : {}),
        turnId,
        createdAt: updatedAt,
        method: "turn.resume",
      }),
      type: "turn.started",
      payload: {},
    });
  };

  const pollTranscriptOnce = async (
    context: SessionContext,
  ): Promise<{ readonly emittedContent: boolean; readonly completedTurn: boolean }> => {
    if (!context.conversationId || context.stopped) {
      return { emittedContent: false, completedTurn: false };
    }
    const transcriptPath = transcriptPathForConversation({
      settings,
      conversationId: context.conversationId,
    });
    let emittedContent = false;
    let completedTurn = false;

    try {
      const stat = await NodeFSP.stat(transcriptPath);
      if (stat.size < context.pollOffset) {
        context.pollOffset = 0;
        context.pollCarry = "";
        context.seenLines.clear();
        context.toolCallStepIndexes.clear();
      }
      if (stat.size === context.pollOffset) return { emittedContent, completedTurn };

      const handle = await NodeFSP.open(transcriptPath, "r");
      try {
        const length = stat.size - context.pollOffset;
        const buffer = Buffer.alloc(length);
        await handle.read(buffer, 0, length, context.pollOffset);
        context.pollOffset = stat.size;
        const text = context.pollCarry + buffer.toString("utf8");
        const lines = text.split(/\r?\n/g);
        context.pollCarry = lines.pop() ?? "";

        for (const line of lines) {
          const key = line.trim();
          if (!key || context.seenLines.has(key)) continue;
          context.seenLines.add(key);
          const record = parseAntigravityTranscriptLine(line);
          if (!record) continue;
          if (isDuplicateConcreteToolRecord(record, context.toolCallStepIndexes)) continue;
          if (typeof record.step_index === "number" && record.tool_calls?.length) {
            context.toolCallStepIndexes.add(record.step_index);
          }

          const mapRecord = () =>
            mapAntigravityTranscriptRecordToRuntimeEvents({
              record,
              threadId: context.session.threadId,
              ...(options.instanceId ? { instanceId: options.instanceId } : {}),
              ...(context.activeTurnId ? { turnId: context.activeTurnId } : {}),
              createdAt: context.session.updatedAt,
            });
          let mapped = mapRecord();
          if (mapped.length === 0) continue;
          if (context.activeTurnId === undefined && !context.stopped) {
            reopenTurn(context);
            mapped = mapRecord();
          }
          appendTurnItem(context, context.activeTurnId, record);

          for (const event of mapped) {
            emit(event);
            if (event.type === "content.delta") emittedContent = true;
            if (event.type === "turn.completed") {
              completedTurn = true;
              context.activeTurnId = undefined;
              context.session = {
                ...context.session,
                status: "ready",
                activeTurnId: undefined,
                updatedAt: new Date().toISOString(),
              };
            }
          }
        }
      } finally {
        await handle.close();
      }
    } catch (cause) {
      const error = cause instanceof Error ? cause : new Error(String(cause));
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        return { emittedContent, completedTurn };
      }
      emitRuntimeWarning({
        context,
        method: "transcript.poll",
        message: `Failed to read Antigravity transcript: ${error.message}`,
        detail: { transcriptPath },
      });
    }
    return { emittedContent, completedTurn };
  };

  const startTranscriptPoller = (context: SessionContext): void => {
    if (!context.conversationId || context.poller) return;
    context.poller = NodeTimers.setInterval(() => {
      void pollTranscriptOnce(context);
    }, TRANSCRIPT_POLL_MS);
    void pollTranscriptOnce(context);
  };

  const startSession: AntigravityAdapterShape["startSession"] = Effect.fn(
    "AntigravityAdapter.startSession",
  )(function* (input) {
    const createdAt = yield* currentTimestamp;
    const modelLabel = resolveAntigravityModelLabel(input.modelSelection);
    const resume = parseResumeCursor(input.resumeCursor);
    const session: ProviderSession = {
      provider: PROVIDER,
      ...(options.instanceId ? { providerInstanceId: options.instanceId } : {}),
      status: "ready",
      runtimeMode: input.runtimeMode,
      cwd: input.cwd ?? serverConfig.cwd,
      ...(modelLabel ? { model: modelLabel } : {}),
      threadId: input.threadId,
      ...(resume ? { resumeCursor: resume } : {}),
      createdAt,
      updatedAt: createdAt,
    };
    const context: SessionContext = {
      session,
      conversationId: resume?.conversationId,
      turns: [],
      activeTurnId: undefined,
      pendingGates: new Map(),
      autoApprovedGates: new Set(),
      poller: undefined,
      gatePoller: undefined,
      daemonEndpoint: undefined,
      daemonEndpointResolved: false,
      agentApiCancel: undefined,
      pollOffset: 0,
      pollCarry: "",
      seenLines: new Set(),
      toolCallStepIndexes: new Set(),
      stopped: false,
    };
    yield* Ref.update(sessionsRef, (sessions) => new Map(sessions).set(input.threadId, context));
    const rawSource = endpointFor(context) ? "antigravity.agentapi" : "antigravity.cli";
    if (context.conversationId) {
      startTranscriptPoller(context);
      startGatePoller(context);
    }
    emit({
      ...runtimeEventBase({
        threadId: input.threadId,
        ...(options.instanceId ? { instanceId: options.instanceId } : {}),
        createdAt,
        method: "session.start",
        rawSource,
      }),
      type: "session.started",
      payload: context.conversationId ? { resume: { conversationId: context.conversationId } } : {},
    });
    return session;
  });

  const sendTurn: AntigravityAdapterShape["sendTurn"] = Effect.fn("AntigravityAdapter.sendTurn")(
    function* (input) {
      const context = yield* getContext(input.threadId);
      const prompt = input.input?.trim();
      if (!prompt) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "Antigravity requires a non-empty text prompt.",
        });
      }

      const turnStartTime = Date.now();
      const cwd = context.session.cwd ?? serverConfig.cwd;
      const modelLabel = resolveAntigravityModelLabel(input.modelSelection);
      const modelAlias = resolveAntigravityCliModelAlias(input.modelSelection);
      const endpoint = endpointFor(context);
      const env = {
        ...baseEnv,
        ...(endpoint
          ? {
              ANTIGRAVITY_LS_ADDRESS: endpoint.address,
              ...(endpoint.csrfToken ? { ANTIGRAVITY_CSRF_TOKEN: endpoint.csrfToken } : {}),
            }
          : {}),
      };
      const rawSource = endpoint ? "antigravity.agentapi" : "antigravity.cli";
      yield* Effect.tryPromise({
        try: () =>
          ensureAntigravityCliSettings({
            settings,
            cwd,
            ...(modelLabel ? { modelLabel } : {}),
          }),
        catch: (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "settings.write",
            detail:
              cause instanceof Error
                ? cause.message
                : "Failed to prepare Antigravity CLI settings.",
            cause,
          }),
      }).pipe(
        Effect.catch((error: ProviderAdapterRequestError) =>
          Effect.sync(() =>
            emitRuntimeWarning({
              context,
              method: "settings.write",
              message: "Failed to prepare Antigravity CLI settings. Continuing without it.",
              detail: error.detail,
            }),
          ),
        ),
      );

      const attachmentText = (input.attachments ?? [])
        .map((attachment) =>
          resolveAttachmentPath({ attachmentsDir: serverConfig.attachmentsDir, attachment }),
        )
        .filter((entry): entry is string => typeof entry === "string" && entry.length > 0)
        .map((entry) => `\nAttachment: ${entry}`)
        .join("");
      let fullPrompt = [
        `<T3_WORKSPACE_CONTEXT>\nCurrent working directory: ${cwd}\nWhen the user refers to "this folder", "here", or the current folder, use this directory.\n</T3_WORKSPACE_CONTEXT>`,
        `${prompt}${attachmentText}`,
      ].join("\n\n");

      if (input.interactionMode === "plan") {
        fullPrompt += [
          "",
          "---",
          "SYSTEM INSTRUCTION FOR PLAN MODE:",
          "You are running in Plan Mode. Do NOT perform any code modifications or execute commands that alter state.",
          "Instead, analyze the codebase and design your proposed changes.",
          "Write your proposed plan to a markdown file inside the `.plans/` directory (e.g. `.plans/new-feature-plan.md`).",
          "Ensure your response explains the plan and lists the created plan file path.",
        ].join("\n");
      }

      const turnId = TurnId.make(`antigravity-turn-${yield* randomUUIDv4}`);
      const updatedAt = yield* currentTimestamp;

      context.activeTurnId = turnId;
      context.session = {
        ...context.session,
        status: "running",
        activeTurnId: turnId,
        ...(modelLabel ? { model: modelLabel } : {}),
        updatedAt,
      };
      context.turns.push({ id: turnId, items: [] });
      emit({
        ...runtimeEventBase({
          threadId: input.threadId,
          ...(options.instanceId ? { instanceId: options.instanceId } : {}),
          turnId,
          createdAt: updatedAt,
          method: "turn.start",
          rawSource,
        }),
        type: "turn.started",
        payload: modelLabel ? { model: modelLabel } : {},
      });

      if (context.conversationId) {
        const cliTranscriptStartOffset = yield* Effect.promise(() =>
          transcriptSizeForConversation({ settings, conversationId: context.conversationId! }),
        );
        context.pollOffset = cliTranscriptStartOffset;
        context.pollCarry = "";
      }

      const args = endpoint
        ? context.conversationId
          ? ["agentapi", "send-message", context.conversationId, fullPrompt]
          : [
              "agentapi",
              "new-conversation",
              ...(modelAlias ? [`--model=${modelAlias}`] : []),
              fullPrompt,
            ]
        : buildAgyPrintArgs({
            prompt: fullPrompt,
            conversationId: context.conversationId,
            modelAlias,
            fullAccess: context.session.runtimeMode === "full-access",
          });
      const commandMethod = endpoint ? (args[1] ?? "agentapi") : "agy.print";
      const previousConversationId = context.conversationId;

      const runAgyPromise = () =>
        options.runAgentApi
          ? options.runAgentApi(binaryPath, args, { cwd, env })
          : endpoint
            ? runAgentApiDefault(binaryPath, args, { cwd, env }, (child) => {
                context.agentApiCancel = () => {
                  if (child.exitCode !== null || child.killed) return;
                  child.kill("SIGTERM");
                  NodeTimers.setTimeout(() => {
                    if (child.exitCode === null && !child.killed) child.kill("SIGKILL");
                  }, 2_000).unref?.();
                };
              })
            : runAgyPrintDefault(binaryPath, args, { cwd, env }, (child) => {
                context.agentApiCancel = () => {
                  if (child.exitCode !== null || child.killed) return;
                  child.kill("SIGTERM");
                  NodeTimers.setTimeout(() => {
                    if (child.exitCode === null && !child.killed) child.kill("SIGKILL");
                  }, 2_000).unref?.();
                };
              });

      const runProcessEffect = Effect.tryPromise({
        try: () => runAgyPromise(),
        catch: (cause) => {
          const detail = cause instanceof Error ? cause.message : String(cause);
          return new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: commandMethod,
            detail,
            cause,
          });
        },
      }).pipe(
        Effect.flatMap((stdout) =>
          Effect.gen(function* () {
            context.agentApiCancel = undefined;
            if (stdout === INTERRUPTED_AGENTAPI_RESULT) {
              return;
            }

            if (!context.conversationId) {
              if (endpoint) {
                try {
                  const parsedJson = JSON.parse(stdout) as unknown;
                  const decoded = decodeNewConversationResponse(parsedJson);
                  context.conversationId = decoded.response.newConversation.conversationId;
                  const cursor = { conversationId: context.conversationId };
                  const resumedAt = yield* currentTimestamp;
                  context.session = {
                    ...context.session,
                    resumeCursor: cursor,
                    updatedAt: resumedAt,
                  };
                  emit({
                    ...runtimeEventBase({
                      threadId: input.threadId,
                      ...(options.instanceId ? { instanceId: options.instanceId } : {}),
                      createdAt: resumedAt,
                      method: "thread.start",
                      rawSource,
                      payload: cursor,
                    }),
                    type: "thread.started",
                    payload: { providerThreadId: context.conversationId },
                  });
                  startTranscriptPoller(context);
                  startGatePoller(context);
                } catch {
                  const discovered = yield* Effect.promise(() =>
                    readLastConversationIdForCwd({ settings, cwd }),
                  );
                  if (discovered) {
                    context.conversationId = discovered;
                    startTranscriptPoller(context);
                  }
                }
              } else {
                const discovered = yield* Effect.promise(() =>
                  readLastConversationIdForCwd({ settings, cwd }),
                );
                if (discovered) {
                  context.conversationId = discovered;
                  startTranscriptPoller(context);
                }
              }
            }

            const transcriptResult = context.conversationId
              ? yield* Effect.promise(() => pollTranscriptOnce(context))
              : { emittedContent: false, completedTurn: false };

            const planContent = yield* Effect.promise(() => scanForNewPlanFile(cwd, turnStartTime));
            if (planContent) {
              emit({
                ...runtimeEventBase({
                  threadId: input.threadId,
                  ...(options.instanceId ? { instanceId: options.instanceId } : {}),
                  turnId,
                  createdAt: yield* currentTimestamp,
                  method: "antigravity/plan-discovered",
                  rawSource,
                }),
                type: "turn.proposed.completed",
                payload: {
                  planMarkdown: planContent,
                },
              });
            }

            const output = stdout.trim();
            if (output && !transcriptResult.emittedContent) {
              appendTurnItem(context, turnId, { source: "cli", content: output });
              emit({
                ...runtimeEventBase({
                  threadId: input.threadId,
                  ...(options.instanceId ? { instanceId: options.instanceId } : {}),
                  turnId,
                  createdAt: yield* currentTimestamp,
                  method: "agy.print",
                  rawSource,
                  payload: { stdoutBytes: Buffer.byteLength(stdout, "utf8") },
                }),
                type: "content.delta",
                payload: { streamKind: "assistant_text", delta: output },
              });
            }

            if (!transcriptResult.completedTurn && context.activeTurnId === turnId) {
              const completedAt = yield* currentTimestamp;
              emit({
                ...runtimeEventBase({
                  threadId: input.threadId,
                  ...(options.instanceId ? { instanceId: options.instanceId } : {}),
                  turnId,
                  createdAt: completedAt,
                  method: "agy.print",
                  rawSource,
                  payload: { exit: 0 },
                }),
                type: "turn.completed",
                payload: { state: "completed" },
              });
              context.activeTurnId = undefined;
              context.session = {
                ...context.session,
                status: "ready",
                activeTurnId: undefined,
                updatedAt: completedAt,
              };
            }
          }),
        ),
        Effect.catch((error) =>
          Effect.gen(function* () {
            const wasInterrupted = context.activeTurnId !== turnId;
            if (wasInterrupted) {
              context.agentApiCancel = undefined;
              return;
            }
            const message = agentApiFailureMessage(error);
            const failedAt = yield* currentTimestamp;
            yield* Queue.offer(eventQueue, {
              ...runtimeEventBase({
                threadId: input.threadId,
                ...(options.instanceId ? { instanceId: options.instanceId } : {}),
                turnId,
                createdAt: failedAt,
                method: `${commandMethod}.error`,
                rawSource,
                payload: { method: commandMethod, detail: error.detail },
              }),
              type: "runtime.error",
              payload: { message, class: "provider_error", detail: error.detail },
            });
            yield* Queue.offer(eventQueue, {
              ...runtimeEventBase({
                threadId: input.threadId,
                ...(options.instanceId ? { instanceId: options.instanceId } : {}),
                turnId,
                createdAt: failedAt,
                method: `${commandMethod}.error`,
                rawSource,
                payload: { method: commandMethod, detail: error.detail },
              }),
              type: "turn.completed",
              payload: { state: "failed", errorMessage: message },
            });
            context.activeTurnId = undefined;
            context.agentApiCancel = undefined;
            context.session = {
              ...context.session,
              status: "error",
              activeTurnId: undefined,
              lastError: message,
              updatedAt: failedAt,
            };
          }),
        ),
      );

      yield* runProcessEffect.pipe(Effect.forkDetach);

      const startConversationIdPoller = (ctx: SessionContext) => {
        let attempts = 0;
        const interval = NodeTimers.setInterval(async () => {
          if (ctx.conversationId || ctx.stopped) {
            NodeTimers.clearInterval(interval);
            return;
          }
          attempts++;
          if (attempts > 30) {
            NodeTimers.clearInterval(interval);
            return;
          }
          const discovered = await readLastConversationIdForCwd({ settings, cwd });
          if (discovered && discovered !== previousConversationId) {
            NodeTimers.clearInterval(interval);
            ctx.conversationId = discovered;
            ctx.pollOffset = 0;
            ctx.pollCarry = "";
            const cursor = { conversationId: discovered };
            const resumedAt = new Date().toISOString();
            ctx.session = {
              ...ctx.session,
              resumeCursor: cursor,
              updatedAt: resumedAt,
            };
            emit({
              ...runtimeEventBase({
                threadId: input.threadId,
                ...(options.instanceId ? { instanceId: options.instanceId } : {}),
                createdAt: resumedAt,
                method: "thread.start",
                rawSource,
                payload: cursor,
              }),
              type: "thread.started",
              payload: { providerThreadId: discovered },
            });
            startTranscriptPoller(ctx);
            if (endpoint) {
              startGatePoller(ctx);
            }
          }
        }, 500);
      };

      if (!context.conversationId) {
        startConversationIdPoller(context);
        let waited = 0;
        while (!context.conversationId && waited < 5000 && !context.stopped) {
          yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 50)));
          waited += 50;
        }
      } else {
        startTranscriptPoller(context);
        yield* Effect.promise(() => pollTranscriptOnce(context));
        if (endpoint) {
          startGatePoller(context);
          yield* Effect.promise(() => pollGates(context));
        }
      }

      return {
        threadId: input.threadId,
        turnId,
        ...(context.conversationId
          ? { resumeCursor: { conversationId: context.conversationId } }
          : {}),
      } satisfies ProviderTurnStartResult;
    },
  );

  const interruptTurn: AntigravityAdapterShape["interruptTurn"] = Effect.fn(
    "AntigravityAdapter.interruptTurn",
  )(function* (threadId, explicitTurnId) {
    const context = yield* getContext(threadId);
    const turnId = explicitTurnId ?? context.activeTurnId;
    context.agentApiCancel?.();
    context.agentApiCancel = undefined;
    const endpoint = endpointFor(context);
    if (context.conversationId && endpoint) {
      yield* Effect.tryPromise({
        try: () =>
          antigravityLanguageServerRpc({
            endpoint,
            method: "CancelCascadeInvocation",
            body: {
              cascadeId: context.conversationId,
              killBackgroundTasks: true,
              notifyParent: false,
            },
          }),
        catch: (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "CancelCascadeInvocation",
            detail: cause instanceof Error ? cause.message : String(cause),
            cause,
          }),
      }).pipe(Effect.catch(() => Effect.void));
    }
    const updatedAt = yield* currentTimestamp;
    context.activeTurnId = undefined;
    context.pendingGates.clear();
    context.autoApprovedGates.clear();
    context.session = { ...context.session, status: "ready", activeTurnId: undefined, updatedAt };
    emit({
      ...runtimeEventBase({
        threadId,
        ...(options.instanceId ? { instanceId: options.instanceId } : {}),
        ...(turnId ? { turnId } : {}),
        createdAt: updatedAt,
        method: "interrupt",
        rawSource: "antigravity.agentapi",
      }),
      type: "turn.completed",
      payload: { state: "cancelled" },
    });
  });

  const respondToRequest: AntigravityAdapterShape["respondToRequest"] = (
    threadId,
    requestId,
    decision: ProviderApprovalDecision,
  ) =>
    getContext(threadId).pipe(
      Effect.flatMap((context) => {
        const gate = context.pendingGates.get(requestId);
        const endpoint = gate ? endpointFor(context) : undefined;
        const conversationId = context.conversationId;
        if (!gate || !conversationId || !endpoint) {
          return Effect.fail(
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "respondToRequest",
              detail: `Antigravity has no pending approval ${requestId}.`,
            }),
          );
        }
        return Effect.tryPromise({
          try: () => sendCascadeGateDecision({ endpoint, conversationId, gate, decision }),
          catch: (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "HandleCascadeUserInteraction",
              detail: cause instanceof Error ? cause.message : String(cause),
              cause,
            }),
        }).pipe(
          Effect.map(() => {
            context.pendingGates.delete(requestId);
            context.autoApprovedGates.delete(requestId);
            emitGateResolved(context, gate, decision);
          }),
        );
      }),
      Effect.asVoid,
    );

  const respondToUserInput: AntigravityAdapterShape["respondToUserInput"] = (
    threadId,
    requestId,
    _answers: ProviderUserInputAnswers,
  ) =>
    getContext(threadId).pipe(
      Effect.flatMap(() =>
        Effect.fail(
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "respondToUserInput",
            detail: `Antigravity daemon mode did not expose pending user input ${requestId}.`,
          }),
        ),
      ),
      Effect.asVoid,
    );

  const stopSession: AntigravityAdapterShape["stopSession"] = Effect.fn(
    "AntigravityAdapter.stopSession",
  )(function* (threadId) {
    const context = yield* getContext(threadId);
    const updatedAt = yield* currentTimestamp;
    context.agentApiCancel?.();
    context.agentApiCancel = undefined;
    const endpoint = endpointFor(context);
    if (context.conversationId && endpoint) {
      yield* Effect.tryPromise({
        try: () =>
          antigravityLanguageServerRpc({
            endpoint,
            method: "CancelCascadeInvocation",
            body: {
              cascadeId: context.conversationId,
              killBackgroundTasks: true,
              notifyParent: false,
            },
          }),
        catch: (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "CancelCascadeInvocation",
            detail: cause instanceof Error ? cause.message : String(cause),
            cause,
          }),
      }).pipe(Effect.catch(() => Effect.void));
    }
    context.stopped = true;
    if (context.poller) NodeTimers.clearInterval(context.poller);
    if (context.gatePoller) NodeTimers.clearInterval(context.gatePoller);
    context.pendingGates.clear();
    context.autoApprovedGates.clear();
    context.session = { ...context.session, status: "closed", updatedAt };
    yield* Ref.update(sessionsRef, (sessions) => {
      const next = new Map(sessions);
      next.delete(threadId);
      return next;
    });
  });

  const listSessions = () =>
    Ref.get(sessionsRef).pipe(
      Effect.map((sessions) => Array.from(sessions.values(), (entry) => entry.session)),
    );

  const hasSession = (threadId: ThreadId) =>
    Ref.get(sessionsRef).pipe(Effect.map((sessions) => sessions.has(threadId)));

  const readThread: AntigravityAdapterShape["readThread"] = (threadId) =>
    getContext(threadId).pipe(Effect.map((context) => ({ threadId, turns: context.turns })));

  const rollbackThread: AntigravityAdapterShape["rollbackThread"] = (threadId, numTurns) =>
    getContext(threadId).pipe(
      Effect.flatMap(
        (): Effect.Effect<never, ProviderAdapterValidationError | ProviderAdapterRequestError> => {
          if (!Number.isInteger(numTurns) || numTurns < 1) {
            return Effect.fail(
              new ProviderAdapterValidationError({
                provider: PROVIDER,
                operation: "rollbackThread",
                issue: "numTurns must be at least 1.",
              }),
            );
          }
          return Effect.fail(
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "rollbackThread",
              detail: "Antigravity does not support provider-side rollback yet.",
            }),
          );
        },
      ),
    );

  const stopAll = () =>
    Ref.get(sessionsRef).pipe(
      Effect.flatMap((sessions) =>
        Effect.forEach(Array.from(sessions.keys()), (threadId) => stopSession(threadId), {
          concurrency: "unbounded",
          discard: true,
        }),
      ),
    );

  return {
    provider: PROVIDER,
    capabilities: { sessionModelSwitch: "in-session" },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    readThread,
    rollbackThread,
    stopAll,
    streamEvents: Stream.fromQueue(eventQueue),
  } satisfies AntigravityAdapterShape;
});

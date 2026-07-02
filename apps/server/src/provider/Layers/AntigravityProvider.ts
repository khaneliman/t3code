// @effect-diagnostics nodeBuiltinImport:off
import {
  type AntigravitySettings,
  type ModelCapabilities,
  type ModelSelection,
  ProviderDriverKind,
  type ServerProviderModel,
} from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { createModelCapabilities, getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import { expandHomePath } from "../../pathExpansion.ts";
import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  spawnAndCollect,
  type CommandResult,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const PROVIDER = ProviderDriverKind.make("antigravity");
const ANTIGRAVITY_PRESENTATION = {
  displayName: "Antigravity",
  showInteractionModeToggle: false,
} as const;

export const DEFAULT_ANTIGRAVITY_HOME_PATH = "~/.gemini/antigravity-cli";
export const DEFAULT_ANTIGRAVITY_BRAIN_PATH = "~/.gemini/antigravity-cli/brain";
export const DEFAULT_ANTIGRAVITY_SETTINGS_PATH = "~/.gemini/antigravity-cli/settings.json";

const CLI_PROBE_TIMEOUT_MS = 4_000;
const DAEMON_PROBE_TIMEOUT_MS = 4_000;

const DEFAULT_MODEL_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

export const CURRENT_ANTIGRAVITY_MODEL_LABELS = [
  "Gemini 3.5 Flash (Medium)",
  "Gemini 3.5 Flash (High)",
  "Gemini 3.5 Flash (Low)",
  "Gemini 3.1 Pro (Low)",
  "Gemini 3.1 Pro (High)",
  "Claude Sonnet 4.6 (Thinking)",
  "Claude Opus 4.6 (Thinking)",
  "GPT-OSS 120B (Medium)",
] as const;

const ANTIGRAVITY_MODEL_ALIASES: Readonly<Record<string, string>> = {
  gemini: "Gemini 3.5 Flash (Medium)",
  flash: "Gemini 3.5 Flash (Medium)",
  flash_lite: "Gemini 3.5 Flash (Low)",
  pro: "Gemini 3.1 Pro (High)",
  low: "Gemini 3.5 Flash (Low)",
  medium: "Gemini 3.5 Flash (Medium)",
  high: "Gemini 3.1 Pro (High)",
};

const ANTIGRAVITY_CLI_MODEL_ALIASES: Readonly<Record<string, string>> = {
  "Gemini 3.5 Flash (Low)": "flash_lite",
  "Gemini 3.5 Flash (Medium)": "flash",
  "Gemini 3.5 Flash (High)": "flash",
  "Gemini 3.1 Pro (Low)": "pro",
  "Gemini 3.1 Pro (High)": "pro",
  flash_lite: "flash_lite",
  flash: "flash",
  pro: "pro",
};

const REASONING_EFFORT_LABELS: Readonly<Record<string, string>> = {
  low: "Low",
  medium: "Medium",
  high: "High",
  thinking: "Thinking",
};

const REASONING_EFFORT_ORDER: ReadonlyArray<string> = ["low", "medium", "high", "thinking"];

interface AntigravityModelLabelParts {
  readonly baseName: string;
  readonly reasoningEffort?: string;
}

export interface AntigravityDaemonEndpoint {
  readonly address: string;
  readonly csrfToken: string | undefined;
}

interface AntigravityDaemonCandidate {
  readonly address: string;
  readonly csrfToken: string | undefined;
}

function normalizeReasoningEffort(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "-")
    .replace(/[^a-z0-9-]/g, "");
}

function reasoningEffortLabel(value: string): string {
  return (
    REASONING_EFFORT_LABELS[value] ??
    value
      .split("-")
      .filter(Boolean)
      .map((part) => `${part.slice(0, 1).toUpperCase()}${part.slice(1)}`)
      .join(" ")
  );
}

function sortReasoningEfforts(values: ReadonlyArray<string>): ReadonlyArray<string> {
  return [...values].sort((left, right) => {
    const leftIndex = REASONING_EFFORT_ORDER.indexOf(left);
    const rightIndex = REASONING_EFFORT_ORDER.indexOf(right);
    if (leftIndex >= 0 || rightIndex >= 0) {
      return (
        (leftIndex >= 0 ? leftIndex : Number.MAX_SAFE_INTEGER) -
        (rightIndex >= 0 ? rightIndex : Number.MAX_SAFE_INTEGER)
      );
    }
    return left.localeCompare(right);
  });
}

export function parseAntigravityModelLabel(label: string): AntigravityModelLabelParts | undefined {
  const trimmed = label.trim();
  if (!trimmed) return undefined;

  const match = /^(?<base>.+?)\s+\((?<suffix>[^()]+)\)$/.exec(trimmed);
  if (!match?.groups) return { baseName: trimmed };

  const baseName = match.groups.base?.trim();
  const reasoningEffort = normalizeReasoningEffort(match.groups.suffix ?? "");
  if (!baseName || !reasoningEffort) return { baseName: trimmed };
  return { baseName, reasoningEffort };
}

export function formatAntigravityModelLabel(input: {
  readonly baseName: string;
  readonly reasoningEffort?: string | null;
}): string {
  const baseName = input.baseName.trim();
  const reasoningEffort = input.reasoningEffort
    ? normalizeReasoningEffort(input.reasoningEffort)
    : "";
  return reasoningEffort ? `${baseName} (${reasoningEffortLabel(reasoningEffort)})` : baseName;
}

function defaultReasoningEffortFor(efforts: ReadonlySet<string>): string | undefined {
  if (efforts.has("medium")) return "medium";
  if (efforts.has("high")) return "high";
  if (efforts.has("thinking")) return "thinking";
  return sortReasoningEfforts([...efforts])[0];
}

function antigravityModelCapabilities(
  reasoningEfforts: ReadonlySet<string>,
  defaultReasoningEffort: string | undefined,
): ModelCapabilities {
  if (reasoningEfforts.size === 0) return DEFAULT_MODEL_CAPABILITIES;

  return createModelCapabilities({
    optionDescriptors: [
      {
        id: "reasoningEffort",
        label: "Reasoning",
        type: "select",
        options: sortReasoningEfforts([...reasoningEfforts]).map((effort) =>
          effort === defaultReasoningEffort
            ? { id: effort, label: reasoningEffortLabel(effort), isDefault: true }
            : { id: effort, label: reasoningEffortLabel(effort) },
        ),
        ...(defaultReasoningEffort ? { currentValue: defaultReasoningEffort } : {}),
      },
    ],
  });
}

export function parseAntigravityModelsOutput(output: string): ReadonlyArray<string> {
  return output
    .split(/\r?\n/g)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
}

export function buildAntigravityProviderModels(input: {
  readonly labels: ReadonlyArray<string>;
  readonly customLabels?: ReadonlyArray<string>;
}): ReadonlyArray<ServerProviderModel> {
  const groups = new Map<
    string,
    {
      readonly baseName: string;
      readonly reasoningEfforts: Set<string>;
      isCustom: boolean;
    }
  >();

  const add = (label: string, isCustom: boolean) => {
    const mapped = ANTIGRAVITY_MODEL_ALIASES[label.trim()] ?? label;
    const parsed = parseAntigravityModelLabel(mapped);
    if (!parsed) return;

    const existing = groups.get(parsed.baseName);
    if (existing) {
      if (parsed.reasoningEffort) existing.reasoningEfforts.add(parsed.reasoningEffort);
      existing.isCustom = existing.isCustom && isCustom;
      return;
    }

    groups.set(parsed.baseName, {
      baseName: parsed.baseName,
      reasoningEfforts: new Set(parsed.reasoningEffort ? [parsed.reasoningEffort] : []),
      isCustom,
    });
  };

  for (const label of input.labels) add(label, false);
  for (const label of input.customLabels ?? []) add(label, true);

  return [...groups.values()].map((group) => {
    const defaultReasoningEffort = defaultReasoningEffortFor(group.reasoningEfforts);
    return {
      slug: formatAntigravityModelLabel(
        defaultReasoningEffort
          ? { baseName: group.baseName, reasoningEffort: defaultReasoningEffort }
          : { baseName: group.baseName },
      ),
      name: group.baseName,
      isCustom: group.isCustom,
      capabilities: antigravityModelCapabilities(group.reasoningEfforts, defaultReasoningEffort),
    };
  });
}

export function resolveAntigravityModelLabel(
  modelSelection: ModelSelection | null | undefined,
): string | undefined {
  const rawModel = modelSelection?.model?.trim();
  if (!rawModel) return undefined;

  const mapped = ANTIGRAVITY_MODEL_ALIASES[rawModel] ?? rawModel;
  const parsed = parseAntigravityModelLabel(mapped);
  if (!parsed) return undefined;

  const selectedReasoning = getModelSelectionStringOptionValue(modelSelection, "reasoningEffort");
  const reasoningEffort = selectedReasoning ?? parsed.reasoningEffort;
  return formatAntigravityModelLabel(
    reasoningEffort
      ? { baseName: parsed.baseName, reasoningEffort }
      : { baseName: parsed.baseName },
  );
}

export function resolveAntigravityCliModelAlias(
  modelSelection: ModelSelection | null | undefined,
): string | undefined {
  const label = resolveAntigravityModelLabel(modelSelection);
  if (label && ANTIGRAVITY_CLI_MODEL_ALIASES[label]) {
    return ANTIGRAVITY_CLI_MODEL_ALIASES[label];
  }
  const rawModel = modelSelection?.model?.trim();
  return rawModel ? ANTIGRAVITY_CLI_MODEL_ALIASES[rawModel] : undefined;
}

export function resolveAntigravityBinaryPath(settings: AntigravitySettings): string {
  return expandHomePath(settings.binaryPath || "agy");
}

export function resolveAntigravityHomePath(settings: AntigravitySettings): string {
  return expandHomePath(settings.homePath || DEFAULT_ANTIGRAVITY_HOME_PATH);
}

export function resolveAntigravityBrainPath(settings: AntigravitySettings): string {
  return expandHomePath(
    settings.brainPath || NodePath.join(resolveAntigravityHomePath(settings), "brain"),
  );
}

export function resolveAntigravitySettingsPath(settings: AntigravitySettings): string {
  return expandHomePath(
    settings.settingsPath || NodePath.join(resolveAntigravityHomePath(settings), "settings.json"),
  );
}

export function transcriptPathForConversation(input: {
  readonly settings: AntigravitySettings;
  readonly conversationId: string;
}): string {
  return NodePath.join(
    resolveAntigravityBrainPath(input.settings),
    input.conversationId,
    ".system_generated",
    "logs",
    "transcript.jsonl",
  );
}

function antigravityModelsFromLabels(
  settings: AntigravitySettings,
  labels: ReadonlyArray<string> = CURRENT_ANTIGRAVITY_MODEL_LABELS,
): ReadonlyArray<ServerProviderModel> {
  return buildAntigravityProviderModels({
    labels: labels.length > 0 ? labels : CURRENT_ANTIGRAVITY_MODEL_LABELS,
    customLabels: settings.customModels,
  });
}

function runAntigravityCommand(
  settings: AntigravitySettings,
  args: ReadonlyArray<string>,
  environment: NodeJS.ProcessEnv,
) {
  return Effect.gen(function* () {
    const command = resolveAntigravityBinaryPath(settings);
    const spawnCommand = yield* resolveSpawnCommand(command, args, { env: environment });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });
}

function runAntigravityAgentApiCommand(
  settings: AntigravitySettings,
  args: ReadonlyArray<string>,
  environment: NodeJS.ProcessEnv,
) {
  return runAntigravityCommand(settings, ["agentapi", ...args], environment);
}

export function parseAntigravityLanguageServerCmdline(
  cmdline: ReadonlyArray<string>,
): { readonly csrfToken: string | undefined } | undefined {
  const executable = cmdline[0] ?? "";
  if (!executable.includes("language_server") && !cmdline.includes("language_server")) {
    return undefined;
  }

  const csrfTokenIndex = cmdline.indexOf("--csrf_token");
  return {
    csrfToken: csrfTokenIndex >= 0 ? cmdline[csrfTokenIndex + 1] : undefined,
  };
}

export function parseLinuxTcpListenPortsForInodes(
  contents: string,
  socketInodes: ReadonlySet<string>,
): ReadonlyArray<number> {
  const ports: Array<number> = [];
  for (const line of contents.split(/\r?\n/g).slice(1)) {
    const columns = line.trim().split(/\s+/g);
    const localAddress = columns[1];
    const state = columns[3];
    const inode = columns[9];
    if (!localAddress || state !== "0A" || !inode || !socketInodes.has(inode)) continue;

    const [hostHex, portHex] = localAddress.split(":");
    if (!hostHex || !portHex) continue;

    const isLoopback =
      hostHex === "0100007F" ||
      hostHex === "00000000000000000000000001000000" ||
      hostHex === "00000000000000000000000000000001";
    if (!isLoopback) continue;

    const port = Number.parseInt(portHex, 16);
    if (Number.isFinite(port)) ports.push(port);
  }

  return [...new Set(ports)].sort((left, right) => left - right);
}

function readProcCmdline(pid: string): ReadonlyArray<string> {
  try {
    return NodeFS.readFileSync(`/proc/${pid}/cmdline`, "utf8").split("\0").filter(Boolean);
  } catch {
    return [];
  }
}

function readSocketInodesForPid(pid: string): ReadonlySet<string> {
  const inodes = new Set<string>();
  try {
    for (const fd of NodeFS.readdirSync(`/proc/${pid}/fd`)) {
      const target = NodeFS.readlinkSync(`/proc/${pid}/fd/${fd}`);
      const match = /^socket:\[(\d+)\]$/.exec(target);
      if (match?.[1]) inodes.add(match[1]);
    }
  } catch {
    // Process can exit while scanning /proc.
  }
  return inodes;
}

function readListenPortsForPid(pid: string): ReadonlyArray<number> {
  const socketInodes = readSocketInodesForPid(pid);
  if (socketInodes.size === 0) return [];

  const ports = new Set<number>();
  for (const table of ["/proc/net/tcp", "/proc/net/tcp6"]) {
    try {
      for (const port of parseLinuxTcpListenPortsForInodes(
        NodeFS.readFileSync(table, "utf8"),
        socketInodes,
      )) {
        ports.add(port);
      }
    } catch {
      // Some kernels or containers do not expose both tables.
    }
  }
  return [...ports].sort((left, right) => left - right);
}

function daemonProbeReached(result: CommandResult): boolean {
  return `${result.stdout}\n${result.stderr}`.includes("trajectory not found: __t3_probe__");
}

function isUsableAntigravityDaemonCandidate(input: {
  readonly binaryPath: string;
  readonly environment: NodeJS.ProcessEnv;
  readonly candidate: AntigravityDaemonCandidate;
}): boolean {
  const env = {
    ...input.environment,
    ANTIGRAVITY_LS_ADDRESS: input.candidate.address,
    ...(input.candidate.csrfToken ? { ANTIGRAVITY_CSRF_TOKEN: input.candidate.csrfToken } : {}),
  };
  try {
    const output = NodeChildProcess.execFileSync(
      input.binaryPath,
      ["agentapi", "get-conversation-metadata", "__t3_probe__"],
      {
        env,
        timeout: 2_000,
        windowsHide: true,
        encoding: "utf8",
      },
    );
    return output.includes("trajectory not found: __t3_probe__");
  } catch (cause) {
    const stdout =
      cause && typeof cause === "object" && "stdout" in cause
        ? String((cause as { readonly stdout?: unknown }).stdout ?? "")
        : "";
    const stderr =
      cause && typeof cause === "object" && "stderr" in cause
        ? String((cause as { readonly stderr?: unknown }).stderr ?? "")
        : "";
    return `${stdout}\n${stderr}`.includes("trajectory not found: __t3_probe__");
  }
}

function detectAntigravityDaemonEndpointFromProc(input: {
  readonly binaryPath: string;
  readonly environment: NodeJS.ProcessEnv;
}): AntigravityDaemonEndpoint | undefined {
  const candidates: Array<AntigravityDaemonCandidate> = [];
  try {
    for (const pid of NodeFS.readdirSync("/proc").filter((entry) => /^\d+$/.test(entry))) {
      const processInfo = parseAntigravityLanguageServerCmdline(readProcCmdline(pid));
      if (!processInfo) continue;

      for (const port of readListenPortsForPid(pid)) {
        candidates.push({
          address: `http://127.0.0.1:${port}`,
          csrfToken: processInfo.csrfToken,
        });
      }
    }
  } catch {
    return undefined;
  }

  return (
    candidates.find((candidate) =>
      isUsableAntigravityDaemonCandidate({
        binaryPath: input.binaryPath,
        environment: input.environment,
        candidate,
      }),
    ) ?? candidates[0]
  );
}

export function resolveAntigravityDaemonEndpoint(
  settings: AntigravitySettings,
  environment: NodeJS.ProcessEnv = process.env,
  platform?: NodeJS.Platform,
): AntigravityDaemonEndpoint | undefined {
  const settingsAddress = settings.languageServerAddress.trim();
  if (settingsAddress) {
    return {
      address: settingsAddress,
      csrfToken: settings.csrfToken.trim() || undefined,
    };
  }

  const envAddress = environment.ANTIGRAVITY_LS_ADDRESS?.trim();
  if (envAddress) {
    return {
      address: envAddress,
      csrfToken: environment.ANTIGRAVITY_CSRF_TOKEN?.trim() || undefined,
    };
  }

  if (platform !== "linux") return undefined;

  return detectAntigravityDaemonEndpointFromProc({
    binaryPath: resolveAntigravityBinaryPath(settings),
    environment,
  });
}

export function makeAntigravityEnvironment(
  settings: AntigravitySettings,
  environment: NodeJS.ProcessEnv = process.env,
  platform?: NodeJS.Platform,
): NodeJS.ProcessEnv {
  const endpoint = resolveAntigravityDaemonEndpoint(settings, environment, platform);
  return {
    ...environment,
    ...(endpoint
      ? {
          ANTIGRAVITY_LS_ADDRESS: endpoint.address,
          ...(endpoint.csrfToken ? { ANTIGRAVITY_CSRF_TOKEN: endpoint.csrfToken } : {}),
        }
      : {}),
  };
}

export async function antigravityLanguageServerRpc(input: {
  readonly endpoint: AntigravityDaemonEndpoint;
  readonly method: string;
  readonly body: unknown;
}): Promise<unknown> {
  const response = await fetch(
    `${input.endpoint.address}/exa.language_server_pb.LanguageServerService/${input.method}`,
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...(input.endpoint.csrfToken ? { "x-codeium-csrf-token": input.endpoint.csrfToken } : {}),
      },
      body: JSON.stringify(input.body),
    },
  );
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`${input.method} failed (${response.status}): ${text.slice(0, 300)}`);
  }
  return text ? (JSON.parse(text) as unknown) : {};
}

function daemonProbeEnvironment(
  environment: NodeJS.ProcessEnv,
  endpoint: AntigravityDaemonEndpoint | undefined,
): NodeJS.ProcessEnv {
  return {
    ...environment,
    ...(endpoint
      ? {
          ANTIGRAVITY_LS_ADDRESS: endpoint.address,
          ...(endpoint.csrfToken ? { ANTIGRAVITY_CSRF_TOKEN: endpoint.csrfToken } : {}),
        }
      : {}),
  };
}

export const checkAntigravityProviderStatus = Effect.fn("checkAntigravityProviderStatus")(
  function* (
    settings: AntigravitySettings,
    environment: NodeJS.ProcessEnv = process.env,
  ): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
    const platform = yield* HostProcessPlatform;
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const fallbackModels = antigravityModelsFromLabels(settings);

    if (!settings.enabled) {
      return buildServerProvider({
        driver: PROVIDER,
        presentation: ANTIGRAVITY_PRESENTATION,
        enabled: false,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown", label: "Antigravity CLI" },
          message: "Antigravity is disabled in T3 Code settings.",
        },
      });
    }

    const versionResult = yield* runAntigravityCommand(settings, ["--version"], environment).pipe(
      Effect.timeoutOption(CLI_PROBE_TIMEOUT_MS),
      Effect.result,
    );

    if (Result.isFailure(versionResult)) {
      const error = versionResult.failure;
      return buildServerProvider({
        driver: PROVIDER,
        presentation: ANTIGRAVITY_PRESENTATION,
        enabled: true,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: !isCommandMissingCause(error),
          version: null,
          status: "error",
          auth: { status: "unknown", label: "Antigravity CLI" },
          message: isCommandMissingCause(error)
            ? "Antigravity CLI (`agy`) is not installed or not on PATH."
            : "Failed to execute Antigravity CLI health check.",
        },
      });
    }

    if (Option.isNone(versionResult.success)) {
      return buildServerProvider({
        driver: PROVIDER,
        presentation: ANTIGRAVITY_PRESENTATION,
        enabled: true,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: true,
          version: null,
          status: "error",
          auth: { status: "unknown", label: "Antigravity CLI" },
          message: "Antigravity CLI timed out while running `agy --version`.",
        },
      });
    }

    const versionOutput = versionResult.success.value;
    const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
    if (versionOutput.code !== 0) {
      return buildServerProvider({
        driver: PROVIDER,
        presentation: ANTIGRAVITY_PRESENTATION,
        enabled: true,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: true,
          version,
          status: "error",
          auth: { status: "unknown", label: "Antigravity CLI" },
          message: "Antigravity CLI is installed but failed to run.",
        },
      });
    }

    const modelsResult = yield* runAntigravityCommand(settings, ["models"], environment).pipe(
      Effect.timeoutOption(CLI_PROBE_TIMEOUT_MS),
      Effect.result,
    );
    const liveModelLabels =
      Result.isSuccess(modelsResult) && Option.isSome(modelsResult.success)
        ? parseAntigravityModelsOutput(modelsResult.success.value.stdout)
        : [];
    const models = antigravityModelsFromLabels(settings, liveModelLabels);

    const endpoint = resolveAntigravityDaemonEndpoint(settings, environment, platform);
    const daemonResult =
      endpoint === undefined
        ? undefined
        : yield* runAntigravityAgentApiCommand(
            settings,
            ["get-conversation-metadata", "__t3_probe__"],
            daemonProbeEnvironment(environment, endpoint),
          ).pipe(Effect.timeoutOption(DAEMON_PROBE_TIMEOUT_MS), Effect.result);
    const daemonReachable =
      daemonResult !== undefined &&
      Result.isSuccess(daemonResult) &&
      Option.isSome(daemonResult.success) &&
      daemonProbeReached(daemonResult.success.value);

    return buildServerProvider({
      driver: PROVIDER,
      presentation: ANTIGRAVITY_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version,
        status: "ready",
        auth: {
          status: "authenticated",
          label: daemonReachable ? "Antigravity agentapi" : "Antigravity CLI",
        },
        ...(daemonReachable
          ? {}
          : {
              message: "Antigravity CLI mode is ready. Daemon-only approval APIs are unavailable.",
            }),
      },
    });
  },
);

export const makePendingAntigravityProvider = Effect.fn("makePendingAntigravityProvider")(
  function* (settings: AntigravitySettings): Effect.fn.Return<ServerProviderDraft> {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    return buildServerProvider({
      driver: PROVIDER,
      presentation: ANTIGRAVITY_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models: antigravityModelsFromLabels(settings),
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown", label: "Antigravity CLI" },
        message: settings.enabled
          ? "Antigravity provider status has not been checked in this session yet."
          : "Antigravity is disabled in T3 Code settings.",
      },
    });
  },
);

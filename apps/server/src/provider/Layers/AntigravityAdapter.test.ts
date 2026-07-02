// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalTimers:off
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import {
  ApprovalRequestId,
  AntigravitySettings,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as NodeFSP from "node:fs/promises";
import * as NodeHttp from "node:http";
import type * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect } from "vite-plus/test";

import { deriveServerPaths, ServerConfig } from "../../config.ts";
import {
  makeAntigravityAdapter,
  mapAntigravityTranscriptRecordToRuntimeEvents,
  parseAntigravityTranscriptLine,
} from "./AntigravityAdapter.ts";

const decodeAntigravitySettings = Schema.decodeSync(AntigravitySettings);

const makeTestServerConfig = (baseDir: string) =>
  Effect.gen(function* () {
    const derivedPaths = yield* deriveServerPaths(baseDir, undefined);
    return {
      logLevel: "Info",
      traceMinLevel: "Info",
      traceTimingEnabled: true,
      traceBatchWindowMs: 200,
      traceMaxBytes: 10 * 1024 * 1024,
      traceMaxFiles: 10,
      otlpTracesUrl: undefined,
      otlpMetricsUrl: undefined,
      otlpExportIntervalMs: 10_000,
      otlpServiceName: "t3-server",
      mode: "web",
      port: 0,
      host: "127.0.0.1",
      cwd: baseDir,
      baseDir,
      ...derivedPaths,
      staticDir: undefined,
      devUrl: undefined,
      noBrowser: true,
      startupPresentation: "browser",
      desktopBootstrapToken: undefined,
      autoBootstrapProjectFromCwd: false,
      logWebSocketEvents: false,
      tailscaleServeEnabled: false,
      tailscaleServePort: 443,
    } satisfies ServerConfig["Service"];
  });

async function waitFor(
  predicate: () => boolean,
  events: ReadonlyArray<ProviderRuntimeEvent>,
  timeoutMs = 5_000,
): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`timeout waiting; events=${events.map((event) => event.type).join(",")}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

function makeAdapter(
  baseDir: string,
  settingsPatch: Partial<AntigravitySettings>,
  runAgentApi?: (
    binaryPath: string,
    args: ReadonlyArray<string>,
    options: { readonly cwd: string; readonly env: NodeJS.ProcessEnv },
  ) => Promise<string>,
) {
  const settings = decodeAntigravitySettings({
    brainPath: NodePath.join(baseDir, "brain"),
    settingsPath: NodePath.join(baseDir, "settings.json"),
    ...settingsPatch,
  });
  return Effect.gen(function* () {
    const config = yield* makeTestServerConfig(baseDir);
    return yield* makeAntigravityAdapter(settings, {
      instanceId: ProviderInstanceId.make("antigravity"),
      environment: {},
      ...(runAgentApi ? { runAgentApi } : {}),
    }).pipe(Effect.provideService(ServerConfig, config));
  }).pipe(Effect.provide(NodeServices.layer));
}

type TestAdapter = Effect.Success<ReturnType<typeof makeAdapter>>;

function collectEvents(adapter: TestAdapter) {
  return Effect.gen(function* () {
    const events: ProviderRuntimeEvent[] = [];
    const fiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
      Effect.sync(() => {
        events.push(event);
      }),
    ).pipe(Effect.forkChild);
    return { events, fiber };
  });
}

async function readRequestBody(request: NodeHttp.IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  const text = Buffer.concat(chunks).toString("utf8");
  return text ? (JSON.parse(text) as unknown) : {};
}

async function startDaemonServer() {
  const decisions: unknown[] = [];
  const server = NodeHttp.createServer((request, response) => {
    void (async () => {
      const method = request.url?.split("/").pop() ?? "";
      const body = await readRequestBody(request);
      if (method === "GetCascadeTrajectory") {
        response.writeHead(200, { "content-type": "application/json" });
        response.end(
          JSON.stringify({
            trajectory: {
              trajectoryId: "traj-1",
              steps: [
                {
                  status: "CORTEX_STEP_STATUS_WAITING",
                  metadata: {
                    sourceTrajectoryStepInfo: {
                      trajectoryId: "traj-1",
                      stepIndex: 0,
                    },
                  },
                  requestedInteraction: {
                    permission: { resource: { target: "pwd" } },
                  },
                },
              ],
            },
          }),
        );
        return;
      }
      if (method === "HandleCascadeUserInteraction") {
        decisions.push(body);
        response.writeHead(200, { "content-type": "application/json" });
        response.end("{}");
        return;
      }
      response.writeHead(404);
      response.end("{}");
    })();
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address() as NodeNet.AddressInfo;
  return {
    url: `http://127.0.0.1:${address.port}`,
    decisions,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

describe("AntigravityAdapter transcript helpers", () => {
  it("parses valid transcript lines and ignores malformed lines", () => {
    expect(
      parseAntigravityTranscriptLine(
        '{"step_index":7,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE"}',
      ),
    ).toMatchObject({ step_index: 7, source: "MODEL" });
    expect(parseAntigravityTranscriptLine("not json")).toBeUndefined();
    expect(parseAntigravityTranscriptLine("   ")).toBeUndefined();
  });

  it("maps command, tool, list-dir, final response, and system error records", () => {
    const base = {
      threadId: ThreadId.make("thread-1"),
      instanceId: ProviderInstanceId.make("antigravity"),
      turnId: TurnId.make("turn-1"),
      createdAt: "2026-05-29T00:00:00.000Z",
    };

    expect(
      mapAntigravityTranscriptRecordToRuntimeEvents({
        ...base,
        record: {
          step_index: 10,
          source: "MODEL",
          type: "RUN_COMMAND",
          status: "DONE",
          content: "47.0",
        },
      }).map((event) => event.type),
    ).toEqual(["item.completed", "content.delta"]);

    expect(
      mapAntigravityTranscriptRecordToRuntimeEvents({
        ...base,
        record: {
          step_index: 7,
          source: "MODEL",
          type: "PLANNER_RESPONSE",
          status: "DONE",
          tool_calls: [{ name: "write_to_file", args: { TargetFile: "/tmp/a.ts" } }],
        },
      })[0]?.payload,
    ).toMatchObject({ itemType: "dynamic_tool_call", title: "Write file" });

    expect(
      mapAntigravityTranscriptRecordToRuntimeEvents({
        ...base,
        record: {
          step_index: 3,
          source: "MODEL",
          type: "LIST_DIRECTORY",
          status: "DONE",
          content: '{"name":"package.json"}',
        },
      })[0]?.payload,
    ).toMatchObject({ itemType: "dynamic_tool_call", title: "Listed directory" });

    expect(
      mapAntigravityTranscriptRecordToRuntimeEvents({
        ...base,
        record: {
          step_index: 12,
          source: "MODEL",
          type: "FINAL_RESPONSE",
          status: "DONE",
          content: "Done.",
        },
      }).map((event) => event.type),
    ).toEqual(["content.delta", "turn.completed"]);

    expect(
      mapAntigravityTranscriptRecordToRuntimeEvents({
        ...base,
        record: {
          step_index: 4,
          source: "SYSTEM",
          type: "ERROR_MESSAGE",
          status: "DONE",
          error: "usage limit has been exhausted",
        },
      }).map((event) => event.type),
    ).toEqual(["runtime.error", "turn.completed"]);
  });
});

describe("AntigravityAdapter sessions", () => {
  it.effect("uses new-conversation first, stores resume cursor, then uses send-message", () =>
    Effect.gen(function* () {
      const baseDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "antig-adapter-send-")),
      );
      try {
        const calls: ReadonlyArray<string>[] = [];
        const adapter = yield* makeAdapter(baseDir, {}, async (_binaryPath, args) => {
          calls.push([...args]);
          return JSON.stringify({ response: { newConversation: { conversationId: "conv-1" } } });
        });
        const threadId = ThreadId.make("thread-send");
        yield* adapter.startSession({ threadId, runtimeMode: "full-access", cwd: baseDir });
        const first = yield* adapter.sendTurn({ threadId, input: "hello", attachments: [] });
        yield* adapter.sendTurn({ threadId, input: "next", attachments: [] });

        expect(first.resumeCursor).toEqual({ conversationId: "conv-1" });
        expect(calls[0]?.slice(0, 2)).toEqual(["agentapi", "new-conversation"]);
        expect(calls[1]?.slice(0, 3)).toEqual(["agentapi", "send-message", "conv-1"]);
        yield* adapter.stopSession(threadId);
      } finally {
        yield* Effect.promise(() => NodeFSP.rm(baseDir, { recursive: true, force: true }));
      }
    }),
  );

  it.effect("starts transcript poller from resume cursor and buffers partial lines", () =>
    Effect.gen(function* () {
      const baseDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "antig-adapter-poll-")),
      );
      try {
        const conversationId = "conv-poll";
        const transcriptPath = NodePath.join(
          baseDir,
          "brain",
          conversationId,
          ".system_generated",
          "logs",
          "transcript.jsonl",
        );
        yield* Effect.promise(() =>
          NodeFSP.mkdir(NodePath.dirname(transcriptPath), { recursive: true }),
        );
        yield* Effect.promise(() => NodeFSP.writeFile(transcriptPath, ""));
        const adapter = yield* makeAdapter(baseDir, {});
        const { events, fiber } = yield* collectEvents(adapter);
        const threadId = ThreadId.make("thread-poll");
        try {
          yield* adapter.startSession({
            threadId,
            runtimeMode: "full-access",
            cwd: baseDir,
            resumeCursor: { conversationId },
          });
          yield* Effect.promise(() =>
            NodeFSP.appendFile(
              transcriptPath,
              '{"step_index":1,"source":"MODEL","type":"FINAL_RESPONSE","status":"DONE","content":"Hel',
            ),
          );
          yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 700)));
          expect(events.some((event) => event.type === "content.delta")).toBe(false);
          yield* Effect.promise(() => NodeFSP.appendFile(transcriptPath, 'lo"}\n'));
          yield* Effect.promise(() =>
            waitFor(() => events.some((event) => event.type === "turn.completed"), events),
          );
          expect(events.find((event) => event.type === "content.delta")?.payload).toMatchObject({
            delta: "Hello",
          });
        } finally {
          yield* adapter.stopSession(threadId).pipe(Effect.catch(() => Effect.void));
          yield* Fiber.interrupt(fiber).pipe(Effect.catch(() => Effect.void));
        }
      } finally {
        yield* Effect.promise(() => NodeFSP.rm(baseDir, { recursive: true, force: true }));
      }
    }),
  );

  it.effect("auto-approves full-access gates without opening approval UI", () =>
    Effect.gen(function* () {
      const baseDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "antig-adapter-auto-")),
      );
      const daemon = yield* Effect.promise(() => startDaemonServer());
      try {
        const adapter = yield* makeAdapter(
          baseDir,
          { languageServerAddress: daemon.url },
          async () => "{}",
        );
        const { events, fiber } = yield* collectEvents(adapter);
        const threadId = ThreadId.make("thread-auto");
        try {
          yield* adapter.startSession({
            threadId,
            runtimeMode: "full-access",
            cwd: baseDir,
            resumeCursor: { conversationId: "conv-auto" },
          });
          yield* adapter.sendTurn({ threadId, input: "run pwd", attachments: [] });
          yield* Effect.promise(() => waitFor(() => daemon.decisions.length > 0, events));
          expect(events.some((event) => event.type === "request.opened")).toBe(false);
        } finally {
          yield* adapter.stopSession(threadId).pipe(Effect.catch(() => Effect.void));
          yield* Fiber.interrupt(fiber).pipe(Effect.catch(() => Effect.void));
        }
      } finally {
        yield* Effect.promise(() => daemon.close());
        yield* Effect.promise(() => NodeFSP.rm(baseDir, { recursive: true, force: true }));
      }
    }),
  );

  it.effect("opens manual approval request and respondToRequest sends decision", () =>
    Effect.gen(function* () {
      const baseDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "antig-adapter-manual-")),
      );
      const daemon = yield* Effect.promise(() => startDaemonServer());
      try {
        const adapter = yield* makeAdapter(
          baseDir,
          { languageServerAddress: daemon.url },
          async () => "{}",
        );
        const { events, fiber } = yield* collectEvents(adapter);
        const threadId = ThreadId.make("thread-manual");
        try {
          yield* adapter.startSession({
            threadId,
            runtimeMode: "approval-required",
            cwd: baseDir,
            resumeCursor: { conversationId: "conv-manual" },
          });
          yield* adapter.sendTurn({ threadId, input: "run pwd", attachments: [] });
          yield* Effect.promise(() =>
            waitFor(() => events.some((event) => event.type === "request.opened"), events),
          );
          const opened = events.find((event) => event.type === "request.opened");
          expect(opened?.payload).toMatchObject({ requestType: "command_execution_approval" });
          yield* adapter.respondToRequest(
            threadId,
            ApprovalRequestId.make(String(opened!.requestId!)),
            "acceptForSession",
          );
          yield* Effect.promise(() => waitFor(() => daemon.decisions.length > 0, events));
          yield* Effect.promise(() =>
            waitFor(() => events.some((event) => event.type === "request.resolved"), events),
          );
        } finally {
          yield* adapter.stopSession(threadId).pipe(Effect.catch(() => Effect.void));
          yield* Fiber.interrupt(fiber).pipe(Effect.catch(() => Effect.void));
        }
      } finally {
        yield* Effect.promise(() => daemon.close());
        yield* Effect.promise(() => NodeFSP.rm(baseDir, { recursive: true, force: true }));
      }
    }),
  );

  it.effect("returns unsupported error for provider-side rollback", () =>
    Effect.gen(function* () {
      const baseDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "antig-adapter-rollback-")),
      );
      try {
        const adapter = yield* makeAdapter(baseDir, {});
        const threadId = ThreadId.make("thread-rollback");
        yield* adapter.startSession({ threadId, runtimeMode: "full-access", cwd: baseDir });
        const result = yield* Effect.exit(adapter.rollbackThread(threadId, 1));
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure") {
          expect(String(result.cause)).toContain("provider-side rollback");
        }
        yield* adapter.stopSession(threadId);
      } finally {
        yield* Effect.promise(() => NodeFSP.rm(baseDir, { recursive: true, force: true }));
      }
    }),
  );
});

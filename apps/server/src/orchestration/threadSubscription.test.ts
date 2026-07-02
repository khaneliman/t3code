import {
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationEvent,
  type OrchestrationThread,
  type OrchestrationThreadDetailSnapshot,
} from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";

import {
  makeThreadSubscriptionStream,
  type ThreadSubscriptionSources,
} from "./threadSubscription.ts";

const threadId = ThreadId.make("thread-1");
const now = "2026-01-01T00:00:00.000Z";

const thread: OrchestrationThread = {
  id: threadId,
  projectId: ProjectId.make("project-1"),
  title: "Thread",
  modelSelection: {
    instanceId: ProviderInstanceId.make("codex"),
    model: "gpt-5.4",
  },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  latestTurn: null,
  createdAt: now,
  updatedAt: now,
  archivedAt: null,
  deletedAt: null,
  messages: [],
  proposedPlans: [],
  activities: [],
  checkpoints: [],
  session: null,
};

function messageSent(sequence: number, targetThreadId: ThreadId = threadId): OrchestrationEvent {
  return {
    sequence,
    eventId: EventId.make(`event-${sequence}`),
    aggregateKind: "thread",
    aggregateId: targetThreadId,
    occurredAt: now,
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
    type: "thread.message-sent",
    payload: {
      threadId: targetThreadId,
      messageId: MessageId.make(`message-${sequence}`),
      role: "user",
      text: `message ${sequence}`,
      turnId: null,
      streaming: false,
      createdAt: now,
      updatedAt: now,
    },
  };
}

function eventSequences(items: ReadonlyArray<{ kind: string; event?: OrchestrationEvent }>) {
  return items.flatMap((item) =>
    item.kind === "event" && item.event ? [item.event.sequence] : [],
  );
}

function makeHarness(options: {
  readonly snapshotSequence: number;
  readonly persistedEvents?: ReadonlyArray<OrchestrationEvent>;
  /** Published to the hot channel while the snapshot read is in flight. */
  readonly publishDuringSnapshotRead?: ReadonlyArray<OrchestrationEvent>;
}) {
  return Effect.gen(function* () {
    const pubsub = yield* PubSub.unbounded<OrchestrationEvent>();
    const sources: ThreadSubscriptionSources = {
      subscribeDomainEvents: PubSub.subscribe(pubsub),
      readEvents: (fromSequenceExclusive) =>
        Stream.fromIterable(
          (options.persistedEvents ?? []).filter((event) => event.sequence > fromSequenceExclusive),
        ),
      getThreadDetailSnapshotById: () =>
        Effect.gen(function* () {
          for (const event of options.publishDuringSnapshotRead ?? []) {
            yield* PubSub.publish(pubsub, event);
          }
          return Option.some({
            snapshotSequence: options.snapshotSequence,
            thread,
          } satisfies OrchestrationThreadDetailSnapshot);
        }),
    };
    return { pubsub, stream: makeThreadSubscriptionStream(sources, threadId) };
  });
}

describe("makeThreadSubscriptionStream", () => {
  it.effect(
    "does not lose an event committed after the snapshot read starts but before the live stream runs",
    () =>
      Effect.gen(function* () {
        // The event is only on the hot channel: it committed too late for the
        // snapshot and the persisted replay has not caught up either.
        const { stream } = yield* makeHarness({
          snapshotSequence: 4,
          publishDuringSnapshotRead: [messageSent(5)],
        });

        const items = yield* Stream.runCollect(Stream.take(stream, 2)).pipe(
          Effect.map((chunk) => Array.from(chunk)),
        );

        assert.equal(items[0]?.kind, "snapshot");
        assert.deepEqual(eventSequences(items), [5]);
      }),
  );

  it.effect("emits replayed events before buffered live events and dedupes overlaps", () =>
    Effect.gen(function* () {
      // 5 and 6 are persisted after the snapshot; 6 and 7 also land on the hot
      // channel while the snapshot read is in flight.
      const { stream } = yield* makeHarness({
        snapshotSequence: 4,
        persistedEvents: [messageSent(5), messageSent(6)],
        publishDuringSnapshotRead: [messageSent(6), messageSent(7)],
      });

      const items = yield* Stream.runCollect(Stream.take(stream, 4)).pipe(
        Effect.map((chunk) => Array.from(chunk)),
      );

      assert.equal(items[0]?.kind, "snapshot");
      assert.deepEqual(eventSequences(items), [5, 6, 7]);
    }),
  );

  it.effect("drops live events the snapshot already covers", () =>
    Effect.gen(function* () {
      const { stream } = yield* makeHarness({
        snapshotSequence: 4,
        publishDuringSnapshotRead: [messageSent(3), messageSent(4), messageSent(5)],
      });

      const items = yield* Stream.runCollect(Stream.take(stream, 2)).pipe(
        Effect.map((chunk) => Array.from(chunk)),
      );

      assert.deepEqual(eventSequences(items), [5]);
    }),
  );

  it.effect("ignores events for other threads", () =>
    Effect.gen(function* () {
      const otherThreadId = ThreadId.make("thread-2");
      const { stream } = yield* makeHarness({
        snapshotSequence: 4,
        persistedEvents: [messageSent(5, otherThreadId)],
        publishDuringSnapshotRead: [messageSent(6, otherThreadId), messageSent(7)],
      });

      const items = yield* Stream.runCollect(Stream.take(stream, 2)).pipe(
        Effect.map((chunk) => Array.from(chunk)),
      );

      assert.deepEqual(eventSequences(items), [7]);
    }),
  );

  it.effect("fails with a snapshot error when the thread does not exist", () =>
    Effect.gen(function* () {
      const pubsub = yield* PubSub.unbounded<OrchestrationEvent>();
      const stream = makeThreadSubscriptionStream(
        {
          subscribeDomainEvents: PubSub.subscribe(pubsub),
          readEvents: () => Stream.empty,
          getThreadDetailSnapshotById: () => Effect.succeed(Option.none()),
        },
        threadId,
      );

      const failure = yield* Stream.runCollect(stream).pipe(Effect.flip);

      assert.equal(failure._tag, "OrchestrationGetSnapshotError");
      assert.include(failure.message, "was not found");
    }),
  );
});

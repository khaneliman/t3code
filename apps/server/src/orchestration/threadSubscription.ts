/**
 * threadSubscription - Consistent thread detail subscription stream.
 *
 * `subscribeThread` must never lose a thread event between the snapshot read
 * and the moment the live event stream starts running. This module builds the
 * stream with a race-free ordering:
 *
 * 1. subscribe to the hot domain-event channel (events landing from here on
 *    are buffered in the subscription),
 * 2. read the thread detail together with its snapshot sequence in one
 *    transaction,
 * 3. replay persisted thread detail events above the snapshot sequence,
 * 4. flush the buffered/live subscription,
 *
 * deduplicating replayed and live events by sequence so an event that appears
 * in both phases is emitted exactly once, in order.
 *
 * @module threadSubscription
 */
import {
  OrchestrationGetSnapshotError,
  type OrchestrationEvent,
  type OrchestrationThreadDetailSnapshot,
  type OrchestrationThreadStreamItem,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type * as PubSub from "effect/PubSub";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

export function isThreadDetailEvent(event: OrchestrationEvent): event is Extract<
  OrchestrationEvent,
  {
    type:
      | "thread.message-sent"
      | "thread.proposed-plan-upserted"
      | "thread.activity-appended"
      | "thread.turn-diff-completed"
      | "thread.reverted"
      | "thread.session-set";
  }
> {
  return (
    event.type === "thread.message-sent" ||
    event.type === "thread.proposed-plan-upserted" ||
    event.type === "thread.activity-appended" ||
    event.type === "thread.turn-diff-completed" ||
    event.type === "thread.reverted" ||
    event.type === "thread.session-set"
  );
}

export interface ThreadSubscriptionSources {
  /**
   * Eager, scoped subscription to the hot domain-event channel. Must be
   * registered with the underlying PubSub before the effect resolves so
   * events published during the snapshot read are buffered, not lost.
   */
  readonly subscribeDomainEvents: Effect.Effect<
    PubSub.Subscription<OrchestrationEvent>,
    never,
    Scope.Scope
  >;
  /** Replay persisted events from an exclusive sequence cursor. */
  readonly readEvents: (
    fromSequenceExclusive: number,
  ) => Stream.Stream<OrchestrationEvent, unknown>;
  /** Transactionally consistent thread detail + snapshot sequence. */
  readonly getThreadDetailSnapshotById: (
    threadId: ThreadId,
  ) => Effect.Effect<Option.Option<OrchestrationThreadDetailSnapshot>, unknown>;
}

export function makeThreadSubscriptionStream(
  sources: ThreadSubscriptionSources,
  threadId: ThreadId,
): Stream.Stream<OrchestrationThreadStreamItem, OrchestrationGetSnapshotError> {
  return Stream.unwrap(
    Effect.gen(function* () {
      const subscription = yield* sources.subscribeDomainEvents;

      const detailSnapshot = yield* sources.getThreadDetailSnapshotById(threadId).pipe(
        Effect.mapError(
          (cause) =>
            new OrchestrationGetSnapshotError({
              message: `Failed to load thread ${threadId}`,
              cause,
            }),
        ),
      );
      if (Option.isNone(detailSnapshot)) {
        return yield* new OrchestrationGetSnapshotError({
          message: `Thread ${threadId} was not found`,
          cause: threadId,
        });
      }
      const snapshot = detailSnapshot.value;

      const isRelevant = (event: OrchestrationEvent) =>
        event.aggregateKind === "thread" &&
        event.aggregateId === threadId &&
        isThreadDetailEvent(event);

      // Sequences are monotonic and replay drains before the subscription, so
      // a single cursor drops both the replay/live overlap and any buffered
      // event the snapshot already covers. Per-run mutable state: the closure
      // is re-created by Stream.unwrap on every subscription run.
      let lastEmittedSequence = snapshot.snapshotSequence;
      const notYetEmitted = (event: OrchestrationEvent) => {
        if (event.sequence <= lastEmittedSequence) {
          return false;
        }
        lastEmittedSequence = event.sequence;
        return true;
      };

      const replayStream = sources.readEvents(snapshot.snapshotSequence).pipe(
        Stream.mapError(
          (cause) =>
            new OrchestrationGetSnapshotError({
              message: `Failed to replay events for thread ${threadId}`,
              cause,
            }),
        ),
      );

      return Stream.concat(
        Stream.make({
          kind: "snapshot" as const,
          snapshot,
        }),
        Stream.concat(replayStream, Stream.fromSubscription(subscription)).pipe(
          Stream.filter(isRelevant),
          Stream.filter(notYetEmitted),
          Stream.map((event) => ({
            kind: "event" as const,
            event,
          })),
        ),
      );
    }),
  );
}

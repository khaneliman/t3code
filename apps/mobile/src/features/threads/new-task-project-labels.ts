import type { EnvironmentId } from "@t3tools/contracts";

import type { SavedRemoteConnection } from "../../lib/connection";
import type { HomeProjectScope } from "../home/homeThreadList";

export type EnvironmentLabelResolver = (environmentId: EnvironmentId) => string | null;

/**
 * Resolves the environment label shown next to a project row. A single saved
 * connection cannot be confused with another one, so labels stay hidden until
 * the device has at least two environments.
 */
export function buildEnvironmentLabelResolver(
  savedConnectionsById: Readonly<Record<string, SavedRemoteConnection>>,
): EnvironmentLabelResolver {
  if (Object.keys(savedConnectionsById).length < 2) {
    return () => null;
  }
  return (environmentId) => savedConnectionsById[environmentId]?.environmentLabel ?? null;
}

/**
 * Joins the distinct environment labels behind a collapsed repository group so
 * the row reports which environments own its workspaces.
 */
export function scopeEnvironmentLabel(
  scope: HomeProjectScope,
  resolveEnvironmentLabel: EnvironmentLabelResolver,
): string | null {
  const labels: string[] = [];
  for (const project of scope.projects) {
    const label = resolveEnvironmentLabel(project.environmentId);
    if (label !== null && !labels.includes(label)) {
      labels.push(label);
    }
  }
  return labels.length === 0 ? null : labels.join(", ");
}

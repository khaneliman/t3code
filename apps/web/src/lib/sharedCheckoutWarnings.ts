import type {
  EnvironmentId,
  OrchestrationSessionStatus,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";

interface SharedCheckoutThread {
  readonly environmentId: EnvironmentId;
  readonly id: ThreadId;
  readonly projectId: ProjectId;
  readonly title: string;
  readonly worktreePath: string | null;
  readonly session: { readonly status: OrchestrationSessionStatus } | null;
}

interface SharedCheckoutProject {
  readonly environmentId: EnvironmentId;
  readonly id: ProjectId;
  readonly workspaceRoot: string;
}

export interface SharedCheckoutWarning {
  readonly cwd: string;
  readonly overlappingThreadIds: ReadonlyArray<ThreadId>;
  readonly overlappingThreadTitles: ReadonlyArray<string>;
}

function normalizeCwd(cwd: string | null | undefined): string | null {
  const normalized = cwd?.trim().replace(/\\/g, "/").replace(/\/+$/, "");
  return normalized && normalized.length > 0 ? normalized : null;
}

function projectKey(environmentId: EnvironmentId, projectId: ProjectId): string {
  return `${environmentId}:${projectId}`;
}

function isRunningSession(thread: SharedCheckoutThread): boolean {
  return thread.session?.status === "running" || thread.session?.status === "starting";
}

function resolveThreadCwd(
  thread: SharedCheckoutThread,
  projectsByKey: ReadonlyMap<string, SharedCheckoutProject>,
): string | null {
  return normalizeCwd(
    thread.worktreePath ??
      projectsByKey.get(projectKey(thread.environmentId, thread.projectId))?.workspaceRoot,
  );
}

export function resolveSharedCheckoutWarning(input: {
  readonly activeThread: SharedCheckoutThread;
  readonly threads: ReadonlyArray<SharedCheckoutThread>;
  readonly projects: ReadonlyArray<SharedCheckoutProject>;
}): SharedCheckoutWarning | null {
  if (input.activeThread.worktreePath !== null) {
    return null;
  }

  const projectsByKey = new Map(
    input.projects.map((project) => [projectKey(project.environmentId, project.id), project]),
  );
  const activeCwd = resolveThreadCwd(input.activeThread, projectsByKey);
  if (activeCwd === null) {
    return null;
  }

  const overlapping = input.threads.filter((thread) => {
    if (
      thread.environmentId !== input.activeThread.environmentId ||
      thread.id === input.activeThread.id
    ) {
      return false;
    }
    if (thread.worktreePath !== null || !isRunningSession(thread)) {
      return false;
    }
    return resolveThreadCwd(thread, projectsByKey) === activeCwd;
  });

  if (overlapping.length === 0) {
    return null;
  }

  return {
    cwd: activeCwd,
    overlappingThreadIds: overlapping.map((thread) => thread.id),
    overlappingThreadTitles: overlapping.map((thread) => thread.title),
  };
}

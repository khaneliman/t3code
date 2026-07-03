import { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { resolveSharedCheckoutWarning } from "./sharedCheckoutWarnings";

const environmentId = EnvironmentId.make("environment-1");
const projectId = ProjectId.make("project-1");

function makeProject(
  overrides: Partial<Parameters<typeof resolveSharedCheckoutWarning>[0]["projects"][number]> = {},
) {
  return {
    environmentId,
    id: projectId,
    workspaceRoot: "/repo",
    ...overrides,
  };
}

function makeThread(
  id: string,
  overrides: Partial<Parameters<typeof resolveSharedCheckoutWarning>[0]["threads"][number]> = {},
) {
  return {
    environmentId,
    id: ThreadId.make(id),
    projectId,
    title: id,
    worktreePath: null,
    session: { status: "running" as const },
    ...overrides,
  };
}

describe("resolveSharedCheckoutWarning", () => {
  it("warns when two running local checkout sessions share a cwd", () => {
    const activeThread = makeThread("thread-1");
    const otherThread = makeThread("thread-2", { title: "Other thread" });

    expect(
      resolveSharedCheckoutWarning({
        activeThread,
        threads: [activeThread, otherThread],
        projects: [makeProject()],
      }),
    ).toEqual({
      cwd: "/repo",
      overlappingThreadIds: [ThreadId.make("thread-2")],
      overlappingThreadTitles: ["Other thread"],
    });
  });

  it("does not warn for separate worktrees", () => {
    const activeThread = makeThread("thread-1", { worktreePath: "/repo/worktrees/a" });
    const otherThread = makeThread("thread-2", { worktreePath: "/repo/worktrees/b" });

    expect(
      resolveSharedCheckoutWarning({
        activeThread,
        threads: [activeThread, otherThread],
        projects: [makeProject()],
      }),
    ).toBeNull();
  });

  it("does not warn for the same thread", () => {
    const activeThread = makeThread("thread-1");

    expect(
      resolveSharedCheckoutWarning({
        activeThread,
        threads: [activeThread],
        projects: [makeProject()],
      }),
    ).toBeNull();
  });
});

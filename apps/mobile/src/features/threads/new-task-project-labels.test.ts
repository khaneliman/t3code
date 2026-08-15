import { EnvironmentId, ProjectId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import type { EnvironmentProject } from "@t3tools/client-runtime/state/shell";
import type { SavedRemoteConnection } from "../../lib/connection";
import type { HomeProjectScope } from "../home/homeThreadList";
import { buildEnvironmentLabelResolver, scopeEnvironmentLabel } from "./new-task-project-labels";

function makeProject(environmentId: string, id: string): EnvironmentProject {
  return {
    environmentId: EnvironmentId.make(environmentId),
    id: ProjectId.make(id),
    title: id,
    workspaceRoot: `/work/${id}`,
    repositoryIdentity: null,
    defaultModelSelection: null,
    scripts: [],
    createdAt: "2026-07-01T00:00:00.000Z",
    updatedAt: "2026-07-01T00:00:00.000Z",
  };
}

function makeScope(projects: ReadonlyArray<EnvironmentProject>): HomeProjectScope {
  return {
    key: "github.com/t3tools/t3code",
    title: "T3 Code",
    representative: projects[0]!,
    projects,
    projectRefs: projects.map((project) => ({
      environmentId: project.environmentId,
      projectId: project.id,
    })),
  };
}

function makeConnection(environmentId: string, label: string): SavedRemoteConnection {
  return {
    environmentId: EnvironmentId.make(environmentId),
    environmentLabel: label,
    pairingUrl: "",
    displayUrl: "",
    httpBaseUrl: "",
    wsBaseUrl: "",
    bearerToken: null,
  };
}

const laptop = makeConnection("laptop", "MacBook Pro");
const desktop = makeConnection("desktop", "Linux desktop");

describe("buildEnvironmentLabelResolver", () => {
  it("hides labels while only one environment is saved", () => {
    const resolve = buildEnvironmentLabelResolver({ laptop });
    expect(resolve(EnvironmentId.make("laptop"))).toBeNull();
  });

  it("labels each environment once a second one is saved", () => {
    const resolve = buildEnvironmentLabelResolver({ laptop, desktop });
    expect(resolve(EnvironmentId.make("laptop"))).toBe("MacBook Pro");
    expect(resolve(EnvironmentId.make("desktop"))).toBe("Linux desktop");
  });

  it("returns null for an environment that has no saved connection", () => {
    const resolve = buildEnvironmentLabelResolver({ laptop, desktop });
    expect(resolve(EnvironmentId.make("relay"))).toBeNull();
  });
});

describe("scopeEnvironmentLabel", () => {
  it("lists the distinct environments behind a collapsed group", () => {
    const scope = makeScope([
      makeProject("laptop", "t3code"),
      makeProject("desktop", "t3code-2"),
      makeProject("desktop", "t3code-3"),
    ]);
    expect(scopeEnvironmentLabel(scope, buildEnvironmentLabelResolver({ laptop, desktop }))).toBe(
      "MacBook Pro, Linux desktop",
    );
  });

  it("returns null when no member environment resolves a label", () => {
    const scope = makeScope([makeProject("laptop", "t3code")]);
    expect(scopeEnvironmentLabel(scope, buildEnvironmentLabelResolver({ laptop }))).toBeNull();
  });
});

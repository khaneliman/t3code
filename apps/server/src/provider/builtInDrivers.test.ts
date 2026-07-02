import { describe, expect, it } from "vite-plus/test";
import { ProviderDriverKind } from "@t3tools/contracts";

import { BUILT_IN_DRIVERS } from "./builtInDrivers.ts";

describe("BUILT_IN_DRIVERS", () => {
  it("includes Antigravity", () => {
    expect(BUILT_IN_DRIVERS.map((driver) => driver.driverKind)).toContain(
      ProviderDriverKind.make("antigravity"),
    );
  });
});

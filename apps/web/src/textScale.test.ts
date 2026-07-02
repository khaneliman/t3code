import { afterEach, describe, expect, it, vi } from "vite-plus/test";

function createLocalStorageStub(): Storage {
  const store = new Map<string, string>();
  return {
    getItem: (key) => store.get(key) ?? null,
    setItem: (key, value) => {
      store.set(key, value);
    },
    removeItem: (key) => {
      store.delete(key);
    },
    clear: () => store.clear(),
    key: (index) => [...store.keys()][index] ?? null,
    get length() {
      return store.size;
    },
  };
}

function stubWindow() {
  const localStorage = createLocalStorageStub();
  const testWindow = {
    localStorage,
  } as Window & typeof globalThis;
  vi.stubGlobal("window", testWindow);
  vi.stubGlobal("localStorage", localStorage);
  return testWindow;
}

function stubDocument() {
  const setProperty = vi.fn();
  const documentElement = {
    dataset: {} as Record<string, string>,
    style: {
      fontSize: "",
      setProperty,
    },
  };
  vi.stubGlobal("document", { documentElement });
  return { documentElement, setProperty };
}

afterEach(() => {
  vi.resetModules();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("textScale", () => {
  it("parses valid text scale values", async () => {
    const { parseTextScale } = await import("./textScale");

    expect(parseTextScale(80)).toBe(80);
    expect(parseTextScale("150")).toBe(150);
    expect(parseTextScale(200)).toBe(200);
  });

  it("rejects invalid text scale values", async () => {
    const { parseTextScale } = await import("./textScale");

    expect(parseTextScale(79)).toBeNull();
    expect(parseTextScale(201)).toBeNull();
    expect(parseTextScale(100.5)).toBeNull();
    expect(parseTextScale("abc")).toBeNull();
    expect(parseTextScale(null)).toBeNull();
  });

  it("resolves bootstrap scale from desktop sync before browser storage", async () => {
    const testWindow = stubWindow();
    testWindow.localStorage.setItem(
      "t3code:client-settings:v1",
      JSON.stringify({ textScale: 120 }),
    );
    testWindow.localStorage.setItem("t3code:text-scale:v1", "110");
    testWindow.desktopBridge = { getInitialTextScale: () => 150 } as never;

    const { resolveInitialTextScale } = await import("./textScale");

    expect(resolveInitialTextScale()).toBe(150);
  });

  it("resolves bootstrap scale from client storage before the mirror", async () => {
    const testWindow = stubWindow();
    testWindow.localStorage.setItem(
      "t3code:client-settings:v1",
      JSON.stringify({ textScale: 130 }),
    );
    testWindow.localStorage.setItem("t3code:text-scale:v1", "110");

    const { resolveInitialTextScale } = await import("./textScale");

    expect(resolveInitialTextScale()).toBe(130);
  });

  it("resolves bootstrap scale from mirror before default", async () => {
    const testWindow = stubWindow();
    testWindow.localStorage.setItem("t3code:text-scale:v1", "175");

    const { resolveInitialTextScale } = await import("./textScale");

    expect(resolveInitialTextScale()).toBe(175);
  });

  it("falls back to default text scale", async () => {
    stubWindow();
    const { resolveInitialTextScale } = await import("./textScale");

    expect(resolveInitialTextScale()).toBe(100);
  });

  it("applies document scale attributes", async () => {
    const { documentElement, setProperty } = stubDocument();
    const { applyDocumentTextScale } = await import("./textScale");

    applyDocumentTextScale(150);

    expect(documentElement.style.fontSize).toBe("150%");
    expect(setProperty).toHaveBeenCalledWith("--t3-text-scale-factor", "1.5");
    expect(documentElement.dataset.textScale).toBe("150");
  });
});

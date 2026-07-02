import {
  DEFAULT_TEXT_SCALE,
  MAX_TEXT_SCALE,
  MIN_TEXT_SCALE,
  type TextScale,
} from "@t3tools/contracts/settings";

export const CLIENT_SETTINGS_STORAGE_KEY = "t3code:client-settings:v1";
export const TEXT_SCALE_STORAGE_KEY = "t3code:text-scale:v1";

export function isTextScale(value: unknown): value is TextScale {
  return (
    typeof value === "number" &&
    Number.isInteger(value) &&
    value >= MIN_TEXT_SCALE &&
    value <= MAX_TEXT_SCALE
  );
}

export function parseTextScale(value: unknown): TextScale | null {
  if (isTextScale(value)) {
    return value;
  }
  if (typeof value !== "string" || value.trim().length === 0) {
    return null;
  }
  const parsed = Number(value);
  return isTextScale(parsed) ? parsed : null;
}

export function applyDocumentTextScale(scale: TextScale): void {
  if (typeof document === "undefined") {
    return;
  }
  const root = document.documentElement;
  root.style.fontSize = `${scale}%`;
  root.style.setProperty("--t3-text-scale-factor", String(scale / 100));
  root.dataset.textScale = String(scale);
}

function readLocalStorageValue(key: string): string | null {
  if (typeof window === "undefined") {
    return null;
  }
  try {
    return window.localStorage.getItem(key);
  } catch {
    return null;
  }
}

function readClientSettingsTextScale(): TextScale | null {
  const raw = readLocalStorageValue(CLIENT_SETTINGS_STORAGE_KEY);
  if (!raw) {
    return null;
  }
  try {
    const parsed: unknown = JSON.parse(raw);
    if (typeof parsed !== "object" || parsed === null || !("textScale" in parsed)) {
      return null;
    }
    return parseTextScale(parsed.textScale);
  } catch {
    return null;
  }
}

function readTextScaleMirror(): TextScale | null {
  return parseTextScale(readLocalStorageValue(TEXT_SCALE_STORAGE_KEY));
}

export function writeTextScaleMirror(scale: TextScale): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.setItem(TEXT_SCALE_STORAGE_KEY, String(scale));
  } catch {
    // Text scale persistence is best-effort; full client settings remain authoritative.
  }
}

export function resolveInitialTextScale(): TextScale {
  if (typeof window === "undefined") {
    return DEFAULT_TEXT_SCALE;
  }

  const desktopTextScale = parseTextScale(window.desktopBridge?.getInitialTextScale?.());
  if (desktopTextScale !== null) {
    return desktopTextScale;
  }

  const clientSettingsTextScale = readClientSettingsTextScale();
  if (clientSettingsTextScale !== null) {
    return clientSettingsTextScale;
  }

  return readTextScaleMirror() ?? DEFAULT_TEXT_SCALE;
}

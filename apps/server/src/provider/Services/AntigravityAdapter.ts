/**
 * AntigravityAdapter - shape type for the Antigravity provider adapter.
 *
 * The driver model bundles one adapter per instance as captured closures,
 * so this module only keeps the named shape interface.
 *
 * @module AntigravityAdapter
 */
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

export interface AntigravityAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}

# Antigravity

T3 Code can use the local Antigravity CLI as a provider.

## Requirements

Install and authenticate Antigravity so this works in a terminal:

```bash
agy --version
agy models
```

T3 Code uses the hidden agent API through:

```bash
agy agentapi ...
```

`agentapi` needs an active Antigravity language server. T3 Code can still run CLI-only with
`agy --print` when no language server is available. In CLI-only mode, T3 Code cannot drive the
daemon-only approval APIs. No desktop app is required for CLI-only mode.

## Settings

Default paths:

```text
Home path      ~/.gemini/antigravity-cli
Brain path     ~/.gemini/antigravity-cli/brain
Settings path  ~/.gemini/antigravity-cli/settings.json
```

Linux supports auto-detecting an active language server from `/proc` when one exists.

macOS and Windows may need these only for daemon-backed approval APIs:

```text
Language server address  http://127.0.0.1:<port>
CSRF token               active daemon token
```

T3 Code never logs CSRF tokens.

## Current Limits

Account switching is not implemented for Antigravity.

Provider-side rollback is not supported yet because `agy agentapi` does not expose a revert API.
Use normal source control rollback instead.

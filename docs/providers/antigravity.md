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

The agent API also needs an active Antigravity daemon/language server. Open Antigravity before
starting a T3 Code session, or configure the language server details manually.

## Settings

Default paths:

```text
Home path      ~/.gemini/antigravity-cli
Brain path     ~/.gemini/antigravity-cli/brain
Settings path  ~/.gemini/antigravity-cli/settings.json
```

Linux supports auto-detecting the active language server from `/proc`.

macOS and Windows may need:

```text
Language server address  http://127.0.0.1:<port>
CSRF token               active daemon token
```

T3 Code never logs CSRF tokens.

## Current Limits

Account switching is not implemented for Antigravity.

Provider-side rollback is not supported yet because `agy agentapi` does not expose a revert API.
Use normal source control rollback instead.

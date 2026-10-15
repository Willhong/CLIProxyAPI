# Strict conversation routing with stock clients

The proxy pins a credential to an authenticated caller and conversation in bbolt, without TTL or account failover. BB, Hermes, Claude Code and Codex do not need source changes. Prior patched-client experiments are not the adopted integration.

## Build and initialize

```sh
GOMAXPROCS=4 go build -p 4 -o ./cli-proxy-api-strict ./cmd/server
GOMAXPROCS=4 go build -p 4 -o ./init-conversation-bindings ./cmd/init-conversation-bindings
./init-conversation-bindings /absolute/private/state/conversation-bindings.db
```

Create the private parent directory first. The initializer creates a 0600 DB, refuses existing files, and stores hashed caller/conversation keys and credential IDs, not tokens. Preserve this DB, the proxy client API key and credential identities across restarts. Do not reinitialize the DB or reuse an existing credential ID for a different account and expect old bindings to remain valid. Embedded SDK users can supply a trusted caller via `WithConversationCaller`.

```yaml
routing:
  strategy: round-robin
  strict-conversation-affinity: true
  conversation-bindings-file: /absolute/private/state/conversation-bindings.db
```

Bindings commit before upstream dispatch and are shared safely by concurrent processes. Missing/corrupt DBs and unavailable/deleted credentials fail closed. Changing to a model/provider the assigned credential cannot serve also fails rather than switching accounts. Home, plugin scheduler/executor and raw SDK HTTP bypass paths are unsupported in strict mode; Antigravity credit fallback is not entered.

## Existing client settings only

- **Codex:** configure the normal Responses provider base URL to `<proxy-origin>/v1` and use the proxy client key. Stock Codex sends `Session-Id` automatically; do not add a fixed custom conversation header. Resume the saved Codex session normally.
- **Claude Code:** set its supported `ANTHROPIC_BASE_URL=<proxy-origin>` and proxy client authentication. Stock Claude Code sends `X-Claude-Code-Session-Id` automatically. Resume normally; no wrapper or custom identity header is required.
- **Hermes:** retain its existing provider configuration and add the supported per-provider affinity-header setting. The tested configuration is below. Supply the client key through the normal protected configuration rather than publishing it.

```yaml
model:
  provider: my-proxy
  default: your-configured-model
providers:
  my-proxy:
    api: http://127.0.0.1:18318/v1
    transport: chat_completions
    default_model: your-configured-model
    session_affinity_header: X-Hermes-Session-ID
    # api_key: supply using the existing protected credential setup
```

The source checkout tested supports this providers schema. Other Hermes versions must expose the same existing setting; do not silently patch an older client. The header is derived by Hermes itself, not a constant copied across conversations. In BB, the original native provider/ACP adapters remain unchanged and use the respective clients' normal configuration.

The proxy namespaces these values as `codex:`, `claude:` and `hermes:`. `Session_id` is also accepted for compatible Codex transports. Missing/duplicate/conflicting identity headers reject; unknown clients are not guessed from content. `X-CLIProxy-Conversation-ID` remains an advanced explicit override for compatibility, but stock-client tests do not inject it. Proxy-specific and Hermes affinity headers are removed upstream.

## What is guaranteed, and what is not

Normal stock CLI resume and Hermes ACP load retained the native ID in actual HTTP captures. Real client and proxy processes were restarted and retained the previously assigned non-default account. Stock Hermes manual `/compress` also preserved its header. HTTP/SSE and WebSocket error/reconnect tests use the same durable selection boundary.

If saved conversation identity is lost and the client starts a new ID, the proxy sees a new conversation. It cannot prove that this was recovery of an older conversation. No prompt-similarity guessing or automatic cross-ID rebinding is implemented. Full live BB host restart and automatic Hermes compaction are not claimed by the current test evidence. This does not prove live subscription acceptance, cache hits, token renewal or billing.

## Tests

```sh
GOMAXPROCS=4 go test -p 4 ./sdk/cliproxy/auth ./sdk/cliproxy ./sdk/api/handlers/... ./internal/config
GOMAXPROCS=4 go test -race -p 4 ./sdk/cliproxy/auth ./sdk/api/handlers/openai -run 'TestStrict(Stock|Conversation)' -count=1
```

`cli-proxy-console/scripts/verify_stock_proxy_e2e.py` drives actual installed stock Codex/Claude and stock Hermes ACP through the compiled proxy to local synthetic Gemini accounts. Its `stock_probes/` helpers are test drivers only, not a production wrapper requirement. Every run uses fresh isolated state. A separate warm-up conversation ensures the tested account differs from cold-start selection before both processes restart. Test configuration and synthetic responses are not production credentials or provider results.

See `cli-proxy-console/docs/STOCK-CONVERSATION-HANDOFF.md` for exact tested versions, evidence paths, commands and outstanding live adoption checks.

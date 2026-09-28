# agent-lifecycle-events

An ADK plugin that publishes an agent's lifecycle events to webhooks and/or Kafka, **without any change to the agent's code**. The agent doesn't know who consumes the events. A BPMN engine, a monitor or anything else can subscribe.

## Attaching it to an agent

Install this package where the agent runs, then add the plugin when starting ADK:

```bash
uv run --with-editable ./agent-lifecycle-events \
  adk web --extra_plugins agent_lifecycle.LifecyclePlugin
# or: adk api_server --extra_plugins agent_lifecycle.LifecyclePlugin
```

For Kafka, install the extra: `--with-editable "./agent-lifecycle-events[kafka]"`.

If the agent is served with a custom `Runner`/`App` instead of the `adk` CLI, the owners have to add `LifecyclePlugin()` to its `plugins` list. That's one line, but it's a change on their side.

> ADK only **logs** a plugin that fails to load and then carries on. If no events appear, check the server log for `Agent lifecycle events enabled` or `Failed to load plugin`.

## Events

| Type | When |
|---|---|
| `agent.started` | A run begins with a new user message |
| `agent.waiting` | A long-running tool started work that finishes outside the run (e.g. a human approval); the agent is paused |
| `agent.resumed` | The long-running tool's result arrived and the agent continues |
| `agent.completed` | The run finished with nothing left pending |
| `agent.failed` | The run failed (model, tool or run error) |

Envelope, identical for every sink:

```json
{
  "id": "evt_3f9c…",
  "seq": 2,
  "type": "agent.waiting",
  "time": "2026-09-24T10:15:02.123+00:00",
  "source": "human_in_the_loop",
  "session_id": "…",
  "user_id": "…",
  "invocation_id": "…",
  "correlation": { "processInstanceId": "pi-111" },
  "data": { … }
}
```

| Type | `data` |
|---|---|
| `agent.started` | `input` |
| `agent.waiting` | `tool`, `call_id`, `args`, `result` (what the tool returned, e.g. `{"status": "pending", …}`) |
| `agent.resumed` | `tool`, `call_id`, `result` (the final result, e.g. `{"status": "approved", …}`), `outcome` |
| `agent.completed` | `outcome`, `guardrail` (details if one blocked the run, else `null`), `response` (final text), `tool_results` (final result of each long-running tool call in the run) |
| `agent.failed` | `stage` (`model`/`tool`/`run`), `error_type`, `error_message`, `tool` (for tool errors). Technical failures only; guardrail blocks are reported as `agent.completed` |

### What the plugin can and can't know

- It sees **generic** agent mechanics. `agent.waiting` means "a long-running tool is pending". It doesn't know *why* (a human, a batch job…); `data.tool` and `data.result` tell you.
- Business outcomes (e.g. approved vs. rejected) come from the tool's own data. The plugin surfaces them as `data.outcome` only when told which field to read (see "Outcome" below). It never interprets them.
- `agent.completed` means the run ended with nothing pending. **It also fires when the agent ends a turn by asking the user a question** (e.g. "what's the reason for the expense?"). Telling those apart requires the agent's cooperation.
- If whatever resumes the agent (e.g. an approval system's webhook) fails to reach it, the agent never runs again, so no further event is sent. Consumers need a timeout for `agent.waiting`.

## Outcome

To branch on a result (e.g. approved vs. rejected) without digging into tool payloads, tell the plugin which field of a long-running tool's result holds the outcome:

```
LIFECYCLE_OUTCOME_FROM=request_approval.status   # "<tool name>.<dot path into the result>"
LIFECYCLE_OUTCOME_FROM=*.status                  # any long-running tool
```

`agent.resumed` and `agent.completed` then carry `data.outcome` (for this agent: `"approved"` or `"rejected"`). It's `null` when not configured, or when the run never got that far. For example, a run where the agent only asked a clarifying question completes with `outcome: null`. So consumers should branch on three cases: the expected values **and** `null`.

A run can set its own `outcome_from` in its `lifecycle` state (below), which takes priority over the environment variable.

The outcome reflects what the **tool** returned (here, the human's decision). A decision made by the agent itself, in its own words, isn't visible to the plugin; that would need the agent to expose it.

So consumers branch on: the configured values (e.g. `approved` / `rejected`), **`guardrail_blocked`** (see below), and **`null`**.

## Guardrails

When a guardrail stops a run, it ends with `agent.completed`, `data.outcome = "guardrail_blocked"`, and details in `data.guardrail`:

```json
"outcome": "guardrail_blocked",
"guardrail": { "source": "model", "reason": "SAFETY", "message": "Finished with SAFETY" }
```

It's reported as `completed`, not `failed`: it's a policy decision, and retrying won't change it. `guardrail_blocked` always takes priority over the configured outcome.

| Guardrail | Detected? | `source` / `reason` |
|---|---|---|
| Model provider blocks the **output** (OpenAI content filter, Gemini safety, blocklist, SPII, recitation…) | ✅ automatically | `model` / `SAFETY`, `PROHIBITED_CONTENT`, `BLOCKLIST`, `SPII`, `RECITATION`, … |
| Model provider blocks the **input** (prompt rejected: LiteLLM `ContentPolicyViolationError`, Gemini `JAILBREAK`, Model Armor…) | ✅ automatically | `model` / `CONTENT_POLICY`, `JAILBREAK`, `MODEL_ARMOR`, … |
| A guardrail **plugin** deployed next to the agent | ✅ if it calls `report_guardrail` | whatever it reports |
| The agent declining in its own words, or guardrail callbacks inside the agent's code | ❌ looks like a normal reply | needs the agent team to call `report_guardrail` |

Reporting from your own guardrail plugin (attach it with `--extra_plugins` like this one):

```python
from agent_lifecycle import report_guardrail

class PiiGuardrail(BasePlugin):
    async def before_model_callback(self, *, callback_context, llm_request):
        if contains_ssn(llm_request):
            report_guardrail(callback_context.invocation_id, "pii_filter", "SSN_DETECTED", "Prompt contained an SSN")
            return LlmResponse(content=...)  # the blocked reply
```

Note: when the provider blocks the **input**, ADK also answers the caller of `/run` with HTTP 500, because the run raised an error. The lifecycle event is still `agent.completed` with `guardrail_blocked`.

## Passing correlation and routing per run

Whoever starts a run puts a `lifecycle` object in the ADK session state. The plugin copies `correlation` back unchanged on every event and never reads inside it.

```bash
# POST /apps/{app}/users/{user}/sessions/{session_id}: the body IS the initial state
curl -X POST localhost:8000/apps/human_in_the_loop/users/u1/sessions/s1 \
  -H 'content-type: application/json' \
  -d '{"lifecycle": {
        "correlation": {"processInstanceId": "pi-111"},
        "callback_url": "https://flow.example.com/agent-events",
        "kafka_topic": "expense-agent.events",
        "outcome_from": "request_approval.status"
      }}'
```

`callback_url` and `kafka_topic` are optional per-run overrides. **They're only used if allowed** by `LIFECYCLE_WEBHOOK_ALLOWED_HOSTS` / `LIFECYCLE_KAFKA_ALLOWED_TOPICS`; otherwise they're ignored with a warning and the defaults apply. This prevents anyone who can create a session from making the agent send requests to arbitrary hosts.

Sessions without a `lifecycle` object still get events, with an empty `correlation`, as long as a default target is configured.

## Configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `LIFECYCLE_SINKS` | *(unset = disabled)* | `webhook`, `kafka`, or `webhook,kafka` |
| `LIFECYCLE_WEBHOOK_URL` | | Default callback URL |
| `LIFECYCLE_WEBHOOK_SECRET` | | If set, each request is signed (see below) |
| `LIFECYCLE_WEBHOOK_ALLOWED_HOSTS` | | Comma-separated hosts allowed as per-run `callback_url` |
| `LIFECYCLE_WEBHOOK_TIMEOUT_SECONDS` | `10` | Per-request timeout |
| `LIFECYCLE_KAFKA_BOOTSTRAP_SERVERS` | | e.g. `localhost:9092` |
| `LIFECYCLE_KAFKA_TOPIC` | | Default topic |
| `LIFECYCLE_KAFKA_ALLOWED_TOPICS` | | Comma-separated topics allowed as per-run `kafka_topic` |
| `LIFECYCLE_KAFKA_SECURITY_PROTOCOL` / `_SASL_MECHANISM` / `_SASL_USERNAME` / `_SASL_PASSWORD` | | Secured clusters |
| `LIFECYCLE_OUTBOX_PATH` | `.agent_lifecycle/outbox.db` | Durable outbox (SQLite) |
| `LIFECYCLE_OUTCOME_FROM` | | Field to surface as `data.outcome`, e.g. `request_approval.status` |
| `LIFECYCLE_MAX_RETRY_SECONDS` | `86400` | Give up on an event after this long |
| `LIFECYCLE_MAX_BACKOFF_SECONDS` | `3600` | Cap on the delay between retries |

## Sinks

**Webhook:** `POST` of the event JSON with headers `X-Lifecycle-Event-Id`, `X-Lifecycle-Event-Type` and, if a secret is set, `X-Lifecycle-Signature: sha256=<hex HMAC-SHA256 of the raw body>`. Any `2xx` counts as delivered. `408`/`425`/`429`/`5xx` and network errors are retried. Other `4xx` responses are treated as permanent rejections and not retried.

**Kafka:** one message per event. The key is `session_id`, so all events of a run land on one partition, in order. The value is the event JSON, and headers follow the CloudEvents Kafka binding (`ce_id`, `ce_type`, `ce_source`, `ce_time`, `ce_subject`, `ce_specversion`). The producer uses `acks=all` and idempotence.

## Delivery guarantees

- **At least once.** Every event is written to a durable local outbox **before** it's sent, and only marked delivered once the sink confirms it. Failures are retried with exponential backoff (1s, 2s, 4s … capped at `LIFECYCLE_MAX_BACKOFF_SECONDS`) until `LIFECYCLE_MAX_RETRY_SECONDS`, then marked `undeliverable` and logged.
- **In order per run and sink.** An event isn't sent until the previous one for the same session and sink is delivered (or given up on). `seq` increases 1, 2, 3… per session.
- **Never breaks the agent.** Recording or delivery problems are logged, and the agent carries on.

**Consumers must:**
1. **Ignore duplicates by `id`.** At-least-once delivery means an event can arrive twice (e.g. when the consumer's `2xx` reply is lost).
2. **Use `seq` to detect gaps.** If `seq` 4 arrives without 3, 3 is still on its way or was given up on.
3. **Have a timeout** for runs that go quiet (see "What the plugin can and can't know").

### After a restart

Undelivered events survive restarts in the outbox. However, ADK loads plugins only when the agent handles its **first request**, so the plugin resumes delivery then. To deliver leftovers immediately, or to keep a dedicated sender running, use the relay. It's safe to run next to the agent server: deliveries are leased, so the two never send the same event at the same time.

```bash
python -m agent_lifecycle relay --once   # deliver what's due now, then exit
python -m agent_lifecycle relay          # run continuously
```

The outbox is a local SQLite file, so this assumes **one agent server per outbox file**. For several replicas, give each its own `LIFECYCLE_OUTBOX_PATH` on persistent storage.

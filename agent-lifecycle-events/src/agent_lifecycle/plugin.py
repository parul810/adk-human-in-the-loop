"""ADK plugin that publishes agent lifecycle events without changes to the agent.

Attach it at startup:
    adk web --extra_plugins agent_lifecycle.LifecyclePlugin
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from google.adk.plugins.base_plugin import BasePlugin

from .config import Config
from .dispatcher import Dispatcher
from .outbox import Outbox
from .sinks import build_sinks

logger = logging.getLogger(__name__)

STARTED = "agent.started"
WAITING = "agent.waiting"
RESUMED = "agent.resumed"
COMPLETED = "agent.completed"
FAILED = "agent.failed"

# data.outcome when a guardrail stopped the run
GUARDRAIL_BLOCKED = "guardrail_blocked"

# Model finish/block reasons that mean a provider's safety or policy filter
# stopped the response (LiteLLM maps OpenAI's "content_filter" to SAFETY)
_GUARDRAIL_REASONS = {
    "SAFETY", "BLOCKLIST", "PROHIBITED_CONTENT", "SPII", "RECITATION", "JAILBREAK", "MODEL_ARMOR",
    "IMAGE_SAFETY", "IMAGE_PROHIBITED_CONTENT", "IMAGE_RECITATION",
}

# invocation id -> guardrail details. Module-level so any plugin can report into it.
_guardrails: dict[str, dict[str, Any]] = {}


def report_guardrail(invocation_id: str, name: str, reason: str | None = None, message: str | None = None) -> None:
    """Record that a guardrail blocked an invocation. The run's agent.completed
    event then carries outcome "guardrail_blocked" and these details.

    Call it from a guardrail plugin or callback, e.g.
        report_guardrail(callback_context.invocation_id, "pii_filter", "SSN in prompt")
    """
    _guardrails.setdefault(invocation_id, {"source": name, "reason": reason, "message": message})


def _reason_name(code: Any) -> str:
    return getattr(code, "name", None) or str(code).rsplit(".", 1)[-1]

# One outbox and sender per process, shared by all plugin instances (ADK
# creates one instance per agent app)
_shared: dict[str, tuple[Outbox, Dispatcher]] = {}


def _shared_for(config: Config) -> tuple[Outbox, Dispatcher]:
    if config.outbox_path not in _shared:
        outbox = Outbox(config.outbox_path)
        _shared[config.outbox_path] = (outbox, Dispatcher(config, outbox, build_sinks(config)))
    return _shared[config.outbox_path]


def _text(content: Any) -> str | None:
    parts = getattr(content, "parts", None) or []
    text = "".join(p.text for p in parts if getattr(p, "text", None) and not getattr(p, "thought", False))
    return text or None


class LifecyclePlugin(BasePlugin):
    def __init__(self, name: str = "agent_lifecycle", config: Config | None = None):
        super().__init__(name=name)
        self.config = config or Config.from_env()
        self.enabled = bool(self.config.sinks)
        # invocation id -> failure details seen before the run ended
        self._failures: dict[str, dict[str, Any]] = {}
        if not self.enabled:
            logger.warning("Agent lifecycle events disabled: LIFECYCLE_SINKS is not set")
            return
        self._outbox, self._dispatcher = _shared_for(self.config)
        # Resume redelivery of anything left over from a previous process
        self._dispatcher.ensure_started()
        logger.info(
            "Agent lifecycle events enabled: sinks=%s webhook=%s kafka_topic=%s outbox=%s",
            ",".join(self.config.sinks), self.config.webhook_url, self.config.kafka_topic, self.config.outbox_path,
        )

    # ----- lifecycle detection -----

    async def on_user_message_callback(self, *, invocation_context, user_message):
        responses = [p.function_response for p in (user_message.parts or []) if p.function_response]
        if responses:
            for fr in responses:
                result = {"tool": fr.name, "call_id": fr.id, "result": fr.response}
                self._emit(
                    invocation_context,
                    RESUMED,
                    {**result, "outcome": self._outcome(invocation_context, [result])},
                )
        else:
            self._emit(invocation_context, STARTED, {"input": _text(user_message)})
        return None

    async def after_tool_callback(self, *, tool, tool_args, tool_context, result):
        # A long-running tool has started work that finishes outside this run
        if getattr(tool, "is_long_running", False):
            self._emit(
                tool_context,
                WAITING,
                {"tool": tool.name, "call_id": tool_context.function_call_id, "args": tool_args, "result": result},
            )
        return None

    async def on_model_error_callback(self, *, callback_context, llm_request, error):
        # LiteLLM raises ContentPolicyViolationError when the provider blocks the input
        if any("ContentPolicy" in cls.__name__ for cls in type(error).__mro__):
            report_guardrail(callback_context.invocation_id, "model", "CONTENT_POLICY", str(error))
        else:
            self._record_failure(callback_context.invocation_id, "model", error)
        return None

    async def on_tool_error_callback(self, *, tool, tool_args, tool_context, error):
        self._record_failure(tool_context.invocation_id, "tool", error, tool=tool.name)
        return None

    async def on_event_callback(self, *, invocation_context, event):
        # Some model errors, including safety blocks, arrive as error events rather than exceptions
        if getattr(event, "error_code", None):
            reason = _reason_name(event.error_code)
            if reason in _GUARDRAIL_REASONS:
                report_guardrail(invocation_context.invocation_id, "model", reason, event.error_message)
            else:
                self._failures.setdefault(
                    invocation_context.invocation_id,
                    {"stage": "model", "error_type": reason, "error_message": event.error_message},
                )
        return None

    async def on_run_error_callback(self, *, invocation_context, error):
        invocation_id = invocation_context.invocation_id
        guardrail = _guardrails.pop(invocation_id, None)
        failure = self._failures.pop(invocation_id, None)
        if guardrail is not None:
            # A policy block, not a technical failure: retrying won't help
            self._emit_completed(invocation_context, guardrail)
            return
        if failure is None:
            failure = {"stage": "run", "error_type": type(error).__name__, "error_message": str(error)}
        self._emit(invocation_context, FAILED, failure)

    async def after_run_callback(self, *, invocation_context):
        invocation_id = invocation_context.invocation_id
        guardrail = _guardrails.pop(invocation_id, None)
        failure = self._failures.pop(invocation_id, None)
        if guardrail is not None:
            self._emit_completed(invocation_context, guardrail)
        elif failure is not None:
            self._emit(invocation_context, FAILED, failure)
        elif not self._pending_long_running_calls(invocation_context.session.events):
            self._emit_completed(invocation_context)
        # else: paused on a long-running tool; agent.waiting was already sent

    async def close(self) -> None:
        if self.enabled:
            await self._dispatcher.close()

    # ----- helpers -----

    def _emit_completed(self, ctx, guardrail: dict[str, Any] | None = None) -> None:
        events = ctx.session.events
        tool_results = self._long_running_results(events, ctx.invocation_id)
        self._emit(
            ctx,
            COMPLETED,
            {
                "outcome": GUARDRAIL_BLOCKED if guardrail else self._outcome(ctx, tool_results),
                "guardrail": guardrail,
                "response": self._final_response(events, ctx.invocation_id),
                "tool_results": tool_results,
            },
        )

    def _record_failure(self, invocation_id: str, stage: str, error: Exception, **extra: Any) -> None:
        self._failures.setdefault(
            invocation_id,
            {"stage": stage, "error_type": type(error).__name__, "error_message": str(error), **extra},
        )

    def _outcome(self, ctx, tool_results: list[dict[str, Any]]) -> Any:
        """Read the configured outcome field from the latest matching tool result."""
        try:
            routing = ctx.session.state.get(self.config.state_key) or {}
            spec = routing.get("outcome_from") or self.config.outcome_from
            if not spec:
                return None
            tool, _, path = spec.partition(".")
            for entry in reversed(tool_results):
                if tool not in ("*", entry["tool"]):
                    continue
                value = entry["result"]
                for key in path.split(".") if path else ():
                    value = value.get(key) if isinstance(value, dict) else None
                if value is not None:
                    return value
        except Exception:
            logger.exception("Failed to read lifecycle outcome")
        return None

    @staticmethod
    def _pending_long_running_calls(events) -> set[str]:
        started, answered = set(), set()
        for event in events:
            started.update(event.long_running_tool_ids or ())
            if event.author == "user":
                answered.update(fr.id for fr in (event.get_function_responses() or []))
        return started - answered

    @staticmethod
    def _final_response(events, invocation_id: str) -> str | None:
        for event in reversed(events):
            if event.invocation_id == invocation_id and event.author != "user":
                text = _text(event.content)
                if text:
                    return text
        return None

    @staticmethod
    def _long_running_results(events, invocation_id: str) -> list[dict[str, Any]]:
        """Final result of each long-running tool call made in this invocation."""
        calls = {}
        for event in events:
            if event.invocation_id != invocation_id:
                continue
            for fc in event.get_function_calls() or []:
                if fc.id in (event.long_running_tool_ids or ()):
                    calls[fc.id] = {"tool": fc.name, "call_id": fc.id, "result": None}
        for event in events:
            for fr in event.get_function_responses() or []:
                if fr.id in calls:
                    calls[fr.id]["result"] = fr.response  # later responses replace "pending"
        return list(calls.values())

    def _targets(self, routing: dict[str, Any]) -> list[tuple[str, str]]:
        targets = []
        if "webhook" in self.config.sinks:
            url = self.config.webhook_url
            override = routing.get("callback_url")
            if override:
                if urlparse(override).hostname in self.config.webhook_allowed_hosts:
                    url = override
                else:
                    logger.warning("Ignoring callback_url %s: host not in LIFECYCLE_WEBHOOK_ALLOWED_HOSTS", override)
            if url:
                targets.append(("webhook", url))
        if "kafka" in self.config.sinks:
            topic = self.config.kafka_topic
            override = routing.get("kafka_topic")
            if override:
                if override in self.config.kafka_allowed_topics:
                    topic = override
                else:
                    logger.warning("Ignoring kafka_topic %s: not in LIFECYCLE_KAFKA_ALLOWED_TOPICS", override)
            if topic:
                targets.append(("kafka", topic))
        return targets

    def _emit(self, ctx, event_type: str, data: dict[str, Any]) -> None:
        """Store the event in the outbox; the dispatcher delivers it. Never raises."""
        if not self.enabled:
            return
        try:
            session = ctx.session
            routing = session.state.get(self.config.state_key) or {}
            targets = self._targets(routing)
            if not targets:
                return
            event = {
                "id": f"evt_{uuid.uuid4().hex}",
                "type": event_type,
                "time": datetime.now(timezone.utc).isoformat(),
                "source": session.app_name,
                "session_id": session.id,
                "user_id": session.user_id,
                "invocation_id": ctx.invocation_id,
                "correlation": routing.get("correlation") or {},
                "data": data,
            }
            session_key = f"{session.app_name}/{session.user_id}/{session.id}"
            self._outbox.append(session_key, event, targets)
            self._dispatcher.notify()
        except Exception:
            logger.exception("Failed to record lifecycle event %s", event_type)

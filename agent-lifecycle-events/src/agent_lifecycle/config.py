"""Configuration for lifecycle event publishing, read from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _csv(value: str | None) -> tuple[str, ...]:
    return tuple(v.strip() for v in (value or "").split(",") if v.strip())


@dataclass(frozen=True)
class Config:
    # Which sinks are enabled: "webhook", "kafka", or both
    sinks: tuple[str, ...] = ()

    # Webhook sink
    webhook_url: str | None = None
    webhook_secret: str | None = None
    # Hosts a session may choose via state["lifecycle"]["callback_url"];
    # per-session URLs are ignored unless their host is listed here
    webhook_allowed_hosts: frozenset[str] = frozenset()
    webhook_timeout_seconds: float = 10.0

    # Kafka sink
    kafka_bootstrap_servers: str | None = None
    kafka_topic: str | None = None
    # Topics a session may choose via state["lifecycle"]["kafka_topic"]
    kafka_allowed_topics: frozenset[str] = frozenset()
    kafka_client_options: dict = field(default_factory=dict)

    # Durable outbox and redelivery
    outbox_path: str = ".agent_lifecycle/outbox.db"
    max_retry_seconds: float = 24 * 3600
    max_backoff_seconds: float = 3600

    # Where to read a business outcome from a long-running tool's result:
    # "<tool name>.<dot path>" (e.g. "request_approval.status"), or "*.<path>"
    # for any tool. Surfaced as data.outcome; can be overridden per run.
    outcome_from: str | None = None

    # Session state key callers use to pass correlation and routing
    state_key: str = "lifecycle"

    @classmethod
    def from_env(cls) -> "Config":
        env = os.environ
        kafka_options = {
            key: env[var]
            for key, var in {
                "security_protocol": "LIFECYCLE_KAFKA_SECURITY_PROTOCOL",
                "sasl_mechanism": "LIFECYCLE_KAFKA_SASL_MECHANISM",
                "sasl_plain_username": "LIFECYCLE_KAFKA_SASL_USERNAME",
                "sasl_plain_password": "LIFECYCLE_KAFKA_SASL_PASSWORD",
            }.items()
            if env.get(var)
        }
        return cls(
            sinks=tuple(s.lower() for s in _csv(env.get("LIFECYCLE_SINKS"))),
            webhook_url=env.get("LIFECYCLE_WEBHOOK_URL") or None,
            webhook_secret=env.get("LIFECYCLE_WEBHOOK_SECRET") or None,
            webhook_allowed_hosts=frozenset(_csv(env.get("LIFECYCLE_WEBHOOK_ALLOWED_HOSTS"))),
            webhook_timeout_seconds=float(env.get("LIFECYCLE_WEBHOOK_TIMEOUT_SECONDS", 10)),
            kafka_bootstrap_servers=env.get("LIFECYCLE_KAFKA_BOOTSTRAP_SERVERS") or None,
            kafka_topic=env.get("LIFECYCLE_KAFKA_TOPIC") or None,
            kafka_allowed_topics=frozenset(_csv(env.get("LIFECYCLE_KAFKA_ALLOWED_TOPICS"))),
            kafka_client_options=kafka_options,
            outbox_path=env.get("LIFECYCLE_OUTBOX_PATH", cls.outbox_path),
            max_retry_seconds=float(env.get("LIFECYCLE_MAX_RETRY_SECONDS", cls.max_retry_seconds)),
            max_backoff_seconds=float(env.get("LIFECYCLE_MAX_BACKOFF_SECONDS", cls.max_backoff_seconds)),
            outcome_from=env.get("LIFECYCLE_OUTCOME_FROM") or None,
        )

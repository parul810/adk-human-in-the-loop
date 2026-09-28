"""Delivery targets. A sink either returns (delivered), raises
PermanentDeliveryError (retrying won't help), or raises anything else (retry)."""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any, Protocol

import httpx

from .config import Config


class PermanentDeliveryError(Exception):
    """The target rejected the event; retrying will not help."""


class Sink(Protocol):
    name: str

    async def send(self, target: str, event: dict[str, Any], payload: str) -> None: ...

    async def close(self) -> None: ...


# 4xx responses that are worth retrying (timeouts, rate limits)
_RETRYABLE_4XX = {408, 425, 429}


class WebhookSink:
    """POSTs the event JSON to a URL. Any 2xx counts as delivered."""

    name = "webhook"

    def __init__(self, config: Config):
        self._secret = config.webhook_secret
        self._client = httpx.AsyncClient(timeout=config.webhook_timeout_seconds)

    async def send(self, target: str, event: dict[str, Any], payload: str) -> None:
        body = payload.encode()
        headers = {
            "Content-Type": "application/json",
            "X-Lifecycle-Event-Id": event["id"],
            "X-Lifecycle-Event-Type": event["type"],
        }
        if self._secret:
            signature = hmac.new(self._secret.encode(), body, hashlib.sha256).hexdigest()
            headers["X-Lifecycle-Signature"] = f"sha256={signature}"
        response = await self._client.post(target, content=body, headers=headers)
        if 400 <= response.status_code < 500 and response.status_code not in _RETRYABLE_4XX:
            raise PermanentDeliveryError(f"HTTP {response.status_code}: {response.text[:200]}")
        response.raise_for_status()

    async def close(self) -> None:
        await self._client.aclose()


class KafkaSink:
    """Produces the event to a topic, keyed by session so a run's events stay
    in order on one partition. CloudEvents metadata goes in the headers."""

    name = "kafka"

    def __init__(self, config: Config):
        self._config = config
        self._producer = None

    async def _get_producer(self):
        if self._producer is None:
            from aiokafka import AIOKafkaProducer  # optional dependency

            producer = AIOKafkaProducer(
                bootstrap_servers=self._config.kafka_bootstrap_servers,
                enable_idempotence=True,
                acks="all",
                **self._config.kafka_client_options,
            )
            await producer.start()
            self._producer = producer
        return self._producer

    async def send(self, target: str, event: dict[str, Any], payload: str) -> None:
        producer = await self._get_producer()
        headers = [
            ("ce_specversion", b"1.0"),
            ("ce_id", event["id"].encode()),
            ("ce_type", event["type"].encode()),
            ("ce_source", event["source"].encode()),
            ("ce_time", event["time"].encode()),
            ("ce_subject", event["session_id"].encode()),
            ("content-type", b"application/json"),
        ]
        try:
            await producer.send_and_wait(
                target, value=payload.encode(), key=event["session_id"].encode(), headers=headers
            )
        except Exception:
            # Start from a fresh producer on the next attempt
            await self.close()
            raise

    async def close(self) -> None:
        if self._producer is not None:
            producer, self._producer = self._producer, None
            try:
                await producer.stop()
            except Exception:
                pass


def build_sinks(config: Config) -> dict[str, Sink]:
    sinks: dict[str, Sink] = {}
    if "webhook" in config.sinks:
        sinks["webhook"] = WebhookSink(config)
    if "kafka" in config.sinks:
        sinks["kafka"] = KafkaSink(config)
    return sinks


def dumps(event: dict[str, Any]) -> str:
    return json.dumps(event, default=str)

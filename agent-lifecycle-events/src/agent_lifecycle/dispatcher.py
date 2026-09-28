"""Background sender that drains the outbox with exponential backoff."""

from __future__ import annotations

import asyncio
import logging
import random
import time

from .config import Config
from .outbox import Delivery, Outbox
from .sinks import PermanentDeliveryError, Sink

logger = logging.getLogger(__name__)

# Upper bound on idle sleep, so deliveries queued by another process are picked up
_IDLE_POLL_SECONDS = 5.0


class Dispatcher:
    def __init__(self, config: Config, outbox: Outbox, sinks: dict[str, Sink]):
        self._config = config
        self._outbox = outbox
        self._sinks = sinks
        self._wake: asyncio.Event | None = None
        self._task: asyncio.Task | None = None

    def ensure_started(self) -> None:
        """Start the sender on the running event loop, if not already running there."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if self._task is None or self._task.done() or self._task.get_loop() is not loop:
            self._wake = asyncio.Event()
            self._task = loop.create_task(self._run(), name="agent-lifecycle-dispatcher")

    def notify(self) -> None:
        self.ensure_started()
        if self._wake is not None:
            self._wake.set()

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        for sink in self._sinks.values():
            await sink.close()

    async def _run(self) -> None:
        while True:
            try:
                deliveries = self._outbox.due()
                for delivery in deliveries:
                    await self._deliver(delivery)
                if deliveries:
                    continue
                next_due = self._outbox.next_due_at()
                timeout = _IDLE_POLL_SECONDS if next_due is None else max(0.0, min(next_due - time.time(), _IDLE_POLL_SECONDS))
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout)
                except asyncio.TimeoutError:
                    pass
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Lifecycle dispatcher loop error")
                await asyncio.sleep(1)

    async def _deliver(self, d: Delivery) -> None:
        sink = self._sinks.get(d.sink)
        if sink is None:
            self._outbox.mark_undeliverable(d, f"sink '{d.sink}' is not enabled")
            return
        try:
            await sink.send(d.target, d.event, d.payload)
        except PermanentDeliveryError as e:
            logger.error("Lifecycle event %s rejected by %s %s: %s", d.event_id, d.sink, d.target, e)
            self._outbox.mark_undeliverable(d, str(e))
            return
        except Exception as e:
            error = f"{type(e).__name__}: {e}"
            if time.time() - d.created_at >= self._config.max_retry_seconds:
                logger.error("Giving up on lifecycle event %s to %s %s: %s", d.event_id, d.sink, d.target, error)
                self._outbox.mark_undeliverable(d, error)
                return
            # Exponential backoff with jitter: 1s, 2s, 4s ... capped
            delay = min(2 ** d.attempts, self._config.max_backoff_seconds) * random.uniform(0.8, 1.2)
            logger.warning(
                "Lifecycle event %s to %s %s failed (attempt %d), retrying in %.0fs: %s",
                d.event_id, d.sink, d.target, d.attempts + 1, delay, error,
            )
            self._outbox.mark_retry(d, time.time() + delay, error)
            return
        self._outbox.mark_delivered(d)

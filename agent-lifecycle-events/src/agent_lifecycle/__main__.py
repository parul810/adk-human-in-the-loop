"""Standalone relay that delivers events from the outbox.

The plugin delivers events itself, but ADK only loads plugins when an agent
handles its first request. Run the relay to deliver events left over from
before a restart right away, or keep it running next to the agent server.

    python -m agent_lifecycle relay          # run continuously
    python -m agent_lifecycle relay --once   # deliver what's due, then exit
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from .config import Config
from .dispatcher import Dispatcher
from .outbox import Outbox
from .sinks import build_sinks


async def _relay(once: bool) -> int:
    config = Config.from_env()
    if not config.sinks:
        raise SystemExit("LIFECYCLE_SINKS is not set")
    outbox = Outbox(config.outbox_path)
    dispatcher = Dispatcher(config, outbox, build_sinks(config))
    try:
        if not once:
            dispatcher.ensure_started()
            await asyncio.Event().wait()  # until interrupted
        while deliveries := outbox.due():
            for delivery in deliveries:
                await dispatcher._deliver(delivery)
        return outbox.pending_count()
    finally:
        await dispatcher.close()


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m agent_lifecycle")
    sub = parser.add_subparsers(dest="command", required=True)
    relay = sub.add_parser("relay", help="deliver events from the outbox")
    relay.add_argument("--once", action="store_true", help="deliver what is due now, then exit")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        remaining = asyncio.run(_relay(args.once))
    except KeyboardInterrupt:
        return
    logging.info("Relay finished; %d deliveries still pending (waiting to retry)", remaining)


if __name__ == "__main__":
    main()

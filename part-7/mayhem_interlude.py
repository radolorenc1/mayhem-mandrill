#!/usr/bin/env python3.7
# Copyright (c) 2018-2019 Lynn Root
"""
Profiling asyncio code - base code
Notice! This requires:
 - attrs==19.1.0
 - aiologger==0.4.0
"""

import asyncio
import functools
import logging
import random
import signal
import string
import uuid

import attr
import aiologger


LOG_FMT_STR = "%(asctime)s,%(msecs)d %(levelname)s: %(message)s"
LOG_DATEFMT_STR = "%H:%M:%S"
aio_formatter = aiologger.formatters.base.Formatter(
    fmt=LOG_FMT_STR, datefmt=LOG_DATEFMT_STR,
)
logger = aiologger.Logger.with_default_handlers(formatter=aio_formatter)

# for the non-coroutine functions
logging.basicConfig(format=LOG_FMT_STR, datefmt=LOG_DATEFMT_STR)


@attr.s
class PubSubMessage:
    instance_name = attr.ib()
    message_id    = attr.ib(repr=False)
    hostname      = attr.ib(repr=False, init=False)
    restarted     = attr.ib(repr=False, default=False)
    saved         = attr.ib(repr=False, default=False)
    acked         = attr.ib(repr=False, default=False)
    extended_cnt  = attr.ib(repr=False, default=0)

    def __attrs_post_init__(self):
        self.hostname = f"{self.instance_name}.example.net"


class RestartFailed(Exception):
    pass


async def publish(queue):
    choices = string.ascii_lowercase + string.digits

    while True:
        msg_id = str(uuid.uuid4())
        host_id = "".join(random.choices(choices, k=4))
        instance_name = f"cattle-{host_id}"
        msg = PubSubMessage(message_id=msg_id, instance_name=instance_name)
        await logger.debug(f"Published message {msg}")
        asyncio.create_task(queue.put(msg))
        await asyncio.sleep(random.random())


async def restart_host(msg):
    await asyncio.sleep(random.random())
    if random.randrange(1, 5) == 3:
        raise RestartFailed(f"Could not restart {msg.hostname}")
    msg.restarted = True
    await logger.info(f"Restarted {msg.hostname}")


async def save(msg):
    await asyncio.sleep(random.random())
    if random.randrange(1, 5) == 3:
        raise Exception(f"Could not save {msg}")
    msg.saved = True
    await logger.info(f"Saved {msg} into database")


async def cleanup(msg, event):
    await event.wait()
    await asyncio.sleep(random.random())
    msg.acked = True
    await logger.info(f"Done. Acked {msg}")


async def extend(msg, event):
    while not event.is_set():
        msg.extended_cnt += 1
        await logger.info(f"Extended deadline by 3 seconds for {msg}")
        await asyncio.sleep(2)


def handle_results(results, msg):
    for result in results:
        if isinstance(result, RestartFailed):
            logging.error(f"Retrying for failure to restart: {msg.hostname}")
        elif isinstance(result, Exception):
            logging.error(f"Handling general error: {result}")


async def handle_message(msg):
    event = asyncio.Event()

    asyncio.create_task(extend(msg, event))
    asyncio.create_task(cleanup(msg, event))

    results = await asyncio.gather(
        save(msg), restart_host(msg), return_exceptions=True
    )
    handle_results(results, msg)
    event.set()


async def consume(queue):
    while True:
        msg = await queue.get()
        await logger.info(f"Pulled {msg}")
        asyncio.create_task(handle_message(msg))


def handle_exception(loop, context):
    msg = context.get("exception", context["message"])
    logging.error(f"Caught exception: {msg}")
    logging.info("Shutting down...")
    asyncio.create_task(shutdown(loop))


async def shutdown(loop, signal=None):
    if signal:
        await logger.info(f"Received exit signal {signal.name}...")
    await logger.info("Closing database connections")
    await logger.info("Nacking outstanding messages")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

    [task.cancel() for task in tasks]

    await logger.info("Cancelling outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    await logger.info(f"Flushing metrics")
    await logger.info(f"Shutting down aiologger")
    await logger.shutdown()
    loop.stop()


def main():
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(loop, signal=s))
        )
    loop.set_exception_handler(handle_exception)
    queue = asyncio.Queue()

    try:
        loop.create_task(publish(queue))
        loop.create_task(consume(queue))
        loop.run_forever()
    finally:
        loop.close()
        logging.info("Successfully shutdown the Mayhem service.")



if __name__ == "__main__":
    main()
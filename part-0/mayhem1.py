#!/usr/bin/env python3.9
"""
Notice! This requires: 
 - attrs==19.1.0
"""

import asyncio
import logging
import random
import string

import attr


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s,%(msecs)d %(levelname)s: %(message)s',
    datefmt='%H:%M:%S',
)


@attr.s
class PubSubMessage:
    instance_name = attr.ib()
    message_id    = attr.ib(repr=False)
    hostname      = attr.ib(repr=False, init=False)

    def __attrs_post_init__(self):
        self.hostname = f'{self.instance_name}.example.net'


# simulating an external publisher of events
async def publish(queue, n):
    choices = string.ascii_lowercase + string.digits

    for x in range(1, n + 1):
        host_id = ''.join(random.choices(choices, k=4))
        instance_name = f'cattle-{host_id}'
        msg = PubSubMessage(message_id=x, instance_name=f'cattle-{host_id}')
        await queue.put(msg)
        logging.info(f'Published {x} of {n} messages')

    await queue.put(None)  # publisher is done


async def consume(queue):
    while True:
        # wait for an item from the publisher
        msg = await queue.get()
        if msg is None:  # publisher is done
            break

        # process the msg
        logging.info(f'Consumed {msg}')
        # unhelpful simulation of i/o work
        await asyncio.sleep(random.random())


def main():
    queue = asyncio.Queue()
    asyncio.run(publish(queue, 5))
    asyncio.run(consume(queue))


if __name__ == '__main__':
    main()
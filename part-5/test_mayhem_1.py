""
import asyncio

import pytest

import mayhem


@pytest.fixture
def message():
    return mayhem.PubSubMessage(message_id="1234", instance_name="mayhem_test")


def test_save(message):
    assert not message.saved  # sanity check
    asyncio.run(mayhem.save(message))
    assert message.saved
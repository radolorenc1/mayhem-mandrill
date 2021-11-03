""

import asyncio

import pytest

import mayhem


# Note: with pytest, I could put these fixtures in `conftest.py` rather than
# copy-paste it to each test file. But I'd rather be explicit
@pytest.fixture
def message():
    return mayhem.PubSubMessage(message_id="1234", instance_name="mayhem_test")


@pytest.fixture
def create_mock_coro(mocker, monkeypatch):
    """Create a mock-coro pair.
    The coro can be used to patch an async method while the mock can
    be used to assert calls to the mocked out method.
    """

    def _create_mock_coro_pair(to_patch=None):
        mock = mocker.Mock()

        async def _coro(*args, **kwargs):
            return mock(*args, **kwargs)

        if to_patch:
            monkeypatch.setattr(to_patch, _coro)

        return mock, _coro

    return _create_mock_coro_pair


@pytest.fixture
def mock_queue(mocker, monkeypatch):
    queue = mocker.Mock()
    monkeypatch.setattr(mayhem.asyncio, "Queue", queue)
    return queue.return_value


@pytest.fixture
def mock_get(mock_queue, create_mock_coro):
    mock_get, coro_get = create_mock_coro()
    mock_queue.get = coro_get
    return mock_get


@pytest.mark.asyncio
async def test_consume(mock_get, mock_queue, message, create_mock_coro):
    mock_get.side_effect = [message, Exception("break while loop")]
    mock_handle_message, _ = create_mock_coro("mayhem.handle_message")

    with pytest.raises(Exception, match="break while loop"):
        await mayhem.consume(mock_queue)

    ret_tasks = [
        t for t in asyncio.all_tasks() if t is not asyncio.current_task()
    ]
    # should be 1 per side effect minus the Exception (i.e. messages consumed)
    assert 1 == len(ret_tasks)
    mock_handle_message.assert_not_called()  # <-- sanity check

    # explicitly await tasks scheduled by `asyncio.create_task`
    await asyncio.gather(*ret_tasks)

    mock_handle_message.assert_called_once_with(message)
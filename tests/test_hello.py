import logging
import threading
from random import randint
from time import sleep

import pytest

from basic_socket_rpc.rpc_low_level import ProtocolError


logger = logging.getLogger(__name__)
TIMEOUT_SECS = 9


@pytest.fixture(scope="module")
def client(hello_client_module_scope, hello_server_cm):
    with hello_server_cm:
        # give server time to spool up (otherwise, could get
        # ConnectionRefusedError)
        sleep(1)
        with hello_client_module_scope:
            yield hello_client_module_scope
            logger.info("Shutting down client")


def test_hello_goodbye(client):
    client.hello("Robert")
    client.goodbye("Robert")


def test_add(client):
    assert 3050 == client.add_2_words(1000, 2050)


def test_unknown_on_server(client):
    with pytest.raises(ProtocolError, match="Unknown command with id: 10"):
        client.unknown_on_server("Robert")
    client.hello("Robert")


def test_server_rpc_exception(client):
    with pytest.raises(OverflowError, match="int too big to convert"):
        client.add_2_words(0xFFFFFFFF, 0x1000)  # should cause overflow error


def test_concurrent_users_only_one(client, hello_client_gen):
    hello_client = hello_client_gen(timeout_secs=TIMEOUT_SECS, retry_interval_secs=2)
    with pytest.raises(ConnectionError, match=r"Timed out waiting for connection"):
        # This should fail because the server has already been connected to by `client`
        # hello_client here represents a second user attempting to use the
        # server but we have prevented this explicitly in the server spec
        # See conftest.py for how this is done
        hello_client.connect()
    # original client shouldn't be affected
    assert 3050 == client.add_2_words(1000, 2050)


def connect_and_say_hi(client):
    with client:
        client.hello("Robert")


def test_waiting_in_line(client, hello_client_gen):
    hello_client = hello_client_gen(timeout_secs=TIMEOUT_SECS, retry_interval_secs=2)
    client_thread = threading.Thread(target=connect_and_say_hi, args=(hello_client,))
    client_thread.start()
    # hello_client should be blocked now until we disconnect client
    client.close()
    client_thread.join()
    client.connect()
    assert 3050 == client.add_2_words(1000, 2050)


def test_fixed_array_serialization(client):
    to_sum = [0x10, 0x100, 0x22222222]
    expected_response = sum(to_sum)
    assert expected_response == client.sum_fixed_array(to_sum)


def test_array_serialization(client):
    to_sum = [0x10, 0x100, 0x22222222]
    expected_response = sum(to_sum)
    assert expected_response == client.sum_array(to_sum)

    to_sum = [0x10, 0x22222222]
    expected_response = sum(to_sum)
    assert expected_response == client.sum_array(to_sum)


def test_array_and_echo_string(client):
    to_sum = [0x10, 0x22222222]
    expected_sum = sum(to_sum)
    string = "blah blah something"
    assert (expected_sum, string) == client.sum_array_echo_string(to_sum, string)

    # corner case
    to_sum = []
    expected_sum = 0
    string = "blah something"
    assert (expected_sum, string) == client.sum_array_echo_string(to_sum, string)

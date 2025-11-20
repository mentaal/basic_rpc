#!/usr/bin/env python
import argparse
import signal
import sys
from enum import Enum
from threading import Event
from time import sleep

from basic_socket_rpc.rpc_serialization_functions import (
    call_no_args,
    deserialize_str,
    int_to_le_bytes_4,
    make_server_deserializer,
    parse_int_from_le_bytes_4,
    serialize_str,
)
from basic_socket_rpc.rpc_spec import RpcServerResp, RpcServerSpec
from basic_socket_rpc.rpc_threaded_server import make_exclusive_access_server_cm, serve


_finished = Event()


def signal_handler(sig, frame):
    _finished.set()


signal.signal(signal.SIGINT, signal_handler)


class greet_server_cmd_ids(Enum):
    hello = 0
    goodbye = 1
    add_2_words = 2
    func_no_args = 3


exclusive_access_cm = make_exclusive_access_server_cm(
    RpcServerResp(
        cmd_id=greet_server_cmd_ids.hello,
        parse_and_call=make_server_deserializer(deserialize_str),
        serialize_response=serialize_str,
        client_function=lambda name: f"hello {name}",
    ),
    RpcServerResp(
        cmd_id=greet_server_cmd_ids.goodbye,
        parse_and_call=make_server_deserializer(deserialize_str),
        serialize_response=serialize_str,
        client_function=lambda name: f"goodbye {name}",
    ),
    RpcServerResp(
        cmd_id=greet_server_cmd_ids.add_2_words,
        parse_and_call=make_server_deserializer(parse_int_from_le_bytes_4, parse_int_from_le_bytes_4),
        serialize_response=int_to_le_bytes_4,
        client_function=lambda a, b: a + b,
    ),
    RpcServerResp(
        cmd_id=greet_server_cmd_ids.func_no_args,
        parse_and_call=call_no_args,
        serialize_response=lambda _: b"",
        client_function=lambda: print("hello with no args") or None,
    ),
)

if __name__ == "__main__":
    # import logging
    # logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(description="Simple greeting RPC server")
    parser.add_argument(
        "--address",
        default="127.0.0.1",
        help="Address to host service on. Defaults to local host",
    )
    default_port = 11599
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=default_port,
        help=f"Port to host service on defaults to {default_port}",
    )

    p = parser.parse_args()

    print("Server starting...")
    with exclusive_access_cm(host_name=p.address, port=p.port):
        while not (finished := _finished.is_set()):
            _finished.wait(60)
        print("Server shutting down...")

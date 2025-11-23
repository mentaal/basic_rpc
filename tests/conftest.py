"""
The server is synchronous and intended to be used with threads

See rpc_low_level.py for more discussion on the protocol
"""

from enum import Enum
from functools import partial

import pytest

from basic_socket_rpc.rpc_blocking_client import gen_client_class
from basic_socket_rpc.rpc_serialization_functions import (
    call_no_args,
    deserialize_str,
    deserialize_str_only,
    int_from_le_bytes_4,
    int_to_le_bytes_4,
    make_apply,
    make_deserialize_array,
    make_deserialize_array_fixed_size,
    make_deserializer_to_tuple,
    make_serialize_array,
    make_serialize_array_fixed_size,
    make_serializer,
    make_server_deserializer,
    parse_int_from_le_bytes_4,
    parse_no_response,
    serialize_str,
)
from basic_socket_rpc.rpc_spec import RpcClientReq, RpcClientSpec, RpcServerResp
from basic_socket_rpc.rpc_threaded_server import make_exclusive_access_server_cm


HOST_NAME = "127.0.0.1"
PORT = 11599


class greet_server_cmd_ids(Enum):
    hello = 0
    goodbye = 1
    add_2_words = 2
    func_no_args = 3
    sum_fixed_array = 4
    sum_array = 5
    sum_array_echo_string = 6


class greet_client_cmd_ids(Enum):
    hello = 0
    goodbye = 1
    add_2_words = 2
    func_no_args = 3
    sum_fixed_array = 4
    sum_array = 5
    sum_array_echo_string = 6
    unknown_on_server = 10


exclusive_access_cm = make_exclusive_access_server_cm(
    responses=(
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
            cmd_id=greet_server_cmd_ids.sum_fixed_array,
            parse_and_call=make_server_deserializer(
                make_deserialize_array_fixed_size(num_elements=3, element_parser=parse_int_from_le_bytes_4)
            ),
            serialize_response=int_to_le_bytes_4,
            client_function=lambda arr: sum(arr),
        ),
        RpcServerResp(
            cmd_id=greet_server_cmd_ids.sum_array,
            parse_and_call=make_server_deserializer(make_deserialize_array(element_parser=parse_int_from_le_bytes_4)),
            serialize_response=int_to_le_bytes_4,
            client_function=lambda arr: sum(arr),
        ),
        RpcServerResp(
            cmd_id=greet_server_cmd_ids.sum_array_echo_string,
            parse_and_call=make_server_deserializer(
                make_deserialize_array(element_parser=parse_int_from_le_bytes_4),
                deserialize_str,
            ),
            serialize_response=make_apply(make_serializer(int_to_le_bytes_4, serialize_str)),
            client_function=lambda arr, s: (sum(arr), s),
        ),
        RpcServerResp(
            cmd_id=greet_server_cmd_ids.func_no_args,
            parse_and_call=call_no_args,
            serialize_response=lambda _: b"",
            client_function=lambda: print("hello with no args") or None,
        ),
    )
)

greet_client_spec = RpcClientSpec(
    requests=[
        RpcClientReq(
            cmd_id=greet_client_cmd_ids.hello,
            serialize_request=serialize_str,
            parse_response=deserialize_str_only,
        ),
        RpcClientReq(
            cmd_id=greet_client_cmd_ids.goodbye,
            serialize_request=serialize_str,
            parse_response=deserialize_str_only,
        ),
        RpcClientReq(
            cmd_id=greet_client_cmd_ids.add_2_words,
            serialize_request=make_serializer(int_to_le_bytes_4, int_to_le_bytes_4),
            parse_response=int_from_le_bytes_4,
        ),
        RpcClientReq(
            cmd_id=greet_client_cmd_ids.unknown_on_server,
            serialize_request=serialize_str,
            parse_response=deserialize_str_only,
        ),
        RpcClientReq(
            cmd_id=greet_client_cmd_ids.sum_fixed_array,
            serialize_request=make_serialize_array_fixed_size(
                num_elements=3,
                element_serializer=int_to_le_bytes_4,
            ),
            parse_response=int_from_le_bytes_4,
        ),
        RpcClientReq(
            cmd_id=greet_client_cmd_ids.sum_array,
            serialize_request=make_serialize_array(
                element_serializer=int_to_le_bytes_4,
            ),
            parse_response=int_from_le_bytes_4,
        ),
        RpcClientReq(
            cmd_id=greet_client_cmd_ids.sum_array_echo_string,
            serialize_request=make_serializer(
                make_serialize_array(element_serializer=int_to_le_bytes_4),
                serialize_str,
            ),
            parse_response=make_deserializer_to_tuple(parse_int_from_le_bytes_4, deserialize_str),
        ),
        RpcClientReq(
            cmd_id=greet_client_cmd_ids.func_no_args,
            serialize_request=lambda: b"",
            parse_response=parse_no_response,
        ),
    ],
    on_connect=lambda local_data: None,
    on_disconnect=lambda local_data: None,
)


@pytest.fixture(scope="session")
def hello_client_class():
    return gen_client_class(greet_client_spec)


@pytest.fixture(scope="session")
def hello_client_gen(hello_client_class):
    return partial(hello_client_class, host_name=HOST_NAME, port=PORT)


@pytest.fixture
def hello_client(hello_client_class):
    return hello_client_class(host_name=HOST_NAME, port=PORT)


@pytest.fixture(scope="module")
def hello_client_module_scope(hello_client_class):
    return hello_client_class(host_name=HOST_NAME, port=PORT)


@pytest.fixture(scope="session")
def hello_server_cm():
    return exclusive_access_cm(host_name=HOST_NAME, port=PORT)

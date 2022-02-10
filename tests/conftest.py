"""
The server is synchronous and intended to be used with threads

See rpc_low_level.py for more discussion on the protocol
"""
from enum import Enum
import pytest
from functools import partial

from basic_rpc.rpc_spec import RpcServerSpec, RpcServerResp, RpcClientSpec, RpcClientReq
from basic_rpc.rpc_serialization_functions import (serialize_str,
        deserialize_str, deserialize_str_only, make_serializer, make_deserializer,
        make_serialize_array_fixed_size, make_deserialize_array_fixed_size,
        make_serialize_array, make_deserialize_array, make_starmap,
        make_deserializer_to_tuple,
        parse_int_from_le_bytes_4, int_to_le_bytes_4, int_from_le_bytes_4,
        call_no_args, parse_no_response,
)
from basic_rpc.rpc_threaded_server import serve_cm
from basic_rpc.rpc_blocking_client import gen_client_class


HOST_NAME = '127.0.0.1'
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

def single_client_only_connect(shared_data:dict, local_data:dict) -> bool:
    """Only allow a single client to use the RPC service at a time
    :param shared_data: data shared across all connected users
    :param local_data: data local to the connected user
    :returns: A boolean to indicate if a RPC session can continue with this user
    """
    locally_connected_already = local_data['user_connected']
    if locally_connected_already:
        raise ValueError("User attempting to connect but already connected. This shouldn't happen")

    # TODO consider adding more logic here to implement a fair queue for users waiting on service
    user_can_connect = not shared_data['user_connected']
    if user_can_connect:
        shared_data['user_connected'] = True
        local_data['user_connected'] = True
    return user_can_connect

def single_client_only_disconnect(shared_data:dict, local_data:dict) -> bool:
    connected_local = local_data['user_connected']
    connected_shared = shared_data['user_connected']
    owns_session = connected_local and connected_shared
    bad_state = connected_local and not connected_shared
    if bad_state:
        raise ValueError("Bad state: User reports as being connected but shared session data doesn't agree")

    if owns_session:
        shared_data['user_connected'] = False

    local_data['user_connected'] = False

gen_local_data = lambda: {'user_connected': False}

greet_server_spec = RpcServerSpec(
        responses = [
            RpcServerResp(
                cmd_id = greet_server_cmd_ids.hello,
                parse_and_call = make_deserializer(deserialize_str),
                serialize_response = serialize_str,
                client_function = lambda name: f'hello {name}',
            ),
            RpcServerResp(
                cmd_id = greet_server_cmd_ids.goodbye,
                parse_and_call = make_deserializer(deserialize_str),
                serialize_response = serialize_str,
                client_function = lambda name: f'goodbye {name}',
            ),
            RpcServerResp(
                cmd_id = greet_server_cmd_ids.add_2_words,
                parse_and_call = make_deserializer(
                    parse_int_from_le_bytes_4, parse_int_from_le_bytes_4),
                serialize_response = int_to_le_bytes_4,
                client_function = lambda a, b: a + b
            ),
            RpcServerResp(
                cmd_id = greet_server_cmd_ids.sum_fixed_array,
                parse_and_call = make_deserializer(
                    make_deserialize_array_fixed_size(
                        num_elements = 3,
                        element_size = 4,
                        element_parser = parse_int_from_le_bytes_4)),
                serialize_response = int_to_le_bytes_4,
                client_function = lambda arr: sum(arr)
            ),
            RpcServerResp(
                cmd_id = greet_server_cmd_ids.sum_array,
                parse_and_call = make_deserializer(
                    make_deserialize_array(
                        element_size = 4,
                        element_parser = parse_int_from_le_bytes_4)),
                serialize_response = int_to_le_bytes_4,
                client_function = lambda arr: sum(arr)
            ),
            RpcServerResp(
                cmd_id = greet_server_cmd_ids.sum_array_echo_string,
                parse_and_call = make_deserializer(
                    make_deserialize_array(
                        element_size = 4,
                        element_parser = parse_int_from_le_bytes_4),
                    deserialize_str),
                serialize_response = make_starmap(
                    make_serializer(int_to_le_bytes_4, serialize_str)
                    ),
                client_function = lambda arr, s: (sum(arr), s)
            ),
            RpcServerResp(
                cmd_id = greet_server_cmd_ids.func_no_args,
                parse_and_call = call_no_args,
                serialize_response = lambda _: b'',
                client_function = lambda: print('hello with no args') or None
            ),
        ],
        on_server_init = lambda: ({'user_connected':False}, gen_local_data),
        on_client_connect = single_client_only_connect,
        on_client_disconnect = single_client_only_disconnect,
)

greet_client_spec = RpcClientSpec(
        requests = [
            RpcClientReq(
                cmd_id = greet_client_cmd_ids.hello,
                serialize_request = serialize_str,
                parse_response = deserialize_str_only,
            ),
            RpcClientReq(
                cmd_id = greet_client_cmd_ids.goodbye,
                serialize_request = serialize_str,
                parse_response = deserialize_str_only,
            ),
            RpcClientReq(
                cmd_id = greet_client_cmd_ids.add_2_words,
                serialize_request = make_serializer(int_to_le_bytes_4, int_to_le_bytes_4),
                parse_response = int_from_le_bytes_4,
            ),
            RpcClientReq(
                cmd_id = greet_client_cmd_ids.unknown_on_server,
                serialize_request = serialize_str,
                parse_response = deserialize_str_only,
            ),
            RpcClientReq(
                cmd_id = greet_client_cmd_ids.sum_fixed_array,
                serialize_request = make_serialize_array_fixed_size(
                    num_elements = 3,
                    element_size = 4,
                    element_serializer = int_to_le_bytes_4,
                ),
                parse_response = int_from_le_bytes_4,
            ),
            RpcClientReq(
                cmd_id = greet_client_cmd_ids.sum_array,
                serialize_request = make_serialize_array(
                    element_size = 4,
                    element_serializer = int_to_le_bytes_4,
                ),
                parse_response = int_from_le_bytes_4,
            ),
            RpcClientReq(
                cmd_id = greet_client_cmd_ids.sum_array_echo_string,
                serialize_request = make_serializer(
                    make_serialize_array(
                        element_size = 4,
                        element_serializer = int_to_le_bytes_4),
                    serialize_str
                ),
                parse_response = make_deserializer_to_tuple(parse_int_from_le_bytes_4, deserialize_str)
            ),
            RpcClientReq(
                cmd_id = greet_client_cmd_ids.func_no_args,
                serialize_request = lambda: b'',
                parse_response = parse_no_response,
            ),
        ],
        on_connect = lambda local_data: None,
        on_disconnect = lambda local_data: None,
)


@pytest.fixture(scope='session')
def hello_client_class():
    return gen_client_class(greet_client_spec)

@pytest.fixture(scope='session')
def hello_client_gen(hello_client_class):
    return partial(hello_client_class, host_name=HOST_NAME, port=PORT)

@pytest.fixture
def hello_client(hello_client_class):
    return hello_client_class(host_name=HOST_NAME, port=PORT)

@pytest.fixture(scope='module')
def hello_client_module_scope(hello_client_class):
    return hello_client_class(host_name=HOST_NAME, port=PORT)

@pytest.fixture(scope='session')
def hello_server_cm():
    return partial(serve_cm,
                   host_name=HOST_NAME,
                   port=PORT,
                   server_spec = greet_server_spec)

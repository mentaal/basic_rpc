from enum import Enum
import argparse
from basic_socket_rpc.rpc_spec import RpcClientSpec, RpcClientReq
from basic_socket_rpc.rpc_serialization_functions import (
    serialize_str,
    deserialize_str_only,
    make_serializer,
    int_to_le_bytes_4,
    int_from_le_bytes_4,
    parse_no_response,
)
from basic_socket_rpc.rpc_blocking_client import gen_client_class


class greet_client_cmd_ids(Enum):
    hello = 0
    goodbye = 1
    add_2_words = 2
    func_no_args = 3


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
            cmd_id=greet_client_cmd_ids.func_no_args,
            serialize_request=lambda: b"",
            parse_response=parse_no_response,
        ),
    ],
)

RpcClient = gen_client_class(greet_client_spec)

if __name__ == "__main__":
    # import logging
    # logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(description="Simple greeting RPC server")
    parser.add_argument(
        "--address",
        default="127.0.0.1",
        help="Address to connect to. Defaults to local host",
    )
    default_port = 11599
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=default_port,
        help=f"Port to onnect to {default_port}",
    )

    p = parser.parse_args()

    rpc_client = RpcClient(
        host_name=p.address,
        port=p.port,
    )

    with rpc_client:
        resp = rpc_client.hello("Robert")
        print(f"got {resp} from server")
        resp = rpc_client.goodbye("Robert")
        print(f"got {resp} from server")
        assert 3050 == rpc_client.add_2_words(1000, 2050)
        rpc_client.func_no_args()

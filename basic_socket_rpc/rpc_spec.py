"""Module to hold RPC spec for rpc implementation."""

from enum import Enum
from typing import Any, Callable, Dict, NamedTuple, Tuple

from .rpc_serialization_functions import Buffer


OnServerInitResp = Tuple[Dict, Callable[[], Dict[str, Any]]]
OnServerInit = Callable[[], OnServerInitResp]


class RpcClientReq(NamedTuple):
    cmd_id: Enum
    serialize_request: Callable[..., bytes]
    parse_response: Callable[[Buffer], Any]
    response_timeout: int = 20


class RpcServerResp(NamedTuple):
    cmd_id: Enum
    parse_and_call: Callable[[Buffer], Any]
    serialize_response: Callable[..., bytes]
    client_function: Callable


class RpcClientSpec(NamedTuple):
    requests: Tuple[RpcClientReq]
    on_connect: Callable[[Any], None] = lambda local_data: None
    on_disconnect: Callable[[Any], bool] = lambda local_data: None


class RpcServerSpec(NamedTuple):
    responses: Tuple[RpcServerResp, ...]
    # Called on server initialization. This callback is expected to produce
    # something which is consumed by the on_client_connect and
    # on_client_disconnect callbacks. Useful for defining shared structures such
    # as locks. Return a dictionary shared by all connected users and also
    # a factory function which returns a dictionary which is local to each user
    on_server_init: OnServerInit = lambda: ({}, lambda: {})
    on_server_close: Callable[[dict], None] = lambda _: None
    # this should be a function accepting a connection specific dictionary which
    # returns a boolean where:
    # True indicates that the connection will be allowed to continue
    # False indicates that the connection should be closed
    # This is useful to implement something like resource/access constraints
    on_client_connect: Callable[[dict, dict], bool] = lambda d1, d2: True  # shared_data, local_data
    on_client_disconnect: Callable[[dict, dict], None] = lambda d1, d2: None
    max_concurrent_connections: int = 5

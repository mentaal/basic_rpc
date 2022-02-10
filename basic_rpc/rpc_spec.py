"""Module to hold RPC spec for rpc implementation. 
"""

from typing import NamedTuple, Callable, Any, List, Tuple
from enum import Enum

class RpcClientReq(NamedTuple):
    cmd_id:Enum
    serialize_request:Callable[[...], bytes]
    parse_response:Callable[[bytes], Any]

class RpcServerResp(NamedTuple):
    cmd_id:Enum
    parse_and_call:Callable[[bytes], Any]
    serialize_response:Callable[[...], bytes]
    client_function:Callable

class RpcClientSpec(NamedTuple):
    requests: List[RpcClientReq]
    on_connect:Callable[[Any], None] = lambda local_data: None
    on_disconnect:Callable[[Any], bool] = lambda local_data: None
    timeout_secs:float = 10 #this is a hint, may not be applied depending on server implementation

class RpcServerSpec(NamedTuple):
    responses: List[RpcServerResp]
    # Called on server initialization. This callback is expected to produce
    # something which is consumed by the on_client_connect and
    # on_client_disconnect callbacks. Useful for defining shared structures such
    # as locks. Return a dictionary shared by all connected users and also
    # a factory function which returns a dictionary which is local to each user
    on_server_init:Callable[[], Tuple[dict, Callable]] = lambda: ({}, lambda: {})
    # this should be a function accepting a connection specific dictionary which
    # returns a boolean where:
    # True indicates that the connection will be allowed to continue
    # False indicates that the connection should be closed
    # This is useful to implement something like resource/access constraints
    on_client_connect:Callable[[dict, dict], bool] = lambda d1, d2: True # shared_data, local_data
    on_client_disconnect:Callable[[dict, dict], None] = lambda d1, d2: None
    max_concurrent_connections:int = 5


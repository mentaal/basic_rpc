"""Basic socket server to be used for RPC

The server is synchronous and intended to be used with threads

This server imposes the following protocol on the wire:

    1. Messages prefixed by their size are transmitted

Client request protocol:
    Common structure:
        <uint32 msg_size> <uint8 msg_type> <bytes msg>

    The following describes the subsequent bytes following the msg_type

    MSG_CLIENT_RPC_REQ: <uint16 command ID> <serialized arguments>
    MSG_CLIENT_INIT: init bytes - this should be parsed with use case specific (on_connect function)

Server response protocol:
    Common structure:
        <uint32 msg_size> <uint8 msg_type> <bytes msg>

    MSG_SERVER_RPC_RESP: <uint16 command ID> <serialized response>
    MSG_SERVER_INIT: init bytes - this should be parsed with use case specific (on_connect function)
    MSG_SERVER_EXCEPTION: <uint8 exception name len>
                          <utf8 buf exception name>
                          <uint8 exception string len>
                          <utf8 buf exception string>
])

"""

import builtins
from enum import Enum
from functools import partial
from typing import Callable, Iterable, Tuple, Union, List, NamedTuple, Any, Optional
from .rpc_serialization_functions import (
    int_to_le_bytes_4,
    int_to_le_bytes_2,
    int_to_le_bytes_1,
    int_from_le_bytes_4,
    int_from_le_bytes_2,
    serialize_str,
    deserialize_str,
    serialize_bool,
)

is_exception = lambda obj: isinstance(obj, type) and issubclass(obj, BaseException)
built_in_exceptions = {k: v for k, v in builtins.__dict__.items() if is_exception(v)}


def enum_from_value(enum_class: "enum.EnumMeta", value: Any) -> Optional[Enum]:
    try:
        return enum_class(value)
    except ValueError:
        return None


serialize_msg_size = int_to_le_bytes_4
deserialize_msg_size = int_from_le_bytes_4
serialize_msg_type = lambda en: int_to_le_bytes_1(en.value)
serialize_cmd_enum = lambda en: int_to_le_bytes_2(en.value)
deserialize_cmd_id = int_from_le_bytes_2


def deserialize_version(bs: bytes) -> Tuple[int]:
    if len(bs) >= 3:
        return tuple(bs[:3])
    raise ValueError(
        f"Cannot deserialize version, insufficient bytes: {len(bs)}. Expected 3"
    )


MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION = 0, 0, 1

VERSION = MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION
VERSION_BYTES = bytes(VERSION)

REQ_HDR_PREFIX_SIZE = 5

EnumTup = Tuple[str, int]

EnumSeq = Union[List[EnumTup], Tuple[EnumTup]]

_enum_tup_to_bytes = lambda tup: (tup[0], int_to_le_bytes_1(tup[1]))


class ProtocolError(ValueError):
    pass


class DisconnectedError(ValueError):
    pass


class ServerError(ValueError):
    pass


class ServerRpcError(ValueError):
    pass


class ServerShutdown(Exception):
    def __init__(self, *args, during_send: bool = False, **kwargs):
        self._during_send = during_send
        super().__init__(*args, **kwargs)


def unexpected_msg_error(expected: Enum, got: Enum):
    raise ProtocolError(f"Unexpected msg_type: {got.name}, should be: {expected.name}")


def gen_enum_and_bytes(enum_name: str, enumerations: EnumSeq) -> Tuple[Enum]:
    enum_cls = Enum(enum_name, enumerations)
    enum_cls_bytes = Enum(
        f"{enum_name}Bytes", list(map(_enum_tup_to_bytes, enumerations))
    )
    return enum_cls, enum_cls_bytes


ClientMsgType, ClientMsgTypeBytes = gen_enum_and_bytes(
    "ClientMsgType",
    [
        ("MSG_CLIENT_INIT", 0),
        ("MSG_CLIENT_RPC_REQ", 1),
        # ('MSG_CLIENT_FINI',    2),
        # ('MSG_CLIENT_RESTART', 3),
    ],
)

ServerMsgType, ServerMsgTypeBytes = gen_enum_and_bytes(
    "ServerMsgType",
    [
        ("MSG_SERVER_INIT", 0),
        ("MSG_SERVER_RPC_RESP", 1),
        # ('MSG_SERVER_FINI',      2),
        # ('MSG_SERVER_RESTART',   3),
        ("MSG_SERVER_EXCEPTION", 4),
    ],
)


def parse_msg_header(enum_type: "enum_meta", msg: bytes):
    msg_type = enum_from_value(enum_type, msg[:1])
    if not msg_type:
        raise ProtocolError(f"Unexpected msg_type: {msg_type}")
    return msg_type


parse_msg_header_from_server = partial(parse_msg_header, ServerMsgTypeBytes)
parse_msg_header_from_client = partial(parse_msg_header, ClientMsgTypeBytes)


def serialize_helper(msg_type: Enum, payload: bytes):
    return b"".join(
        [
            serialize_msg_size(len(payload) + REQ_HDR_PREFIX_SIZE),
            msg_type.value,
            payload,
        ]
    )


serialize_client_init = partial(serialize_helper, ClientMsgTypeBytes.MSG_CLIENT_INIT)
serialize_server_rpc_response = partial(
    serialize_helper, ServerMsgTypeBytes.MSG_SERVER_RPC_RESP
)


def serialize_client_rpc_req(cmd_id: Enum, bs: bytes) -> Tuple[bytes]:
    return serialize_helper(
        msg_type=ClientMsgTypeBytes.MSG_CLIENT_RPC_REQ,
        payload=b"".join([serialize_cmd_enum(cmd_id), bs]),
    )


def serialize_server_init_resp(success: bool) -> Tuple[bytes]:
    return serialize_helper(
        msg_type=ServerMsgTypeBytes.MSG_SERVER_INIT,
        payload=serialize_bool(success),
    )


def serialize_exception(exc: Exception):
    name = type(exc).__name__
    reason = str(exc)
    payload_bs = b"".join(map(serialize_str, [name, reason]))
    return serialize_helper(
        msg_type=ServerMsgTypeBytes.MSG_SERVER_EXCEPTION,
        payload=payload_bs,
    )


def deserialize_exception_raise(bs: bytes):
    exception_type, remaining = deserialize_str(bs)
    exception_str, remaining = deserialize_str(remaining)
    if exception_type == "ValueError":
        raise ValueError(exception_str)
    elif exception_type == "ProtocolError":
        raise ProtocolError(exception_str)
    elif exception_type == "ServerShutdown":
        raise ServerShutdown(exception_str)
    else:
        exc_type = built_in_exceptions.get(exception_type)
        if exc_type:
            raise exc_type(f"Server exception: {exception_str}")
        else:
            raise ServerError(f"Server exception {exception_type}: {exception_str}")

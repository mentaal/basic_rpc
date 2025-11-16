"""Basic socket client to be used for RPC"""

import threading
import socket
from enum import Enum
from typing import Optional, Callable
from logging import debug, error as log_error, info
from functools import wraps
from time import sleep, time as now

from .rpc_spec import RpcClientSpec, RpcClientReq
from .rpc_serialization_functions import deserialize_bool_only
from .rpc_low_level import (
    ServerMsgTypeBytes,
    deserialize_exception_raise,
    REQ_HDR_PREFIX_SIZE,
    VERSION_BYTES,
    serialize_client_rpc_req,
    serialize_client_init,
    ProtocolError,
    DisconnectedError,
    unexpected_msg_error,
    parse_msg_header_from_server,
    deserialize_msg_size,
)

CONNECT_TIMEOUT = 7


def has_timed_out(deadline: float) -> bool:
    current = now()
    remaining_time = deadline - current if deadline >= current else 0
    debug(f"remaining time: {remaining_time:.02f}")
    return now() >= deadline


class SocketClientBase:
    def __init__(
        self,
        host_name: str,
        port: int,
        timeout_secs: float = 20,
        retry_interval_secs: float = 4.5,
    ):
        if timeout_secs < CONNECT_TIMEOUT:
            raise ValueError(
                "Connect on sockets don't like timeouts too short. Provide one larger than 5 secs"
            )
        self.sock = None
        self.host = host_name, port
        self.local_data = {}

        self.connected = False
        self.timeout_secs = timeout_secs
        self.retry_interval_secs = retry_interval_secs

    def connect(self):
        if self.connected:
            raise ValueError("Already connected")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        sock.settimeout(CONNECT_TIMEOUT)
        info(f"connecting to host: {self.host}")
        start = now()
        deadline = start + self.timeout_secs
        while True:
            try:
                sock.connect(self.host)
                break
            except socket.timeout:
                if has_timed_out(deadline):
                    raise
        self.sock = sock
        try:
            self.connect_init_wait(deadline, self.retry_interval_secs)
        except Exception as exc:
            self.close()
            raise
        self.connected = True
        return self

    def close(self):
        if self.sock:
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
            self.sock = None
        self.connected = False
        self.client_spec.on_disconnect(self.local_data)

    def send_msg(self, cmd_id: Enum, bs: bytes, deadline: float):
        msg = serialize_client_rpc_req(cmd_id, bs)
        self.send_all(msg, deadline)

    def get_msg(self, deadline: float):
        msg_size_bytes = self.recv_all(4, deadline)
        msg_size = deserialize_msg_size(msg_size_bytes)
        response_size = msg_size - REQ_HDR_PREFIX_SIZE
        debug(f"got response msg of size: {msg_size}")
        if msg_size < REQ_HDR_PREFIX_SIZE:
            raise ProtocolError(f"message of size {msg_size} less than minimum")

        debug(f"Going to recv message of size: {msg_size - 4}")
        msg_bytes = self.recv_all(msg_size - 4, deadline)
        msg_type = parse_msg_header_from_server(msg_bytes)
        response_payload = msg_bytes[REQ_HDR_PREFIX_SIZE - 4 :]
        if msg_type == ServerMsgTypeBytes.MSG_SERVER_EXCEPTION:
            deserialize_exception_raise(response_payload)
        return msg_type, response_payload

    def connect_init_wait(self, deadline: float, retry_interval_secs):
        while True:
            connection_established = self.connect_init(deadline)
            if connection_established:
                break
            # subtraction here to save a pointless sleep if we know that we will time out
            # also make the deadline more stringent (for whatever it's worth)
            elif has_timed_out(deadline - retry_interval_secs):
                raise ConnectionError(
                    "Timed out waiting for connection with server to be established (at the protocol layer, not socket layer)"
                )
            sleep(retry_interval_secs)
        self.client_spec.on_connect(self.local_data)

    def connect_init(self, deadline: float) -> bool:
        msg = serialize_client_init(VERSION_BYTES)
        debug("Sending init msg..")
        self.send_all(msg, deadline)
        debug("Getting init response...")
        msg_type, response_payload = self.get_msg(deadline)
        debug("Got init response...")
        if msg_type != ServerMsgTypeBytes.MSG_SERVER_INIT:
            raise ProtocolError(f"Expected server INIT response. Got: {msg_type}")
        return deserialize_bool_only(response_payload)

    def recv_all(self, num: int, deadline: float = 10) -> bytes:
        sock = self.sock
        if not sock:
            raise ValueError("Trying to receive without a socket")
        chunks = []
        remaining = num
        start = now()
        while remaining:
            try:
                recvd = sock.recv(min(remaining, 8192))
            except socket.timeout as ex:
                if has_timed_out(deadline):
                    raise ConnectionError(
                        "Timed out waiting for complete response from the server"
                    ) from ex
                else:
                    continue
            recvd_len = len(recvd)
            if recvd_len:
                chunks.append(recvd)
                remaining -= recvd_len
                debug(f"received: {recvd_len}, remaining: {remaining}")
                if remaining == 0:  # done
                    return b"".join(chunks)
            else:
                raise DisconnectedError()

    def send_all(self, bs: bytes, deadline: float):
        sock = self.sock
        if not sock:
            raise ValueError("Trying to send without a socket")
        to_send_len = len(bs)
        total_sent = 0
        debug(f"sending a msg of {to_send_len} bytes...")
        while total_sent < to_send_len:
            try:
                num_sent = sock.send(bs[total_sent : total_sent + 8192])
            except socket.timeout as ex:
                if has_timed_out(deadline):
                    raise ConnectionError(
                        "Timed out waiting to send data to server"
                    ) from ex
                else:
                    continue
            total_sent += num_sent

    __enter__ = connect

    def __exit__(self, exc_type: Optional, exc_value: Optional, traceback: Optional):
        self.close()


def _create_req_func(client_req: RpcClientReq) -> Callable:
    @wraps(client_req.serialize_request)
    def req_func(self, *args, _timeout_secs=10, **kwargs):
        bs = client_req.serialize_request(*args, **kwargs)
        deadline = now() + _timeout_secs
        self.send_msg(cmd_id=client_req.cmd_id, bs=bs, deadline=deadline)
        msg_type, response_payload = self.get_msg(deadline)
        if msg_type != ServerMsgTypeBytes.MSG_SERVER_RPC_RESP:
            unexpected_msg_error(ServerMsgTypeBytes.MSG_SERVER_RPC_RESP, msg_type)

        return client_req.parse_response(response_payload)

    return req_func


def gen_client_class(client_spec: RpcClientSpec):
    class_dict = {
        req.cmd_id.name: _create_req_func(req) for req in client_spec.requests
    }
    class_dict["client_spec"] = client_spec
    return type(
        "RpcClient",
        (SocketClientBase,),
        class_dict,
    )

"""Basic socket client to be used for RPC"""

import logging
import socket
from abc import ABC, abstractmethod
from enum import Enum
from functools import wraps
from time import sleep
from time import time as now
from typing import Callable, Dict, Optional

from .rpc_low_level import (
    deserialize_exception_raise,
    deserialize_msg_size,
    DisconnectedError,
    parse_msg_header_from_server,
    ProtocolError,
    REQ_HDR_PREFIX_SIZE,
    serialize_client_init,
    serialize_client_rpc_req,
    ServerMsgTypeBytes,
    unexpected_msg_error,
    VERSION_BYTES,
)
from .rpc_serialization_functions import deserialize_bool_only
from .rpc_spec import RpcClientReq, RpcClientSpec


logger = logging.getLogger(__name__)
CONNECT_TIMEOUT = 15


def has_timed_out(deadline: float) -> bool:
    current = now()
    remaining_time = deadline - current if deadline >= current else 0
    logger.debug(f"remaining time: {remaining_time:.02f}")
    return now() >= deadline


class SocketClient:
    def __init__(
        self,
        host_name: str,
        port: int,
        on_connect: Callable[[Dict], None],
        on_disconnect: Callable[[Dict], None],
        timeout_secs: float = 60,
        retry_interval_secs: float = 10,
    ):
        if timeout_secs < CONNECT_TIMEOUT:
            err_msg = f"Connect on sockets don't like timeouts too short. Provide one larger than {CONNECT_TIMEOUT} secs"
            logger.error(err_msg)
            raise ValueError(err_msg)
        self.sock = None
        self.host = host_name, port

        self.connected = False
        self.timeout_secs = timeout_secs
        self.retry_interval_secs = retry_interval_secs
        self.local_data = {}
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect

    def connect(self):
        if self.connected:
            err_msg = "Already connected"
            logger.error(err_msg)
            raise ValueError(err_msg)
        start = now()
        deadline = start + self.timeout_secs
        while True:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
            sock.settimeout(CONNECT_TIMEOUT)
            logger.info(f"connecting to host: {self.host}")
            try:
                sock.connect(self.host)
                break
            except socket.timeout:
                if has_timed_out(deadline):
                    raise
        self.sock = sock
        try:
            self.connect_init_wait(deadline, self.retry_interval_secs)
        except Exception:
            self.disconnect()
            raise
        self.connected = True
        self.on_connect(self.local_data)
        return self

    def disconnect(self):
        if self.sock:
            logger.debug("Shutting down socket")
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
            self.sock = None
        self.connected = False
        self.on_disconnect(self.local_data)

    def send_msg(self, cmd_id: Enum, bs: bytes, deadline: float):
        msg = serialize_client_rpc_req(cmd_id, bs)
        self.send_all(msg, deadline)

    def get_msg(self, deadline: float):
        msg_size_bytes = self.recv_all(4, deadline)
        msg_size = deserialize_msg_size(msg_size_bytes)
        logger.debug(f"got response msg of size: {msg_size}")
        if msg_size < REQ_HDR_PREFIX_SIZE:
            raise ProtocolError(f"message of size {msg_size} less than minimum")

        logger.debug(f"Going to recv message of size: {msg_size - 4}")
        msg_bytes = self.recv_all(msg_size - 4, deadline)
        msg_mv = memoryview(msg_bytes)
        msg_type = parse_msg_header_from_server(msg_mv)
        response_payload = msg_mv[REQ_HDR_PREFIX_SIZE - 4 :]
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

    def connect_init(self, deadline: float) -> bool:
        msg = serialize_client_init(VERSION_BYTES)
        logger.debug("Sending init msg..")
        self.send_all(msg, deadline)
        logger.debug("Getting init response...")
        msg_type, response_payload = self.get_msg(deadline)
        logger.debug("Got init response...")
        if msg_type != ServerMsgTypeBytes.MSG_SERVER_INIT:
            err_msg = f"Expected server INIT response. Got: {msg_type}"
            logger.error(err_msg)
            raise ProtocolError(err_msg)
        return deserialize_bool_only(response_payload)

    def recv_all(self, num: int, deadline: float = 10) -> bytes:
        sock = self.sock
        if not sock:
            err_msg = "Trying to receive without a socket"
            logger.error(err_msg)
            raise ValueError(err_msg)
        chunks = []
        remaining = num
        while remaining:
            try:
                recvd = sock.recv(min(remaining, 8192))
            except socket.timeout as ex:
                if has_timed_out(deadline):
                    raise ConnectionError("Timed out waiting for complete response from the server") from ex
                else:
                    continue
            recvd_len = len(recvd)
            if recvd_len:
                chunks.append(recvd)
                remaining -= recvd_len
                logger.debug(f"received: {recvd_len}, remaining: {remaining}")
                if remaining == 0:  # done
                    return b"".join(chunks)
            else:
                raise DisconnectedError()

    def send_all(self, bs: bytes, deadline: float):
        sock = self.sock
        if not sock:
            err_msg = "Trying to send without a socket"
            logger.error(err_msg)
            raise ValueError(err_msg)
        to_send_len = len(bs)
        total_sent = 0
        logger.debug(f"sending a msg of {to_send_len} bytes...")
        while total_sent < to_send_len:
            try:
                num_sent = sock.send(bs[total_sent : total_sent + 8192])
            except socket.timeout as ex:
                if has_timed_out(deadline):
                    err_msg = "Timed out waiting to send data to server"
                    logger.error(err_msg)
                    raise ConnectionError(err_msg) from ex
                else:
                    continue
            total_sent += num_sent


class RpcClientBase:

    def __init__(
        self,
        host_name: str,
        port: int,
        timeout_secs: float = 20,
        retry_interval_secs: float = 4.5,
    ):
        self._socket_client = SocketClient(
            host_name=host_name,
            port=port,
            timeout_secs=timeout_secs,
            retry_interval_secs=retry_interval_secs,
            on_connect=self._on_connect,
            on_disconnect=self._on_disconnect,
        )

    def socket_client_connect(self):
        self._socket_client.connect()

    def socket_client_disconnect(self):
        self._socket_client.disconnect()

    @staticmethod
    @abstractmethod
    def _on_disconnect(local_data: Dict):
        pass

    @staticmethod
    @abstractmethod
    def _on_connect(local_data: Dict):
        pass

    __enter__ = socket_client_connect

    def __exit__(self, exc_type: Optional, exc_value: Optional, traceback: Optional):
        self.socket_client_disconnect()


def _create_req_func(client_req: RpcClientReq) -> Callable:
    @wraps(client_req.serialize_request)
    def req_func(self, *args, _timeout_secs=client_req.response_timeout, **kwargs):
        bs = client_req.serialize_request(*args, **kwargs)
        deadline = now() + _timeout_secs
        self._socket_client.send_msg(cmd_id=client_req.cmd_id, bs=bs, deadline=deadline)
        msg_type, response_payload = self._socket_client.get_msg(deadline)
        if msg_type != ServerMsgTypeBytes.MSG_SERVER_RPC_RESP:
            unexpected_msg_error(ServerMsgTypeBytes.MSG_SERVER_RPC_RESP, msg_type)

        return client_req.parse_response(response_payload)

    return req_func


def gen_client_class(client_spec: RpcClientSpec):
    class_dict = {req.cmd_id.name: _create_req_func(req) for req in client_spec.requests}
    class_dict["_on_connect"] = staticmethod(client_spec.on_connect)
    class_dict["_on_disconnect"] = staticmethod(client_spec.on_disconnect)
    return type(
        "RpcClient",
        (RpcClientBase,),
        class_dict,
    )

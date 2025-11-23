"""Basic socket server to be used for RPC

The server is synchronous and intended to be used with threads

See rpc_low_level.py for more discussion on the protocol
"""

import socket
import threading
from contextlib import contextmanager
from enum import Enum
from functools import partial
from logging import debug
from logging import error as log_error
from time import sleep
from typing import Any, Callable, Dict, Optional, Tuple

from .rpc_low_level import (
    ClientMsgTypeBytes,
    deserialize_cmd_id,
    deserialize_msg_size,
    deserialize_version,
    DisconnectedError,
    MAJOR_VERSION,
    parse_msg_header_from_client,
    ProtocolError,
    REQ_HDR_PREFIX_SIZE,
    serialize_exception,
    serialize_server_init_resp,
    serialize_server_rpc_response,
    ServerRpcError,
    ServerShutdown,
    unexpected_msg_error,
)
from .rpc_serialization_functions import (
    Buffer,
)
from .rpc_spec import OnServerInit, OnServerInitResp, RpcServerResp, RpcServerSpec


def log_print(msg: str):
    debug(msg)
    print(msg)


class SocketServer:
    def __init__(
        self,
        sock: socket.socket,
        client: Any,
        server_spec: RpcServerSpec,
        spec_dict: dict,
        shared_data_lock: Tuple[Dict, Dict, threading.Lock],
        shutdown_event: threading.Event,
    ):
        self.sock = sock
        self.client = client
        self.server_spec = server_spec
        self.spec_dict = spec_dict
        self.session_established = False
        self.shared_data_lock = shared_data_lock
        self.shutdown_event = shutdown_event

    def close(self):
        debug(f"closing connection from client {self.client}...")
        self.session_established = False
        shared_data, local_data, lock = self.shared_data_lock
        with lock:
            self.server_spec.on_client_disconnect(shared_data, local_data)
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()
        debug("closing connection from client complete")

    def send_all(self, bs: Buffer):
        sock, shutdown_event = self.sock, self.shutdown_event
        to_send_len = len(bs)
        total_sent = 0
        debug(f"sending a msg of {to_send_len} bytes...")
        while total_sent < to_send_len:
            try:
                num_sent = sock.send(bs[total_sent : total_sent + 8192])
                if num_sent == 0:  # client has disconnected
                    debug(f"client {self.client} disconnected...")
                    raise DisconnectedError()
                total_sent += num_sent
            except socket.timeout:
                if shutdown_event.is_set():
                    raise ServerShutdown("server is shutting down", during_send=True)
        debug("finished sending msg")

    def recv_all(self, num: int) -> bytes:
        assert num, f"Expected non-zero num argument. Got: {num}"
        sock, shutdown_event = self.sock, self.shutdown_event
        chunks = []
        remaining = num
        while remaining:
            try:
                recvd = sock.recv(remaining)
            except socket.timeout:
                if shutdown_event.is_set():
                    raise ServerShutdown("server is shutting down")
                continue
            recvd_len = len(recvd)
            debug(f"got {recvd_len} bytes of data...")
            if recvd_len:
                chunks.append(recvd)
                remaining -= len(recvd)
                if remaining == 0:  # done
                    return b"".join(chunks)
            else:
                debug(f"client {self.client} disconnected...")
                raise DisconnectedError()
        raise RuntimeError("Expected unreachable in recv_all")

    def get_msg(self) -> Tuple[Enum, memoryview]:
        msg_size_bytes = self.recv_all(4)
        msg_size = deserialize_msg_size(msg_size_bytes)
        debug(f"received a message of payload size: {msg_size}")
        if msg_size < REQ_HDR_PREFIX_SIZE:
            raise ProtocolError(f"Reported message size too small: {msg_size}")
        msg_bytes = self.recv_all(msg_size - 4)
        # debug(f'msg_bytes: {msg_bytes}')

        msg_mv = memoryview(msg_bytes)
        msg_type = parse_msg_header_from_client(msg_mv)
        payload = msg_mv[REQ_HDR_PREFIX_SIZE - 4 :]
        debug(f"received a message of type: {msg_type}")
        return msg_type, payload

    def handle_init(self, msg_type: ClientMsgTypeBytes, msg: memoryview):
        if not self.session_established:
            major, minor, patch = deserialize_version(msg)
            if MAJOR_VERSION != major:
                err_str = f"Incorrect major version: {major}. Expected {MAJOR_VERSION}"
                raise ProtocolError(err_str)
            else:
                shared_data, local_data, lock = self.shared_data_lock
                with lock:
                    session_established = self.server_spec.on_client_connect(shared_data, local_data)
                self.session_established = session_established
                debug(f"session established: {session_established}")
                bs = serialize_server_init_resp(session_established)
                debug("sending init response...")
                self.send_all(bs)
                debug("sending init response done...")
                return
        else:
            raise ProtocolError("Initialization already occurred")

    def handle_cmd(self, msg: memoryview):
        if len(msg) < 2:
            raise ProtocolError("Insufficient bytes in command.")
        cmd_id = deserialize_cmd_id(msg)

        spec = self.spec_dict.get(cmd_id)
        if not spec:
            raise ProtocolError(f"Unknown command with id: {cmd_id}")
        debug(f"got RPC request: {spec.cmd_id.name}")

        try:
            res = spec.parse_and_call(spec.client_function, msg[2:])
            res_bytes = spec.serialize_response(res)
        except Exception as exc:
            raise ServerRpcError("RPC error") from exc
        bs = serialize_server_rpc_response(res_bytes)
        self.send_all(bs)
        debug(f"finished serving RPC request: {spec.cmd_id.name}")

    def handle_exception(self, exc: Exception, rpc_exception: bool = False):
        response = serialize_exception(exc)
        log_error(f"Exception: {exc}", exc_info=exc)
        self.sock.sendall(response)

    def run(self):

        debug("waiting for a message from client {self.client} ...")
        try:
            while True:
                try:
                    msg_type, payload = self.get_msg()
                    if msg_type == ClientMsgTypeBytes.MSG_CLIENT_INIT:
                        self.handle_init(msg_type, payload)
                    elif msg_type == ClientMsgTypeBytes.MSG_CLIENT_RPC_REQ:
                        if not self.session_established:
                            raise ProtocolError("Need to initialize connection first")
                        self.handle_cmd(payload)
                    else:
                        unexpected_msg_error(ClientMsgTypeBytes.MSG_CLIENT_RPC_REQ, msg_type)
                except DisconnectedError:
                    break
                except ServerShutdown as exc:
                    debug("server shutdown...")
                    if not exc._during_send:
                        self.handle_exception(exc)
                    return
                except ProtocolError as exc:
                    self.handle_exception(exc)
                except ServerRpcError as exc:
                    self.handle_exception(exc.__cause__, rpc_exception=True)
                except Exception as exc:
                    self.handle_exception(exc)
                    break
        finally:
            self.close()


def safe_join(thread: threading.Thread):
    """signals don't gel with threads so don't call blocking calls directly"""
    while True:
        if not thread.is_alive():
            return
        sleep(0.2)


def accepter(
    host: Tuple[str, int],
    server_spec: RpcServerSpec,
    shutdown_event: threading.Event,
):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
    sock.bind(host)
    sock.listen(server_spec.max_concurrent_connections)
    sock.settimeout(2.0)
    spec_dict = {spec.cmd_id.value: spec for spec in server_spec.responses}
    (shared_data, local_data_gen), lock = server_spec.on_server_init(), threading.Lock()

    child_threads = []

    host_name, port = host
    displayed_host_name = "all interfaces" if host_name == "" else host_name
    log_print(f"listening for connections on {displayed_host_name} on port: {port}")

    while True:
        if shutdown_event.is_set():
            break
        try:
            client_sock, address = sock.accept()
            client_sock.settimeout(2.0)
        except socket.timeout:
            continue
        debug(f"Got connection from address: {address}")
        socket_server = SocketServer(
            sock=client_sock,
            client=address,
            shutdown_event=shutdown_event,
            server_spec=server_spec,
            spec_dict=spec_dict,
            shared_data_lock=(shared_data, local_data_gen(), lock),
        )
        thread = threading.Thread(target=serve_client_thread, args=(socket_server,))
        child_threads[:] = (t for t in child_threads if t.is_alive())
        child_threads.append(thread)
        thread.start()
    debug("waiting for client threads to shutdown")
    for thread in child_threads:
        safe_join(thread)
    debug("client threads shutdown")
    sock.close()
    server_spec.on_server_close(shared_data)
    debug("accepter thread done")


def serve_client_thread(socket_server: SocketServer):
    socket_server.run()


def serve(
    host_name: str,
    port: int,
    server_spec: RpcServerSpec,
    _shutdown_event: Optional[threading.Event] = None,
):
    shutdown_event = _shutdown_event or threading.Event()
    accepter_thread = threading.Thread(
        target=accepter,
        kwargs={
            "host": (host_name, port),
            "server_spec": server_spec,
            "shutdown_event": shutdown_event,
        },
    )

    try:
        accepter_thread.start()

        safe_join(accepter_thread)
        debug("accepter thread finished")
    except (Exception, KeyboardInterrupt) as exc:
        log_error(f"Exception: {exc}", exc_info=exc)
        shutdown_event.set()
        safe_join(accepter_thread)
        debug("accepter thread finished")
        raise


def make_serve(server_spec: RpcServerSpec) -> Callable:
    return partial(serve, server_spec=server_spec)


def make_serve_cm(server: Callable) -> Callable:
    """Make a context manager in which to run the supplied server
    :param: Already made server (using something like `make_serve`)
    :returns: The context manager
    """
    shutdown_event = threading.Event()

    @contextmanager
    def serve_cm(
        host_name: str,
        port: int,
    ) -> Callable:
        kwargs = {
            "_shutdown_event": shutdown_event,
            "host_name": host_name,
            "port": port,
        }

        server_thread = threading.Thread(target=server, kwargs=kwargs)
        server_thread.start()
        debug("Started server thread from context manager...")
        try:
            yield
        finally:
            debug("Shutting down server")
            shutdown_event.set()
            safe_join(server_thread)
            debug("Shutdown complete")

    return serve_cm


#####################################################
# Logic to implement an exclusive access RPC service


def single_client_only_connect(shared_data: dict, local_data: dict) -> bool:
    """Only allow a single client to use the RPC service at a time
    :param shared_data: data shared across all connected users
    :param local_data: data local to the connected user
    :returns: A boolean to indicate if a RPC session can continue with this user
    """
    locally_connected_already = local_data["user_connected"]
    if locally_connected_already:
        raise ValueError("User attempting to connect but already connected. This shouldn't happen")

    waiting_threads = shared_data["waiting_threads"]
    my_tid = threading.get_ident()
    # TODO consider adding more logic here to implement a fair queue for users waiting on service
    user_in_wait_queue = waiting_threads and my_tid in waiting_threads
    user_next_in_line = bool(waiting_threads) and my_tid == waiting_threads[0]
    noone_connected = not shared_data["user_connected"]
    empty_queue = not waiting_threads
    user_can_connect = noone_connected and (user_next_in_line or empty_queue)
    assert type(user_can_connect) == bool

    if user_can_connect:
        shared_data["user_connected"] = True
        local_data["user_connected"] = True
        debug(f"thread:{my_tid} connected")
        if user_next_in_line:
            debug(f"thread: {my_tid} can now become active so popping from waiting list")
            waiting_threads.remove(my_tid)
    else:
        if not user_in_wait_queue:
            waiting_threads.append(my_tid)
            debug(f"thread: {my_tid} added to wait queue. Position: {waiting_threads.index(my_tid)}...")
        else:
            debug(f"thread: {my_tid} waiting in line... position: {waiting_threads.index(my_tid)}")
    return user_can_connect


def single_client_only_disconnect(shared_data: dict, local_data: dict) -> bool:
    connected_local = local_data["user_connected"]
    connected_shared = shared_data["user_connected"]
    owns_session = connected_local and connected_shared
    bad_state = connected_local and not connected_shared
    my_tid = threading.get_ident()
    waiting_threads = shared_data["waiting_threads"]
    if bad_state:
        raise ValueError("Bad state: User reports as being connected but shared session data doesn't agree")

    if owns_session:
        shared_data["user_connected"] = False
    my_tid = threading.get_ident()
    if my_tid in waiting_threads:
        debug(f"thread: {my_tid} never got to connect. popping from waiting list")
        waiting_threads.remove(my_tid)

    debug(f"{my_tid} disconnected. Wait queue length: {len(waiting_threads)}")

    local_data["user_connected"] = False


def gen_exclusive_access_shared_data() -> Dict[str, Any]:
    return {
        "user_connected": False,
        "waiting_threads": [],
    }


def gen_exclusive_access_local_data() -> Dict[str, Any]:
    return {
        "user_connected": False,
    }


def on_exclusive_access_server_init() -> OnServerInitResp:
    return (
        gen_exclusive_access_shared_data(),
        gen_exclusive_access_local_data,
    )


def make_exclusive_access_server(
    responses: Tuple[RpcServerResp, ...],
    on_server_init: OnServerInit = on_exclusive_access_server_init,
    on_client_connect: Callable = single_client_only_connect,
    on_client_disconnect: Callable = single_client_only_disconnect,
) -> Callable:
    server_spec = RpcServerSpec(
        responses=responses,
        on_server_init=on_server_init,
        on_client_connect=on_client_connect,
        on_client_disconnect=on_client_disconnect,
    )
    return make_serve(server_spec)


def make_exclusive_access_server_cm(
    responses: Tuple[RpcServerResp, ...],
    on_server_init: OnServerInit = on_exclusive_access_server_init,
    on_client_connect: Callable = single_client_only_connect,
    on_client_disconnect: Callable = single_client_only_disconnect,
) -> Callable:
    server = make_exclusive_access_server(
        responses=responses,
        on_server_init=on_server_init,
        on_client_connect=on_client_connect,
        on_client_disconnect=on_client_disconnect,
    )
    return make_serve_cm(server)

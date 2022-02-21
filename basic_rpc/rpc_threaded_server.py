"""Basic socket server to be used for RPC

The server is synchronous and intended to be used with threads

See rpc_low_level.py for more discussion on the protocol
"""
import threading
import socket
import signal
from typing import Tuple, Any, Callable
from logging import debug, error as log_error
from time import sleep
from contextlib import contextmanager
from functools import wraps, partial

from .rpc_spec import RpcServerSpec, RpcServerResp
from .rpc_low_level import (ClientMsgTypeBytes,
    serialize_exception, REQ_HDR_PREFIX_SIZE, MAJOR_VERSION, deserialize_version,
    serialize_server_init_resp, deserialize_cmd_id, serialize_server_rpc_response,
    ProtocolError, DisconnectedError, ServerRpcError, unexpected_msg_error,
    ServerShutdown, parse_msg_header_from_client, deserialize_msg_size,
)

def log_print(msg:str):
    debug(msg)
    print(msg)

class SocketServer:
    def __init__(self,
                 sock:socket.socket,
                 client: Tuple[str,int],
                 server_spec:RpcServerSpec,
                 spec_dict:dict,
                 shared_data_lock:Tuple[Any, threading.Lock],
                 shutdown_event:threading.Event,
     ):
        self.sock = sock
        self.client = client
        self.server_spec = server_spec
        self.spec_dict = spec_dict
        self.session_established = False
        self.shared_data_lock = shared_data_lock
        self.shutdown_event = shutdown_event

    def close(self):
        debug('closing connection from client...')
        self.session_established = False
        shared_data, local_data, lock = self.shared_data_lock
        with lock:
            self.server_spec.on_client_disconnect(shared_data, local_data)
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()
        debug('closing connection from client complete')

    def send_all(self, bs:bytes):
        sock, shutdown_event = self.sock, self.shutdown_event
        remaining = len(bs)
        debug(f'sending a msg of {remaining} bytes...')
        while remaining:
            try:
                num_sent = sock.send(bs)
            except socket.timeout:
                if shutdown_event.is_set():
                    raise ServerShutdown('server is shutting down', during_send=True)
            remaining -= num_sent
            if remaining == 0:
                return

    def recv_all(self, num:int) -> bytes:
        sock, shutdown_event = self.sock, self.shutdown_event
        chunks = []
        remaining = num
        while remaining:
            try:
                recvd = sock.recv(remaining)
            except socket.timeout:
                if shutdown_event.is_set():
                    raise ServerShutdown('server is shutting down')
                continue
            recvd_len = len(recvd)
            debug(f'got {recvd_len} bytes of data...')
            if recvd_len:
                chunks.append(recvd)
                remaining -= len(recvd)
                if remaining == 0: #done
                    return b''.join(chunks)
            else:
                debug('client disconnected...')
                raise DisconnectedError()

    def get_msg(self):
        sock, shutdown_event = self.sock, self.shutdown_event
        msg_size_bytes = self.recv_all(4)
        msg_size = deserialize_msg_size(msg_size_bytes)
        debug(f'received a message of payload size: {msg_size}')
        if msg_size < REQ_HDR_PREFIX_SIZE:
            raise ProtocolError(f'Reported message size too small: {msg_size}')
        msg_bytes = self.recv_all(msg_size - 4)
        #debug(f'msg_bytes: {msg_bytes}')

        msg_type = parse_msg_header_from_client(msg_bytes)
        payload = msg_bytes[REQ_HDR_PREFIX_SIZE-4:]
        debug(f'received a message of type: {msg_type}')
        return msg_type, payload


    def handle_init(self, msg_type:ClientMsgTypeBytes, msg:bytes):
        if not self.session_established:
            major, minor, patch = deserialize_version(msg)
            if MAJOR_VERSION != major:
                err_str = f'Incorrect major version: {major}. Expected {MAJOR_VERSION}'
                raise ProtocolError(err_str)
            else:
                shared_data, local_data, lock = self.shared_data_lock
                with lock:
                    session_established = self.server_spec.on_client_connect(
                            shared_data, local_data)
                self.session_established = session_established
                debug(f'session established: {session_established}')
                bs = serialize_server_init_resp(session_established)
                debug('sending init response...')
                self.send_all(bs)
                debug('sending init response done...')
                return
        else:
            raise ProtocolError('Initialization already occurred')

    def handle_cmd(self, msg:bytes):
        if len(msg) <  2:
            raise ProtocolError('Insufficient bytes in command.')
        cmd_id = deserialize_cmd_id(msg)

        spec = self.spec_dict.get(cmd_id)
        if not spec:
            raise ProtocolError(f'Unknown command with id: {cmd_id}')
        debug(f"got RPC request: {spec.cmd_id.name}")

        try:
            res = spec.parse_and_call(spec.client_function, msg[2:])
            res_bytes = spec.serialize_response(res)
        except Exception as exc:
            raise ServerRpcError('RPC error') from exc
        bs = serialize_server_rpc_response(res_bytes)
        self.send_all(bs)
        debug(f"finished serving RPC request: {spec.cmd_id.name}")

    def handle_exception(self, exc:Exception, rpc_exception:bool=False):
        response = serialize_exception(exc)
        log_error(f'Exception: {exc}', exc_info=exc)
        self.sock.sendall(response)

    def run(self):
        sock = self.sock

        debug('waiting for a message from client...')
        while True:
            try:
                msg_type, payload = self.get_msg()
                if msg_type == ClientMsgTypeBytes.MSG_CLIENT_INIT:
                    self.handle_init(msg_type, payload)
                elif msg_type == ClientMsgTypeBytes.MSG_CLIENT_RPC_REQ:
                    if not self.session_established:
                        raise ProtocolError('Need to initialize connection first')
                    self.handle_cmd(payload)
                else:
                    unexpected_msg_error(ClientMsgTypeBytes.MSG_CLIENT_RPC_REQ, msg_type)
            except DisconnectedError:
                self.close()
                break
            except ServerShutdown as exc:
                debug('server shutdown...')
                if not exc._during_send:
                    self.handle_exception(exc)
                self.close()
                return
            except ProtocolError as exc:
                self.handle_exception(exc)
            except ServerRpcError as exc:
                self.handle_exception(exc.__cause__, rpc_exception=True)
            except Exception as exc:
                self.handle_exception(exc)
                break

def safe_join(thread:threading.Thread):
    """signals don't gel with threads so don't call blocking calls directly"""
    while True:
        if not thread.is_alive():
            return
        sleep(0.2)

def accepter(
        host:Tuple[str,int],
        server_spec:RpcServerSpec,
        shutdown_event:threading.Event,
):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
    sock.bind(host)
    sock.listen(server_spec.max_concurrent_connections)
    sock.settimeout(2.0)
    spec_dict = {spec.cmd_id.value:spec for spec in server_spec.responses}
    (shared_data, local_data_gen), lock = server_spec.on_server_init(), threading.Lock()

    child_threads = []

    host_name, port = host
    displayed_host_name = 'all interfaces' if host_name == '' else host_name
    log_print(f'listening for connections on {displayed_host_name} on port: {port}')

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
                sock = client_sock,
                client = (client_sock, address),
                shutdown_event = shutdown_event,
                server_spec = server_spec,
                spec_dict = spec_dict,
                shared_data_lock = (shared_data, local_data_gen(), lock),
        )
        thread = threading.Thread(target=serve_client_thread, args=(socket_server,))
        child_threads[:] = (t for t in child_threads if t.is_alive())
        child_threads.append(thread)
        thread.start()
    debug("waiting for down client threads")
    for thread in child_threads:
        safe_join(thread)
    debug("client threads shutdown")
    sock.close()
    server_spec.on_server_close(shared_data)
    debug("accepter thread done")


def serve_client_thread(socket_server:SocketServer):
    socket_server.run()

def serve(host_name:str,
          port:int,
          server_spec:RpcServerSpec,
          _shutdown_event:threading.Event=None,
):
    shutdown_event = _shutdown_event or threading.Event()
    accepter_thread = threading.Thread(
            target=accepter,
            kwargs = {
                'host' : (host_name, port),
                'server_spec' : server_spec,
                'shutdown_event' : shutdown_event,
            }
    )

    try:
        accepter_thread.start()

        safe_join(accepter_thread)
        debug('accepter thread finished')
    except (Exception, KeyboardInterrupt) as exc:
        log_error(f'Exception: {exc}', exc_info=exc)
        shutdown_event.set()
        safe_join(accepter_thread)
        debug('accepter thread finished')
        raise

def make_serve(server_spec:RpcServerSpec) -> Callable:
    return partial(serve, server_spec = server_spec)

def make_serve_cm(server:Callable) -> Callable:
    """Make a context manager in which to run the supplied server
    :param: Already made server (using something like `make_serve`)
    :returns: The context manager
    """
    shutdown_event = threading.Event()
    @contextmanager
    def serve_cm(
          host_name:str,
          port:int,
    ) -> Callable:
        kwargs = {
                '_shutdown_event' : shutdown_event,
                'host_name' : host_name,
                'port' : port,
        }

        server_thread = threading.Thread(target=server, kwargs=kwargs)
        server_thread.start()
        debug("Started server thread from context manager...")
        try:
            yield
        finally:
            debug('Shutting down server')
            shutdown_event.set()
            safe_join(server_thread)
            debug('Shutdown complete')

    return serve_cm


#####################################################
# Logic to implement an exclusive access RPC service

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

gen_exclusive_access_data = lambda: {'user_connected': False}

def make_exclusive_access_server(responses:Tuple[RpcServerResp]) -> Callable:
    server_spec = RpcServerSpec(
            responses = responses,
            # return an instance of shared data as well as the factory function
            # which produced it because it just so happens that the shared data
            # shape matches the local data in this instance
            on_server_init = lambda: (gen_exclusive_access_data(),
                                      gen_exclusive_access_data),
            on_client_connect = single_client_only_connect,
            on_client_disconnect = single_client_only_disconnect,
    )
    return make_serve(server_spec)

def make_exclusive_access_server_cm(*responses:Tuple[RpcServerResp]) -> Callable:
    server = make_exclusive_access_server(responses)
    return make_serve_cm(server)


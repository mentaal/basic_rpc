# basic-socket-rpc

It's in the name, basic RPC implementation with just enough functionality to
solve my own problem.

Currently implemented a threaded server and a blocking client though there's no
reason an async server/client couldn't be added.

See [greet_server.py](greet_server.py) and [greet_client.py](greet_client.py)
for example usage.

Constraints:
1. Supports up to 16k registered commands (due to low level command id
   represented as a 16 bit integer)
2. Runs on Python 3.6+.

Pros:
1. No dependencies
2. Support for managing amount of concurrent users of the RPC service. (I need
   exclusive access).
3. Client/Server decoupled from user code.
4. Some basic support for exceptions.

Cons:
1. No encryption/authentication
2. user code responsible for serializing/deserializing messages.
   The RPC protocol is not aware of the type of arguments/responses being sent.
   I.e. the message payload is tightly coupled with user code.
   The tediousness of this is mitigated somewhat by using composable
   serializer/deserializer functions such as `make_serializer`. See
   [conftest.py][1] for an example of this.
   The serialization helper functions reside in [rpc_serialization_functions][2].
3. No autogeneration logic to generate RPC APIs.
4. parsing of large messages slow due to inefficient copying

[1]: tests/conftest.py
[2]: basic_rpc/rpc_serialization_functions.py

## Explanation of how it works
This RPC library provides provides (de-)serialization functions to serialize
data primitives to bytes which are then transmitted over a socket using a
minimal protocol. Basically just prefixing the serialized data with a message
type and a size. See
[rpc_low_level.py](basic_rpc/rpc_serialization_functions.py) for more info.
You are expected to construct the serializer and deserializer
for the function you wish to use over an RPC channel yourself and if they don't
match, then you'll get a message transmission error that is hard to debug. The
simplest thing to do is to visually compare your definition of the server and
client and see if the (de-)serialization names match.

For example, say the server definition is: (from [greet_server.py](greet_server.py))

    greet_server_spec = RpcServerSpec(
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
                cmd_id=greet_server_cmd_ids.func_no_args,
                parse_and_call=call_no_args,
                serialize_response=serialize_no_response,
                client_function=lambda: print("hello with no args") or None,
            ),
        ),
    )

With the associated client definition:


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
                serialize_request=serialize_no_response,
                parse_response=parse_no_response,
            ),
        ],
    )

Note how the (de-)serialization functions names are somewhat reciprocal.
The `add_2_words` command on the server side deserializes its two arguments with
`make_server_deserializer(parse_int_from_le_bytes_4,
parse_int_from_le_bytes_4),`.
On the client side those two arguments are serialized with
`serialize_request=make_serializer(int_to_le_bytes_4, int_to_le_bytes_4),`

Similarly with the `hello` command. The server side deserializes the command
argument with `make_server_deserializer(deserialize_str)` and client serializes
it with `serialize_request=serialize_str`.

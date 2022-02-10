# basic_rpc

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

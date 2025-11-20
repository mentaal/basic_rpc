"""Module to hold serialization functions"""

from functools import partial, wraps
from itertools import chain, repeat
from typing import Any, Callable, Iterable, Tuple, TypeVar, Union


Buffer = Union[memoryview, bytes]
T = TypeVar("T")
DeserializeResult = Tuple[T, Buffer]
Deserializer = Callable[[Buffer], DeserializeResult]


def int_to_le_bytes(num_bytes: int, num: int) -> bytes:
    return num.to_bytes(num_bytes, "little")


int_to_le_bytes_8 = partial(int_to_le_bytes, 8)
int_to_le_bytes_4 = partial(int_to_le_bytes, 4)
int_to_le_bytes_2 = partial(int_to_le_bytes, 2)
int_to_le_bytes_1 = partial(int_to_le_bytes, 1)


def int_from_le_bytes(num_bytes: int, bs: Buffer) -> int:
    to_parse = bs[:num_bytes]
    len_to_parse = len(to_parse)
    if len_to_parse != num_bytes:
        raise ValueError(f"Not enough bytes to parse to integer. Expected: {num_bytes}. Got {len_to_parse}")
    return int.from_bytes(to_parse, "little")


int_from_le_bytes_8 = partial(int_from_le_bytes, 8)
int_from_le_bytes_4 = partial(int_from_le_bytes, 4)
int_from_le_bytes_2 = partial(int_from_le_bytes, 2)
int_from_le_bytes_1 = partial(int_from_le_bytes, 1)


def parse_int_from_le_bytes(num_bytes: int, bs: Buffer) -> DeserializeResult[int]:
    bs_len = len(bs)
    if bs_len < num_bytes:
        raise ValueError(f"Insufficient number of bytes to parse. Expected {num_bytes}, got: {bs_len}")
    return int_from_le_bytes(num_bytes, bs), bs[num_bytes:]


parse_int_from_le_bytes_8 = partial(parse_int_from_le_bytes, 8)
parse_int_from_le_bytes_4 = partial(parse_int_from_le_bytes, 4)
parse_int_from_le_bytes_2 = partial(parse_int_from_le_bytes, 2)
parse_int_from_le_bytes_1 = partial(parse_int_from_le_bytes, 1)


def serialize_bool(b: bool) -> bytes:
    return b"\x01" if b else b"\x00"


def serialize_cmd_msg_size(msg: bytes) -> bytes:
    return int_to_le_bytes_4(len(msg))


def deserialize_cmd_msg_size(bs: Buffer) -> DeserializeResult[int]:
    return (int_from_le_bytes_4(bs), bs[4:])


def deserialize_bool(bs: Buffer) -> DeserializeResult[bool]:
    b, remaining = bs[0], bs[1:]
    return bool(b), remaining


def make_deserialize_only(
    deserializer: Callable,
    type_to_deserialize: str,
    check_no_remaining: bool = True,
) -> Callable:
    error_template = f"Expected no trailing bytes when deserializing {type_to_deserialize}. Got: {{len_remaining}}"
    if check_no_remaining:

        @wraps(deserializer)
        def only_deserializer(*args, **kwargs):
            res, remaining = deserializer(*args, **kwargs)
            len_remaining = len(remaining)
            if len_remaining:
                raise ValueError(error_template.format(len_remaining=len_remaining))
            return res  # discard remaining

    else:

        @wraps(deserializer)
        def only_deserializer(*args, **kwargs):
            res, remaining = deserializer(*args, **kwargs)
            return res  # discard remaining

    return only_deserializer


deserialize_bool_only = make_deserialize_only(deserialize_bool, "bool")


def serialize_str(s: str) -> bytes:
    raw_msg_bytes = s.encode()
    return b"".join(
        [
            serialize_cmd_msg_size(raw_msg_bytes),
            raw_msg_bytes,
        ]
    )


def serialize_bytes(bs: bytes) -> bytes:
    return b"".join(
        [
            serialize_cmd_msg_size(bs),
            bs,
        ]
    )


def deserialize_str(bs: Buffer) -> DeserializeResult[str]:
    """Returns the deserialized string and the remaining bytes after the string"""
    size, str_bytes = deserialize_cmd_msg_size(bs)
    str_bytes_len = len(str_bytes)
    if str_bytes_len < size:
        raise ValueError(f"Cannot deserialize string, missing bytes. Expected {size}, got {str_bytes_len}.")
    return str_bytes[:size].tobytes().decode(), str_bytes[size:]


deserialize_str_only = make_deserialize_only(deserialize_str, "string")


def deserialize_bytes(bs: Buffer) -> DeserializeResult[bytes]:
    """Returns the deserialized msg bytes and the remaining bytes after the msg"""
    size, payload_bytes = deserialize_cmd_msg_size(bs)
    payload_bytes_len = len(payload_bytes)
    if payload_bytes_len < size:
        raise ValueError(f"Cannot deserialize msg, missing bytes. Expected {size}, got {payload_bytes_len}.")
    return payload_bytes[:size], payload_bytes[size:]


deserialize_bytes_only = make_deserialize_only(deserialize_bytes, "bytes")


def _deserializer_helper(parsers: Iterable[Callable], bs: Buffer) -> Iterable[Any]:
    for p in parsers:
        result, bs = p(bs)
        yield result
    if len(bs) != 0:
        raise ValueError(f"ParseError, {len(bs)} unparsed bytes remaining in message")


def _deserializer_helper_and_remaining(parsers: Iterable[Callable], bs: Buffer) -> Iterable[Any]:
    for p in parsers:
        result, bs = p(bs)
        yield result
    yield bs  # remaining Buffer - needs to be accounted for in calling code


def to_memory_view(bs: Buffer) -> memoryview:
    if isinstance(bs, memoryview):
        return bs
    else:
        return memoryview(bs)


def make_deserializer(*args, _and_remaining=False) -> Deserializer:
    if _and_remaining:

        def deserializer_and_remaining(bs: Buffer) -> DeserializeResult:
            *results, remaining_bs = _deserializer_helper_and_remaining(args, to_memory_view(bs))
            return results, remaining_bs

        deserializer = deserializer_and_remaining
    else:

        def deserializer_no_remaining(bs: Buffer) -> DeserializeResult:
            return tuple(_deserializer_helper(args, to_memory_view(bs)))

        deserializer = deserializer_no_remaining
    return deserializer


def make_server_deserializer(*args) -> Callable:
    deserializer = make_deserializer(*args)

    def deserialize_and_call(func: Callable, bs: Buffer):
        return func(*deserializer(bs))

    return deserialize_and_call


def make_client_deserializer(*args) -> Callable:
    return make_deserializer(*args)


def make_deserializer_to_tuple(*args) -> Callable:
    def deserialize(bs: Buffer):
        return tuple(_deserializer_helper(args, bs))

    return deserialize


def make_serializer(*serializer_funcs, _splat_args: bool = True) -> Callable:
    len_serializer_funcs = len(serializer_funcs)

    def serializer_args(args):
        len_args = len(args)
        if len_serializer_funcs != len_args:
            raise ValueError(f"Number of supplied args: {len_args} doesn't match expectation: {len_serializer_funcs}")
        return b"".join((f(a) for f, a in zip(serializer_funcs, args)))

    if _splat_args:

        def serializer_splat_args(*args):
            return serializer_args(args)

        serializer = serializer_splat_args
    else:
        serializer = serializer_args

    return serializer


def make_apply(func):
    """Useful when using `make_serializer` in a server context when serializing
    the response from the invoked function.
    """

    def apply(args):
        return func(*args)

    return apply


def make_serialize_array_fixed_size(
    num_elements: int,
    element_serializer: Callable,
) -> Callable:
    def array_serializer_fixed_size(array: Iterable) -> bytes:
        serialized_elements = list(map(element_serializer, array))

        payload = b"".join(serialized_elements)
        if len(serialized_elements) != num_elements:
            raise ValueError(f"Expected {num_elements} elements in array. Got: " f"{len(serialized_elements)}")
        return payload

    return array_serializer_fixed_size


def make_deserialize_array_fixed_size(
    num_elements: int,
    element_parser: Callable,
) -> Callable:
    def array_deserializer_fixed_size(bs: Buffer) -> Tuple[Any, Buffer]:
        parsers = repeat(element_parser, num_elements)
        *res, remaining = _deserializer_helper_and_remaining(parsers, bs)
        len_res = len(res)

        # this is a redundant check. The element parser should complain when it
        # receives an empty byte array
        if len_res != num_elements:
            raise ValueError(f"Expected {num_elements} elements. Only got {len_res}")
        return res, remaining

    return array_deserializer_fixed_size


def make_serialize_array(element_serializer: Callable) -> Callable:
    def array_serializer(array: Iterable) -> bytes:
        serialized_elements = list(map(element_serializer, array))
        num_elements = len(serialized_elements)
        payload = b"".join(chain([int_to_le_bytes_4(num_elements)], serialized_elements))
        return serialize_bytes(payload)

    return array_serializer


def make_deserialize_array(element_parser: Callable) -> Callable:
    def deserialize_array(bs: Buffer) -> Tuple[Any, Buffer]:
        array_def, remaining_bs = deserialize_bytes(bs)
        array_len, array_bs = parse_int_from_le_bytes_4(array_def)
        parsers = repeat(element_parser, array_len)
        deserialized = tuple(_deserializer_helper(parsers, array_bs))
        return deserialized, remaining_bs

    return deserialize_array


def call_no_args(func: Callable, bs: Buffer):
    if len(bs):
        raise ValueError("Expected no arguments in function invocation")
    return func()


def serialize_no_response(_):
    return b""


def client_call_no_args():
    return b""


def parse_no_response(bs: Buffer):
    len_bs = len(bs)
    if len_bs:
        raise ValueError(f"Unexpected response from server. Got {len_bs}")
    return

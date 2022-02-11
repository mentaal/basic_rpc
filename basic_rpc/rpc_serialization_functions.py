"""Module to hold serialization functions
"""
from functools import partial, wraps
from itertools import repeat
from typing import Tuple, Callable, Any, Iterable, List
from logging import debug

ParseResult = Tuple[Any, bytes]

int_to_le_bytes = lambda num_bytes, num: num.to_bytes(num_bytes, 'little')

int_to_le_bytes_8 = partial(int_to_le_bytes, 8)
int_to_le_bytes_4 = partial(int_to_le_bytes, 4)
int_to_le_bytes_2 = partial(int_to_le_bytes, 2)
int_to_le_bytes_1 = partial(int_to_le_bytes, 1)

def int_from_le_bytes(num_bytes:int, bs:bytes) -> int:
    to_parse = bs[:num_bytes]
    len_to_parse = len(to_parse)
    if len_to_parse != num_bytes:
        raise ValueError(f'Not enough bytes to parse to integer. Expected: {num_bytes}. Got {len_to_parse}')
    return int.from_bytes(to_parse, 'little')

int_from_le_bytes_8 = partial(int_from_le_bytes, 8)
int_from_le_bytes_4 = partial(int_from_le_bytes, 4)
int_from_le_bytes_2 = partial(int_from_le_bytes, 2)
int_from_le_bytes_1 = partial(int_from_le_bytes, 1)

def parse_int_from_le_bytes(num_bytes:int, bs:bytes) -> ParseResult:
    bs_len = len(bs)
    if bs_len < num_bytes:
        raise ValueError('Insufficient number of bytes to parse')
    return int_from_le_bytes(num_bytes, bs), bs[num_bytes:]

parse_int_from_le_bytes_8 = partial(parse_int_from_le_bytes, 8)
parse_int_from_le_bytes_4 = partial(parse_int_from_le_bytes, 4)
parse_int_from_le_bytes_2 = partial(parse_int_from_le_bytes, 2)
parse_int_from_le_bytes_1 = partial(parse_int_from_le_bytes, 1)

serialize_bool = lambda b: b'\x01' if b else b'\x00'

serialize_cmd_msg_size = lambda msg: int_to_le_bytes_4(len(msg))
deserialize_cmd_msg_size = lambda bs: (int_from_le_bytes_4(bs), bs[4:])

def deserialize_bool(bs:bytes) -> Tuple[bool, bytes]:
    len_bs = len(bs)
    b, remaining = bs[0], bs[1:]
    return bool(b), remaining

def make_deserialize_only(
        deserializer:Callable,
        type_to_deserialize:str,
        check_no_remaining:bool=True,
) -> Callable:
    error_template = f'Expected no trailing bytes when deserializing {type_to_deserialize}. Got: {{len_remaining}}'
    if check_no_remaining:
        @wraps(deserializer)
        def only_deserializer(*args, **kwargs):
            res, remaining = deserializer(*args, **kwargs)
            len_remaining = len(remaining)
            if len_remaining:
                raise ValueError(error_template.format(len_remaining = len_remaining))
            return res # discard remaining
    else:
        @wraps(deserializer)
        def only_deserializer(*args, **kwargs):
            res, remaining = deserializer(*args, **kwargs)
            return res # discard remaining
    return only_deserializer

deserialize_bool_only = make_deserialize_only(deserialize_bool, 'bool')

def serialize_str(s:str) -> bytes:
    raw_msg_bytes = s.encode()
    return b''.join([
        serialize_cmd_msg_size(raw_msg_bytes),
        raw_msg_bytes,
    ])

def serialize_bytes(bs:bytes) -> bytes:
    return b''.join([
        serialize_cmd_msg_size(bs),
        bs,
    ])

def deserialize_str(bs:bytes) -> Tuple[str, bytes]:
    """Returns the deserialized string and the remaining bytes after the string
    """
    size, str_bytes = deserialize_cmd_msg_size(bs)
    if len(str_bytes) < size:
        raise ValueError('Cannot deserialize string, missing bytes')
    return str_bytes[:size].decode(), str_bytes[size:]

deserialize_str_only = make_deserialize_only(deserialize_str, 'string')

def deserialize_bytes(bs:bytes) -> Tuple[bytes]:
    """Returns the deserialized msg bytes and the remaining bytes after the msg
    """
    size, payload_bytes = deserialize_cmd_msg_size(bs)
    if len(payload_bytes) < size:
        raise ValueError('Cannot deserialize msg, missing bytes')
    return payload_bytes[:size], payload_bytes[size:]

deserialize_bytes_only = make_deserialize_only(deserialize_bytes, 'bytes')

def _deserializer_helper(parsers:Iterable[Callable], bs:bytes) -> Iterable[Any]:
    for p in parsers:
        result, bs = p(bs)
        yield result
    if len(bs) != 0:
        raise ValueError(f'ParseError, {len(bs)} unparsed bytes remaining in message')

def _deserializer_helper_and_remaining(parsers:Iterable[Callable], bs:bytes) -> Iterable[Any]:
    for p in parsers:
        result, bs = p(bs)
        yield result
    yield bs # remaining bytes - needs to be accounted for in calling code

def make_deserializer(*args) -> Callable:
    def deserializer(bs:bytes):
        return _deserializer_helper(args, bs)
    def deserialize_and_call(func:Callable, bs:bytes):
        return func(*deserializer(bs))
    return deserialize_and_call

def make_deserializer_to_tuple(*args) -> Callable:
    def deserialize(bs:bytes):
        return tuple(_deserializer_helper(args, bs))
    return deserialize

def make_serializer(*serializer_funcs) -> Callable:
    len_serializer_funcs = len(serializer_funcs)
    def serializer(*args):
        len_args = len(args)
        if len_serializer_funcs != len_args:
            raise ValueError(f"Number of supplied args: {len_args} doesn't match expectation: {len_serializer_funcs}")
        return b''.join((f(a) for f,a in zip(serializer_funcs, args)))
    return serializer

def make_apply(func):
    """Useful when using `make_serializer` in a server context when serializing
    the response from the invoked function.
    """
    def starmapped(kwargs):
        return func(*kwargs)
    return starmapped

def make_serialize_array_fixed_size(
        num_elements:int,
        element_size:int,
        element_serializer:Callable,
) -> Callable:
    expected_payload_size = num_elements*element_size
    def array_serializer_fixed_size(array:Iterable) -> bytes:
        payload = b''.join(map(element_serializer, array))
        len_payload = len(payload)
        if expected_payload_size != len_payload:
            raise ValueError(f"Serialized payload size: {len_payload} doesn't "
                    "match expectation: {expected_payload_size}. Are you sure "
                    "you provided {num_elements}?")
        return payload
    return array_serializer_fixed_size

def make_deserialize_array_fixed_size(
        num_elements:int,
        element_size:int,
        element_parser:Callable,
) -> Callable:
    min_required_bytes = num_elements * element_size
    def array_deserializer_fixed_size(bs:bytes) -> Tuple[Any, bytes]:
        len_bs = len(bs)
        if len_bs < min_required_bytes:
            raise ValueError(f'Got {len_bs} bytes but require {len_bs} to parse fixed size array')
        parsers = repeat(element_parser, num_elements)
        *res, remaining = _deserializer_helper_and_remaining(parsers, bs)
        len_res = len(res)

        # this is a redundant check. The element parser should complain when it
        # receives an empty byte array
        if len_res != num_elements:
            raise ValueError(f'Expected {num_elements} elements. Only got {len_res}')
        return res, remaining

    return array_deserializer_fixed_size

def make_serialize_array(
        element_size:int,
        element_serializer:Callable,
) -> Callable:
    def array_serializer(array:Iterable) -> bytes:
        payload = b''.join(map(element_serializer, array))
        return serialize_bytes(payload)
    return array_serializer

def make_deserialize_array(element_size:int, element_parser:Callable) -> Callable:
    def deserialize_array(bs:bytes) -> Tuple[Any, bytes]:
        array_bs, remaining_bs = deserialize_bytes(bs)
        len_array_bs = len(array_bs)
        num_elements, remainder = divmod(len_array_bs, element_size)
        if remainder:
            raise ValueError('Parsing error when deserializing array of size '
                f'{len_array_bs}. It is not an integer multiple of {element_size}')
        parsers = repeat(element_parser, num_elements)
        return tuple(_deserializer_helper(parsers, array_bs)), remaining_bs
    return deserialize_array

def call_no_args(func:Callable, bs:bytes):
    if len(bs):
        raise ValueError('Expected no arguments in function invocation')
    return func()

def parse_no_response(bs:bytes):
    len_bs = len(bs)
    if len_bs:
        raise ValueError(f'Unexpected response from server. Got {len_bs}')
    return

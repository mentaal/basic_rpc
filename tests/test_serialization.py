import random
from typing import Tuple, Any, List
from basic_rpc.rpc_serialization_functions import (
    int_to_le_bytes_4,
    int_to_le_bytes_2,
    make_serialize_array_fixed_size,
    make_serialize_array,
    parse_int_from_le_bytes_4,
    parse_int_from_le_bytes_2,
    make_deserialize_array,
    make_deserialize_array_fixed_size,
    serialize_bytes,
    make_serializer,
    make_deserializer,
    deserialize_bytes,
)


def test_sanity():
    bs = bytes([0, 1, 2])
    serialized = serialize_bytes(bs)
    deserialized, remaining = deserialize_bytes(serialized)
    assert deserialized == bs

    se = make_serializer(int_to_le_bytes_4, int_to_le_bytes_2)
    de = make_deserializer(parse_int_from_le_bytes_4, parse_int_from_le_bytes_2)

    to_serialize = 4, 2

    serialized = se(*to_serialize)
    # breakpoint()
    deserialized = tuple(de(serialized))

    assert deserialized == to_serialize


# serializer for profiling testing
se = make_serializer(
    int_to_le_bytes_2,
    make_serialize_array_fixed_size(50_000, int_to_le_bytes_4),
    make_serialize_array(int_to_le_bytes_4),
)
de = make_deserializer(
    parse_int_from_le_bytes_2,
    make_deserialize_array_fixed_size(50_000, parse_int_from_le_bytes_4),
    make_deserialize_array(parse_int_from_le_bytes_4),
)

rand_int = lambda: random.randint(0, 0xFFFF_FFFF)  # noqa: E731
rand_int_2 = lambda: random.randint(0, 0xFFFF)  # noqa: E731


def create_random_args_for_serialization() -> Tuple[Any, ...]:
    return (
        rand_int_2(),
        list(rand_int() for _ in range(50_000)),
        tuple(rand_int() for _ in range(200_000)),
    )


def test_profile():
    to_serialize = create_random_args_for_serialization()
    serialized = se(*to_serialize)
    deserialized = de(serialized)
    assert deserialized == to_serialize

from basic_rpc.rpc_serialization_functions import *


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

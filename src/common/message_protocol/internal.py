import json
import zlib


def serialize(message):
    json_str = json.dumps(message, separators=(',', ':'))
    return zlib.compress(json_str.encode("utf-8"))


def deserialize(message):
    json_str = zlib.decompress(message).decode("utf-8")
    return json.loads(json_str)

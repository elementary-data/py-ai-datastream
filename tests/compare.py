from typing import Callable

from ai_datastream.stream_parts import DataStreamPart, DataStreamStartStep

_COMPARER_MAP: dict[
    type[DataStreamPart], Callable[[DataStreamPart, DataStreamPart], bool]
] = {
    # skips comparing randomly generated message_id
    DataStreamStartStep: lambda a, b: True,
}


def compare_stream_parts(a: DataStreamPart, b: DataStreamPart) -> bool:
    if type(a) != type(b):  # noqa: E721
        return False
    if type(a) in _COMPARER_MAP:
        return _COMPARER_MAP[type(a)](a, b)
    return a == b

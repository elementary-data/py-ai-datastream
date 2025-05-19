import abc
from typing import AsyncGenerator, Generator, Sequence

from ai_datastream.messages import ChatMessage
from ai_datastream.stream_parts import DataStreamPart


class Streamer(abc.ABC):
    @abc.abstractmethod
    def stream(
        self, prompt: str, messages: Sequence[ChatMessage]
    ) -> Generator[DataStreamPart, None, None]:
        pass


class AsyncStreamer(abc.ABC):
    @abc.abstractmethod
    def async_stream(
        self, prompt: str, messages: Sequence[ChatMessage]
    ) -> AsyncGenerator[DataStreamPart, None]:
        pass

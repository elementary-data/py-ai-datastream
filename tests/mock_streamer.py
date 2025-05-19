from typing import AsyncGenerator, Generator, Sequence

from ai_datastream.messages import ChatMessage
from ai_datastream.stream_parts import DataStreamPart
from ai_datastream.streamer import AsyncStreamer, Streamer


class MockStreamer(Streamer, AsyncStreamer):
    def __init__(self, stream_parts: Sequence[DataStreamPart]):
        self.stream_parts = stream_parts

    def stream(
        self, prompt: str, messages: Sequence[ChatMessage]
    ) -> Generator[DataStreamPart, None, None]:
        for part in self.stream_parts:
            yield part

    async def async_stream(
        self, prompt: str, messages: Sequence[ChatMessage]
    ) -> AsyncGenerator[DataStreamPart, None]:
        for part in self.stream_parts:
            yield part

from typing import AsyncGenerator, Generator, Sequence

from starlette.responses import StreamingResponse

from ai_datastream.consts import DATA_STREAM_HEADER, DATA_STREAM_HEADER_VALUE
from ai_datastream.messages import ChatMessage
from ai_datastream.streamer import AsyncStreamer, Streamer

HEADERS = {
    # Makes sure the GZip middleware is not applied.
    "Content-Type": "text/event-stream",
    "Transfer-Encoding": "chunked",
    DATA_STREAM_HEADER: DATA_STREAM_HEADER_VALUE,
}


class AiChatDataStreamSyncResponse(StreamingResponse):
    def __init__(
        self, streamer: Streamer, prompt: str, messages: Sequence[ChatMessage]
    ):
        self.streamer = streamer
        self.prompt = prompt
        self.messages = messages
        super().__init__(
            self._stream(),
            headers=HEADERS,
        )

    def _stream(self) -> Generator[str, None, None]:
        for message in self.streamer.stream(self.prompt, self.messages):
            yield message.format()


class AiChatDataStreamAsyncResponse(StreamingResponse):
    def __init__(
        self, streamer: AsyncStreamer, prompt: str, messages: Sequence[ChatMessage]
    ):
        self.streamer = streamer
        self.prompt = prompt
        self.messages = messages
        super().__init__(
            self._stream(),
            headers=HEADERS,
        )

    async def _stream(self) -> AsyncGenerator[str, None]:
        async for message in self.streamer.async_stream(self.prompt, self.messages):
            yield message.format()

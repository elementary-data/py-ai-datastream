import uuid
from enum import Enum
from typing import Any, AsyncGenerator, Generator, Sequence, Union

from langchain_core.messages import BaseMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph.graph import CompiledGraph
from langgraph.types import Command

from ai_datastream.agent.langgraph.message_parser import LanggraphMessageParser
from ai_datastream.agent.langgraph.stream_converter import LanggraphStreamConverter
from ai_datastream.messages import ChatMessage
from ai_datastream.stream_parts import (
    DataStreamFinishRun,
    DataStreamFinishStep,
    DataStreamFinishStepReason,
    DataStreamPart,
)
from ai_datastream.streamer import AsyncStreamer, Streamer


class StreamStatus(Enum):
    INIT = "init"
    RUNNING = "running"
    FINISHED = "finished"
    INTERRUPTED = "interrupted"


class LanggraphStreamer(Streamer, AsyncStreamer):
    def __init__(self, agent: CompiledGraph, thread_id: Union[str, None] = None):
        self.agent = agent
        self.config = RunnableConfig(
            configurable={"thread_id": thread_id or uuid.uuid4().hex}
        )
        self.converter = LanggraphStreamConverter()
        self.message_parser = LanggraphMessageParser()
        self.status = StreamStatus.INIT

    def _handle_stream_finish(self) -> Generator[DataStreamPart, None, None]:
        try:
            snapshot = self.agent.get_state(self.config)
        except ValueError:
            snapshot = None
        if snapshot and snapshot.next:
            self.status = StreamStatus.INTERRUPTED
            return
        if self.converter.current_message_id is not None:
            self.status = StreamStatus.FINISHED
            if self.converter.current_message_id:
                yield DataStreamFinishStep(DataStreamFinishStepReason.STOP)
            yield DataStreamFinishRun()

    def _parse_stream_input(self, prompt: str, messages: Sequence[ChatMessage]) -> dict:
        return {
            "messages": [
                SystemMessage(prompt),
                *self.message_parser.parse_many(messages),
            ]
        }

    def _stream(self, input: Any) -> Generator[DataStreamPart, None, None]:
        self.status = StreamStatus.RUNNING
        for stream_mode, step in self.agent.stream(
            input,
            self.config,
            stream_mode=self.converter.supported_stream_modes,
        ):
            for message in self.converter.iter_step(stream_mode, step):  # type: ignore[arg-type]
                yield message
        yield from self._handle_stream_finish()

    async def _async_stream(self, input: Any) -> AsyncGenerator[DataStreamPart, None]:
        self.status = StreamStatus.RUNNING
        async for stream_mode, step in self.agent.astream(
            input,
            self.config,
            stream_mode=self.converter.supported_stream_modes,
        ):
            for message in self.converter.iter_step(stream_mode, step):  # type: ignore[arg-type]
                yield message
        for finish_message in self._handle_stream_finish():
            yield finish_message

    def stream(
        self, prompt: str, messages: Sequence[ChatMessage] = []
    ) -> Generator[DataStreamPart, None, None]:
        input = self._parse_stream_input(prompt, messages)
        return self._stream(input)

    async def async_stream(
        self, prompt: str, messages: Sequence[ChatMessage] = []
    ) -> AsyncGenerator[DataStreamPart, None]:
        input = self._parse_stream_input(prompt, messages)
        async for message in self._async_stream(input):
            yield message

    def continue_stream(
        self, interrupt_response: Any
    ) -> Generator[DataStreamPart, None, None]:
        return self._stream(Command(resume=interrupt_response))

    async def async_continue_stream(
        self, interrupt_response: Any
    ) -> AsyncGenerator[DataStreamPart, None]:
        async for message in self._async_stream(Command(resume=interrupt_response)):
            yield message

    def get_messages(self) -> Sequence[BaseMessage]:
        snapshot = self.agent.get_state(self.config)
        messages = snapshot.values.get("messages", [])
        if not isinstance(messages, Sequence):
            return []
        return messages

import uuid
from typing import Callable, Generator, Union

from langchain_core.messages import AIMessageChunk
from langgraph.types import StreamMode

from ai_datastream.stream_parts import (
    DataStreamFinishStep,
    DataStreamFinishStepReason,
    DataStreamPart,
    DataStreamStartStep,
    DataStreamText,
    DataStreamToolCall,
    DataStreamToolCallStart,
    DataStreamToolResult,
)


class LanggraphStreamConverter:
    def __init__(self) -> None:
        self.current_message_id: Union[str, None] = None
        self._running_tool_call_ids: set[str] = set()

    def _iter_ai_message_chunk_content(
        self, message: AIMessageChunk
    ) -> Generator[DataStreamPart, None, None]:
        if not isinstance(message.content, list):
            return
        for content in message.content:
            if isinstance(content, str):
                continue
            elif content.get("type") == "tool_use":
                tool_call_id = content.get("id")
                tool_call_name = content.get("name")
                if tool_call_id and tool_call_name:
                    yield DataStreamToolCallStart(tool_call_id, tool_call_name)

    def iter_message_step(
        self, step: tuple[AIMessageChunk, dict]
    ) -> Generator[DataStreamPart, None, None]:
        message, metadata = step
        if isinstance(message, AIMessageChunk):
            if self.current_message_id is None:
                self.current_message_id = uuid.uuid4().hex
                yield DataStreamStartStep(self.current_message_id)
            text = message.text()
            if text:
                yield DataStreamText(text)
            yield from self._iter_ai_message_chunk_content(message)

    def _iter_update_messages(
        self, step: dict
    ) -> Generator[DataStreamPart, None, None]:
        messages = step.get("agent", {}).get("messages", [])
        ai_message = messages[-1] if messages else None
        if ai_message and ai_message.tool_calls:
            for tool_call in ai_message.tool_calls:
                tool_call_id = tool_call["id"]
                self._running_tool_call_ids.add(tool_call_id)
                yield DataStreamToolCall(
                    tool_call_id,
                    tool_call["name"],
                    tool_call["args"],
                )

    def _iter_tool_results(self, step: dict) -> Generator[DataStreamPart, None, None]:
        tool_messages = step.get("tools", {}).get("messages", [])
        for tool_message in tool_messages:
            if tool_message.tool_call_id in self._running_tool_call_ids:
                self._running_tool_call_ids.remove(tool_message.tool_call_id)
            yield DataStreamToolResult(
                tool_message.tool_call_id,
                tool_message.content,
            )
        if tool_messages:
            yield DataStreamFinishStep(DataStreamFinishStepReason.TOOL_CALLS)
            self.current_message_id = None

    def iter_update_step(self, step: dict) -> Generator[DataStreamPart, None, None]:
        yield from self._iter_update_messages(step)
        yield from self._iter_tool_results(step)

    def _get_stream_mode_map(self) -> dict[StreamMode, Callable]:
        return {
            "messages": self.iter_message_step,
            "updates": self.iter_update_step,
        }

    def iter_step(
        self, stream_mode: StreamMode, step: Union[tuple[AIMessageChunk, dict], dict]
    ) -> Generator[DataStreamPart, None, None]:
        stream_mode_map = self._get_stream_mode_map()
        if stream_mode not in stream_mode_map:
            raise ValueError(f"Unknown stream mode: {stream_mode}")
        yield from stream_mode_map[stream_mode](step)

    @property
    def supported_stream_modes(self) -> list[StreamMode]:
        return list(self._get_stream_mode_map().keys())

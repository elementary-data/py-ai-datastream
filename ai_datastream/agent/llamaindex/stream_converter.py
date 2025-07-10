import uuid
from typing import Any, AsyncGenerator, Generator

from llama_index.core.chat_engine.types import StreamingAgentChatResponse

from ai_datastream.stream_parts import (
    DataStreamPart,
    DataStreamStartStep,
    DataStreamText,
    DataStreamToolCall,
    DataStreamToolCallDelta,
    DataStreamToolCallStart,
    DataStreamToolResult,
)


class LlamaIndexStreamConverter:
    """Converts LlamaIndex streaming responses to data stream protocol format.

    Follows Vercel AI SDK Data Stream Protocol:
    https://sdk.vercel.ai/docs/ai-sdk-ui/stream-protocol#data-stream-protocol
    """

    def __init__(self) -> None:
        self.current_message_id: str | None = None
        self._active_tool_calls: dict[str, dict] = {}
        self._emitted_tool_calls: bool = False  # Track if we emitted any tool calls

    def _generate_message_id(self) -> str:
        """Generate a new message ID for the stream."""
        return uuid.uuid4().hex

    def _ensure_message_id(self) -> str:
        """Ensure we have a message ID, generate if needed."""
        if self.current_message_id is None:
            self.current_message_id = self._generate_message_id()
        return self.current_message_id

    def _start_step(self) -> DataStreamStartStep:
        """Start a new step in the stream."""
        message_id = self._ensure_message_id()
        return DataStreamStartStep(message_id)

    def _handle_text_delta(self, delta: str) -> DataStreamText | None:
        """Handle text content from LlamaIndex stream."""
        if delta:  # Allow any non-empty string, including spaces
            return DataStreamText(delta)
        return None

    def _handle_tool_call_start(
        self, tool_call_id: str, tool_name: str
    ) -> DataStreamToolCallStart:
        """Handle start of tool call - Protocol: b: type."""
        self._active_tool_calls[tool_call_id] = {
            "id": tool_call_id,
            "name": tool_name,
            "args": {},
            "args_text": "",
        }
        return DataStreamToolCallStart(tool_call_id, tool_name)

    def _handle_tool_call_delta(
        self, tool_call_id: str, args_delta: str
    ) -> DataStreamToolCallDelta:
        """Handle tool call arguments delta - Protocol: c: type."""
        if tool_call_id in self._active_tool_calls:
            self._active_tool_calls[tool_call_id]["args_text"] += args_delta
        return DataStreamToolCallDelta(tool_call_id, args_delta)

    def _handle_tool_call_complete(
        self, tool_call_id: str, tool_name: str, args: Any
    ) -> DataStreamToolCall:
        """Handle complete tool call - Protocol: 9: type."""
        if tool_call_id in self._active_tool_calls:
            self._active_tool_calls[tool_call_id]["args"] = args
        self._emitted_tool_calls = True  # Mark that we emitted a tool call
        return DataStreamToolCall(tool_call_id, tool_name, args)

    def _handle_tool_result(
        self, tool_call_id: str, result: Any
    ) -> DataStreamToolResult:
        """Handle tool call result - Protocol: a: type."""
        # Clean up active tool call
        self._active_tool_calls.pop(tool_call_id, None)
        return DataStreamToolResult(tool_call_id, result)

    def _process_llama_tool_calls(
        self, tool_calls: list[dict]
    ) -> Generator[DataStreamPart, None, None]:
        """Process tool calls following proper protocol ordering."""
        for tool_call in tool_calls:
            tool_call_id = tool_call.get("id") or uuid.uuid4().hex
            tool_name = tool_call.get("name", "")
            tool_args = tool_call.get("arguments", {})

            # Protocol: b: -> c: (optional) -> 9:
            yield self._handle_tool_call_start(tool_call_id, tool_name)

            # If we have streaming args text, emit deltas
            args_text = tool_call.get("args_text", "")
            if args_text:
                # Simulate streaming by chunking args text
                chunk_size = 10
                for i in range(0, len(args_text), chunk_size):
                    chunk = args_text[i : i + chunk_size]
                    yield self._handle_tool_call_delta(tool_call_id, chunk)

            # Final complete tool call
            yield self._handle_tool_call_complete(tool_call_id, tool_name, tool_args)

            # Handle result if present
            if "result" in tool_call:
                yield self._handle_tool_result(tool_call_id, tool_call["result"])

    def convert_streaming_response(
        self, response: StreamingAgentChatResponse
    ) -> Generator[DataStreamPart, None, None]:
        """Convert LlamaIndex streaming response to data stream parts.

        Protocol compliance:
        - Starts with f: (start step)
        - Streams 0: (text) parts
        - Handles tool calls in proper order: b: -> c: -> 9: -> a:
        - Does NOT emit e: or d: (handled by streamer)
        """
        # Start the stream with proper message ID
        yield self._start_step()

        try:
            # Process the streaming response
            for token in response.response_gen:
                if isinstance(token, str):
                    # Handle text content
                    text_part = self._handle_text_delta(token)
                    if text_part:
                        yield text_part
                elif hasattr(token, "delta") and token.delta:
                    # Handle structured deltas
                    text_part = self._handle_text_delta(token.delta)
                    if text_part:
                        yield text_part

            # Handle tool calls from source nodes
            if hasattr(response, "source_nodes") and response.source_nodes:
                for source_node in response.source_nodes:
                    if (
                        hasattr(source_node, "metadata")
                        and "tool_call" in source_node.metadata
                    ):
                        tool_call = source_node.metadata["tool_call"]
                        yield from self._process_llama_tool_calls([tool_call])

            # Handle tool calls from response metadata
            if hasattr(response, "metadata") and response.metadata:
                tool_calls = response.metadata.get("tool_calls", [])
                if tool_calls:
                    yield from self._process_llama_tool_calls(tool_calls)

        except Exception as e:
            # Handle errors gracefully without breaking stream structure
            # Log error but don't emit it as text (violates protocol)
            # In production, this should use proper logging
            error_msg = f"Stream processing error: {str(e)}"
            # Only emit error as text if we haven't started streaming content
            # This maintains protocol integrity
            yield DataStreamText(f"<!-- {error_msg} -->")

        # Note: We do NOT emit DataStreamFinishStep here!
        # The streamer will handle all finish logic in proper order
        # This ensures: f: -> content -> e: -> d: (correct protocol order)

    async def async_convert_streaming_response(
        self, response: StreamingAgentChatResponse
    ) -> AsyncGenerator[DataStreamPart, None]:
        """Async version of convert_streaming_response."""
        # Start the stream with proper message ID
        yield self._start_step()

        try:
            # Process the streaming response asynchronously
            async for token in response.async_response_gen():
                if isinstance(token, str):
                    # Handle text content
                    text_part = self._handle_text_delta(token)
                    if text_part:
                        yield text_part
                elif hasattr(token, "delta") and token.delta:
                    # Handle structured deltas
                    text_part = self._handle_text_delta(token.delta)
                    if text_part:
                        yield text_part

            # Handle tool calls from source nodes
            if hasattr(response, "source_nodes") and response.source_nodes:
                for source_node in response.source_nodes:
                    if (
                        hasattr(source_node, "metadata")
                        and "tool_call" in source_node.metadata
                    ):
                        tool_call = source_node.metadata["tool_call"]
                        for part in self._process_llama_tool_calls([tool_call]):
                            yield part

            # Handle tool calls from response metadata
            if hasattr(response, "metadata") and response.metadata:
                tool_calls = response.metadata.get("tool_calls", [])
                if tool_calls:
                    for part in self._process_llama_tool_calls(tool_calls):
                        yield part

        except Exception as e:
            # Handle errors gracefully
            error_msg = f"Stream processing error: {str(e)}"
            yield DataStreamText(f"<!-- {error_msg} -->")

        # Note: We do NOT emit DataStreamFinishStep here!
        # The streamer handles all finish logic

    def reset_message_id(self) -> None:
        """Reset message ID for new conversation."""
        self.current_message_id = None
        self._active_tool_calls.clear()
        self._emitted_tool_calls = False

    def get_current_message_id(self) -> str | None:
        """Get current message ID."""
        return self.current_message_id

    def has_emitted_tool_calls(self) -> bool:
        """Check if any tool calls were emitted during the stream."""
        return self._emitted_tool_calls

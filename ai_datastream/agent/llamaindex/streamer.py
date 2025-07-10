import uuid
from typing import AsyncGenerator, Generator, Sequence, Union

from llama_index.core.agent import AgentRunner, CustomSimpleAgentWorker
from llama_index.core.chat_engine.types import StreamingAgentChatResponse
from llama_index.core.memory import ChatMemoryBuffer

from ai_datastream.agent.llamaindex.message_parser import LlamaIndexMessageParser
from ai_datastream.agent.llamaindex.stream_converter import LlamaIndexStreamConverter
from ai_datastream.messages import ChatMessage
from ai_datastream.stream_parts import (
    DataStreamFinishRun,
    DataStreamFinishStep,
    DataStreamFinishStepReason,
    DataStreamPart,
    DataStreamStartStep,
    DataStreamText,
)
from ai_datastream.streamer import AsyncStreamer, Streamer


class LlamaIndexStreamer(Streamer, AsyncStreamer):
    """LlamaIndex agent streamer that follows Vercel AI SDK Data Stream Protocol.

    Ensures proper protocol order: f: -> content -> e: -> d:
    """

    def __init__(
        self,
        agent: Union[AgentRunner, CustomSimpleAgentWorker],
        session_id: Union[str, None] = None,
    ):
        self.agent = agent
        self.session_id = session_id or uuid.uuid4().hex
        self.converter = LlamaIndexStreamConverter()
        self.message_parser = LlamaIndexMessageParser()

        # Set up memory if agent doesn't have it
        if isinstance(agent, AgentRunner) and not agent.memory:
            agent.memory = ChatMemoryBuffer.from_defaults(token_limit=2000)

    def _parse_stream_input(self, prompt: str, messages: Sequence[ChatMessage]) -> str:
        """Parse input messages and add to agent memory."""
        # Convert messages to LlamaIndex format and add to memory
        llama_messages = self.message_parser.parse_many(messages)

        if hasattr(self.agent, "memory") and self.agent.memory:
            for msg in llama_messages:
                self.agent.memory.put(msg)

        return prompt

    def _determine_finish_reason(self) -> DataStreamFinishStepReason:
        """Determine appropriate finish reason based on converter state."""
        # Check if converter emitted any tool calls during the stream
        if self.converter.has_emitted_tool_calls():
            return DataStreamFinishStepReason.TOOL_CALLS
        return DataStreamFinishStepReason.STOP

    def _handle_stream_finish(self) -> Generator[DataStreamPart, None, None]:
        """Handle proper stream finishing in protocol-compliant order.

        Protocol: e: (finish step) -> d: (finish run)
        """
        finish_reason = self._determine_finish_reason()
        yield DataStreamFinishStep(finish_reason)
        yield DataStreamFinishRun()

        # Reset converter state for next conversation
        self.converter.reset_message_id()

    def _handle_non_streaming_response(
        self, response_text: str
    ) -> Generator[DataStreamPart, None, None]:
        """Handle non-streaming responses with proper protocol structure."""
        # Generate message ID and start step
        message_id = self.converter._ensure_message_id()
        yield DataStreamStartStep(message_id)

        # Emit text content
        if response_text:
            yield DataStreamText(response_text)

    def stream(
        self, prompt: str, messages: Sequence[ChatMessage] = []
    ) -> Generator[DataStreamPart, None, None]:
        """Stream LlamaIndex agent response with protocol compliance."""
        input_text = self._parse_stream_input(prompt, messages)

        # Stream the agent response
        if hasattr(self.agent, "stream_chat"):
            response = self.agent.stream_chat(input_text)

            if isinstance(response, StreamingAgentChatResponse):
                # Convert streaming response to data stream parts
                # Converter handles: f: -> 0: -> b:/c:/9:/a: (NO e: or d:)
                for stream_part in self.converter.convert_streaming_response(response):
                    yield stream_part
            else:
                # Handle non-streaming response with proper structure
                response_text = (
                    response.response
                    if hasattr(response, "response")
                    else str(response)
                )
                yield from self._handle_non_streaming_response(response_text)
        else:
            # Handle agent worker case
            response_text = f"Agent worker response to: {input_text}"
            yield from self._handle_non_streaming_response(response_text)

        # Streamer adds final e: -> d: (protocol compliant order)
        yield from self._handle_stream_finish()

    async def async_stream(
        self, prompt: str, messages: Sequence[ChatMessage] = []
    ) -> AsyncGenerator[DataStreamPart, None]:
        """Async version of stream with protocol compliance."""
        input_text = self._parse_stream_input(prompt, messages)

        # Stream the agent response asynchronously
        if hasattr(self.agent, "astream_chat"):
            response = await self.agent.astream_chat(input_text)

            if isinstance(response, StreamingAgentChatResponse):
                # Convert streaming response to data stream parts
                # Converter handles: f: -> 0: -> b:/c:/9:/a: (NO e: or d:)
                async for (
                    stream_part
                ) in self.converter.async_convert_streaming_response(response):
                    yield stream_part
            else:
                # Handle non-streaming response with proper structure
                response_text = (
                    response.response
                    if hasattr(response, "response")
                    else str(response)
                )
                for part in self._handle_non_streaming_response(response_text):
                    yield part
        else:
            # Handle agent worker case
            response_text = f"Agent worker response to: {input_text}"
            for part in self._handle_non_streaming_response(response_text):
                yield part

        # Streamer adds final e: -> d: (protocol compliant order)
        for finish_part in self._handle_stream_finish():
            yield finish_part

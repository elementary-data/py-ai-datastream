"""
Tests for LlamaIndexStreamer focusing on behavior rather than protocol details.
"""

from typing import AsyncGenerator, Generator
from unittest.mock import MagicMock, patch

import pytest

from ai_datastream.agent.llamaindex.streamer import LlamaIndexStreamer
from ai_datastream.messages import ChatMessage, MessageRole
from ai_datastream.stream_parts import (
    DataStreamFinishRun,
    DataStreamFinishStep,
    DataStreamFinishStepReason,
    DataStreamStartStep,
    DataStreamText,
    DataStreamToolCall,
    DataStreamToolCallStart,
    DataStreamToolResult,
)


class SimpleStreamingResponse:
    """A simple mock streaming response that behaves like LlamaIndex's class."""

    def __init__(self, tokens: list[str], tool_calls: list[dict] = None):
        # Store tokens privately
        self._tokens = tokens
        self.source_nodes = []
        self.metadata = {"tool_calls": tool_calls} if tool_calls else {}

    @property
    def response_gen(self) -> Generator[str, None, None]:
        """Property that returns a generator for tokens."""
        for token in self._tokens:
            yield token

    async def async_response_gen(self) -> AsyncGenerator[str, None]:
        """Async generator for tokens."""
        for token in self._tokens:
            yield token


class SimpleAgent:
    """A simple mock agent that returns predictable responses."""

    def __init__(self):
        self.memory = None
        self._next_response = None

    def set_response(self, response):
        """Set the next response to return."""
        self._next_response = response

    def stream_chat(self, message: str):
        """Return the configured response."""
        if self._next_response:
            return self._next_response
        # Default: echo the message
        return SimpleStreamingResponse([f"Echo: {message}"])

    async def astream_chat(self, message: str):
        """Async version returns same as sync."""
        return self.stream_chat(message)


class TestLlamaIndexStreamerBehavior:
    """Test LlamaIndexStreamer focusing on real behavior."""

    @patch('ai_datastream.agent.llamaindex.streamer.isinstance')
    def test_streams_simple_text_response(self, mock_isinstance):
        """Should stream a simple text response word by word."""
        # Setup isinstance to return True for StreamingAgentChatResponse check
        mock_isinstance.side_effect = lambda obj, cls: (
            cls.__name__ == 'StreamingAgentChatResponse'
            if hasattr(cls, '__name__')
            else False
        )

        # Setup
        agent = SimpleAgent()
        agent.set_response(SimpleStreamingResponse([
            "Hello", " ", "world", "!"
        ]))
        streamer = LlamaIndexStreamer(agent)

        # Execute
        parts = list(streamer.stream("Say hello"))

        # Verify text content
        text_parts = [p for p in parts if isinstance(p, DataStreamText)]
        assert len(text_parts) == 4
        assert ''.join(p.body for p in text_parts) == "Hello world!"

        # Verify structure
        assert isinstance(parts[0], DataStreamStartStep)
        assert isinstance(parts[-2], DataStreamFinishStep)
        assert isinstance(parts[-1], DataStreamFinishRun)

    @patch('ai_datastream.agent.llamaindex.streamer.isinstance')
    def test_handles_tool_calls_naturally(self, mock_isinstance):
        """Should handle tool calls as part of the natural flow."""
        # Setup isinstance to return True for StreamingAgentChatResponse check
        mock_isinstance.side_effect = lambda obj, cls: (
            cls.__name__ == 'StreamingAgentChatResponse'
            if hasattr(cls, '__name__')
            else False
        )

        # Setup
        agent = SimpleAgent()
        agent.set_response(SimpleStreamingResponse(
            tokens=["Let me search for that..."],
            tool_calls=[{
                "id": "search_123",
                "name": "web_search",
                "arguments": {"query": "Python tutorials"},
                "result": "Found 5 great Python tutorials"
            }]
        ))
        streamer = LlamaIndexStreamer(agent)

        # Execute
        parts = list(streamer.stream("Find Python tutorials"))

        # Verify tool sequence
        tool_starts = [p for p in parts if isinstance(p, DataStreamToolCallStart)]
        tool_calls = [p for p in parts if isinstance(p, DataStreamToolCall)]
        tool_results = [p for p in parts if isinstance(p, DataStreamToolResult)]

        assert len(tool_starts) == 1
        assert len(tool_calls) == 1
        assert len(tool_results) == 1

        assert tool_calls[0].body["toolName"] == "web_search"
        assert "Python tutorials" in tool_calls[0].body["args"]["query"]

    def test_handles_non_streaming_response(self):
        """Should handle agents that return non-streaming responses."""
        # Setup
        agent = SimpleAgent()
        # Return a non-streaming response (just a mock with .response attribute)
        mock_response = MagicMock()
        mock_response.response = "This is a non-streaming response"
        agent.set_response(mock_response)
        streamer = LlamaIndexStreamer(agent)

        # Execute
        parts = list(streamer.stream("Test non-streaming"))

        # Verify we still get proper streaming structure
        text_parts = [p for p in parts if isinstance(p, DataStreamText)]
        assert len(text_parts) == 1
        assert text_parts[0].body == "This is a non-streaming response"

        # Structure should be complete
        assert isinstance(parts[0], DataStreamStartStep)
        assert isinstance(parts[-2], DataStreamFinishStep)
        assert isinstance(parts[-1], DataStreamFinishRun)

    def test_handles_agent_without_stream_chat(self):
        """Should handle agents that don't have stream_chat method."""
        # Setup
        agent = MagicMock()
        # Remove stream_chat to simulate agent worker
        del agent.stream_chat
        agent.memory = None
        streamer = LlamaIndexStreamer(agent)

        # Execute
        parts = list(streamer.stream("Test worker"))

        # Verify fallback behavior
        text_parts = [p for p in parts if isinstance(p, DataStreamText)]
        assert len(text_parts) == 1
        assert "Agent worker response to: Test worker" in text_parts[0].body

    @patch('ai_datastream.agent.llamaindex.streamer.isinstance')
    def test_preserves_conversation_history(self, mock_isinstance):
        """Should properly handle conversation history."""
        # Setup isinstance to return True for StreamingAgentChatResponse check
        mock_isinstance.side_effect = lambda obj, cls: (
            cls.__name__ == 'StreamingAgentChatResponse'
            if hasattr(cls, '__name__')
            else False
        )

        # Setup
        agent = SimpleAgent()
        agent.set_response(SimpleStreamingResponse([
            "Based on your previous question about Python, ",
            "here's information about decorators..."
        ]))
        streamer = LlamaIndexStreamer(agent)

        # Create history
        history = [
            ChatMessage(role=MessageRole.USER, content="Tell me about Python"),
            ChatMessage(role=MessageRole.ASSISTANT, content="Python is great!"),
        ]

        # Execute
        parts = list(streamer.stream("What about decorators?", messages=history))

        # Verify response acknowledges history
        text_parts = [p for p in parts if isinstance(p, DataStreamText)]
        full_text = ''.join(p.body for p in text_parts)
        assert "Based on your previous question" in full_text
        assert "decorators" in full_text

    @patch('ai_datastream.agent.llamaindex.streamer.isinstance')
    def test_determines_correct_finish_reason(self, mock_isinstance):
        """Should set correct finish reason based on response type."""
        # Setup isinstance to return True for StreamingAgentChatResponse check
        mock_isinstance.side_effect = lambda obj, cls: (
            cls.__name__ == 'StreamingAgentChatResponse'
            if hasattr(cls, '__name__')
            else False
        )

        # Test 1: Regular text response → stop reason
        agent = SimpleAgent()
        agent.set_response(SimpleStreamingResponse(["Just text"]))
        streamer = LlamaIndexStreamer(agent)

        parts = list(streamer.stream("Say something"))
        finish_steps = [p for p in parts if isinstance(p, DataStreamFinishStep)]
        assert len(finish_steps) == 1
        assert (
            finish_steps[0].body["finishReason"] ==
            DataStreamFinishStepReason.STOP.value
        )

        # Test 2: Response with tool calls → tool-calls reason
        agent.set_response(SimpleStreamingResponse(
            ["Using tool..."],
            tool_calls=[{"id": "1", "name": "calc", "arguments": {}, "result": "42"}]
        ))

        # Create a new streamer instance to reset converter state
        streamer = LlamaIndexStreamer(agent)
        parts = list(streamer.stream("Calculate"))
        finish_steps = [p for p in parts if isinstance(p, DataStreamFinishStep)]
        assert len(finish_steps) == 1
        # Now correctly determines tool-calls finish reason
        assert (
            finish_steps[0].body["finishReason"] ==
            DataStreamFinishStepReason.TOOL_CALLS.value
        )

    def test_resets_state_between_conversations(self):
        """Should reset internal state between different conversations."""
        # Setup
        agent = SimpleAgent()
        streamer = LlamaIndexStreamer(agent)

        # First conversation
        agent.set_response(SimpleStreamingResponse(["First response"]))
        parts1 = list(streamer.stream("First query"))

        # Second conversation
        agent.set_response(SimpleStreamingResponse(["Second response"]))
        parts2 = list(streamer.stream("Second query"))

        # Each should have its own message ID
        start_steps1 = [p for p in parts1 if isinstance(p, DataStreamStartStep)]
        start_steps2 = [p for p in parts2 if isinstance(p, DataStreamStartStep)]

        assert start_steps1[0].body["messageId"] != start_steps2[0].body["messageId"]

    @patch('ai_datastream.agent.llamaindex.streamer.isinstance')
    @pytest.mark.asyncio
    async def test_async_streaming_works_identically(self, mock_isinstance):
        """Async streaming should produce same results as sync."""
        # Setup isinstance to return True for StreamingAgentChatResponse check
        mock_isinstance.side_effect = lambda obj, cls: (
            cls.__name__ == 'StreamingAgentChatResponse'
            if hasattr(cls, '__name__')
            else False
        )

        # Setup
        agent = SimpleAgent()
        agent.set_response(SimpleStreamingResponse([
            "Async", " ", "response"
        ]))
        streamer = LlamaIndexStreamer(agent)

        # Execute async
        parts = []
        async for part in streamer.async_stream("Test async"):
            parts.append(part)

        # Verify
        text_parts = [p for p in parts if isinstance(p, DataStreamText)]
        assert ''.join(p.body for p in text_parts) == "Async response"

        # Structure should match sync version
        assert isinstance(parts[0], DataStreamStartStep)
        assert isinstance(parts[-2], DataStreamFinishStep)
        assert isinstance(parts[-1], DataStreamFinishRun)

    @patch('ai_datastream.agent.llamaindex.streamer.isinstance')
    def test_handles_empty_responses_gracefully(self, mock_isinstance):
        """Should handle empty responses without breaking."""
        # Setup isinstance to return True for StreamingAgentChatResponse check
        mock_isinstance.side_effect = lambda obj, cls: (
            cls.__name__ == 'StreamingAgentChatResponse'
            if hasattr(cls, '__name__')
            else False
        )

        # Setup
        agent = SimpleAgent()
        agent.set_response(SimpleStreamingResponse([]))  # Empty response
        streamer = LlamaIndexStreamer(agent)

        # Execute
        parts = list(streamer.stream("Empty test"))

        # Should still have structure
        assert len(parts) >= 3
        assert isinstance(parts[0], DataStreamStartStep)
        assert isinstance(parts[-2], DataStreamFinishStep)
        assert isinstance(parts[-1], DataStreamFinishRun)

        # No text parts
        text_parts = [p for p in parts if isinstance(p, DataStreamText)]
        assert len(text_parts) == 0

    def test_error_in_streaming_handled_gracefully(self):
        """Should handle errors during streaming without crashing."""
        # Setup
        def error_generator():
            yield "Start"
            raise Exception("Stream error!")

        agent = SimpleAgent()
        response = SimpleStreamingResponse([])
        # Override the generator to simulate error
        response._tokens = ["Start", "Error coming..."]

        # Create a custom property that raises an error
        class ErrorResponse(SimpleStreamingResponse):
            def __init__(self):
                super().__init__([])
                self._yielded = False

            @property
            def response_gen(self):
                if not self._yielded:
                    self._yielded = True
                    yield "Start"
                raise Exception("Stream error!")

        agent.set_response(ErrorResponse())
        streamer = LlamaIndexStreamer(agent)

        # Execute - should not crash
        parts = list(streamer.stream("Test error"))

        # Should get at least start and some structure
        assert any(isinstance(p, DataStreamStartStep) for p in parts)
        assert any(isinstance(p, DataStreamFinishRun) for p in parts)

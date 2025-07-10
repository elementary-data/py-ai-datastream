"""
Scenario-based tests for LlamaIndex streaming behavior.

These tests focus on real-world scenarios with minimal mocking,
showing how different model behaviors translate to streaming output.
"""

from typing import AsyncGenerator, Generator, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from ai_datastream.agent.llamaindex.streamer import LlamaIndexStreamer
from ai_datastream.messages import ChatMessage, MessageRole
from ai_datastream.stream_parts import (
    DataStreamFinishRun,
    DataStreamFinishStep,
    DataStreamStartStep,
    DataStreamText,
    DataStreamToolCall,
    DataStreamToolResult,
)


class MockStreamingResponse:
    """Mock streaming response that properly simulates LlamaIndex behavior."""

    def __init__(self, tokens: List[str], tool_calls: Optional[List[dict]] = None):
        self._tokens = tokens
        self._tool_calls = tool_calls or []
        self.metadata = {"tool_calls": self._tool_calls} if self._tool_calls else {}
        self.source_nodes = []

    @property
    def response_gen(self) -> Generator[str, None, None]:
        """Simulate the response generator."""
        for token in self._tokens:
            yield token

    async def async_response_gen(self) -> AsyncGenerator[str, None]:
        """Simulate async response generator."""
        for token in self._tokens:
            yield token


def create_mock_agent():
    """Create a mock agent that properly simulates stream_chat behavior."""
    agent = MagicMock()
    agent.memory = None

    # Store responses to return
    agent._responses = []
    agent._response_idx = 0

    def stream_chat(message: str):
        if agent._response_idx < len(agent._responses):
            response = agent._responses[agent._response_idx]
            agent._response_idx += 1
            return response
        return MockStreamingResponse([f"Echo: {message}"])

    async def astream_chat(message: str):
        return stream_chat(message)

    agent.stream_chat = stream_chat
    agent.astream_chat = astream_chat

    def add_response(response):
        agent._responses.append(response)
        agent._response_idx = 0  # Reset index

    agent.add_response = add_response
    return agent


class TestScenarios:
    """Scenario-based tests showing real behavior patterns."""

    @patch('ai_datastream.agent.llamaindex.streamer.isinstance')
    def test_reasoning_then_text_scenario(self, mock_isinstance):
        """Model reasons about a problem then provides an answer."""
        # Setup isinstance to return True for StreamingAgentChatResponse check
        mock_isinstance.side_effect = lambda obj, cls: (
            cls.__name__ == 'StreamingAgentChatResponse'
            if hasattr(cls, '__name__')
            else False
        )

        # Create agent with reasoning response
        agent = create_mock_agent()
        agent.add_response(MockStreamingResponse([
            "Let me think about this problem. ",
            "The square root of 16 is 4 because ",
            "4 × 4 = 16. ",
            "So the answer is 4."
        ]))

        streamer = LlamaIndexStreamer(agent)

        # Execute
        parts = list(streamer.stream("What is the square root of 16?"))

        # Verify structure
        assert isinstance(parts[0], DataStreamStartStep)

        # Collect all text
        text_parts = [p for p in parts if isinstance(p, DataStreamText)]
        full_text = ''.join(p.body for p in text_parts)

        assert "Let me think" in full_text
        assert "4 × 4 = 16" in full_text
        assert "answer is 4" in full_text

        # Verify proper ending
        assert isinstance(parts[-2], DataStreamFinishStep)
        assert isinstance(parts[-1], DataStreamFinishRun)

    @patch('ai_datastream.agent.llamaindex.streamer.isinstance')
    def test_text_then_tool_then_text_scenario(self, mock_isinstance):
        """Model explains, uses a tool, then summarizes results."""
        # Setup isinstance to return True for StreamingAgentChatResponse check
        mock_isinstance.side_effect = lambda obj, cls: (
            cls.__name__ == 'StreamingAgentChatResponse'
            if hasattr(cls, '__name__')
            else False
        )

        # Create response with text and tool usage
        agent = create_mock_agent()
        agent.add_response(MockStreamingResponse(
            tokens=["I'll calculate the sum of 2 + 2 for you."],
            tool_calls=[{
                "id": "calc_123",
                "name": "calculator",
                "arguments": {"expression": "2 + 2"},
                "result": "4"
            }]
        ))

        streamer = LlamaIndexStreamer(agent)

        # Execute
        parts = list(streamer.stream("Calculate 2 + 2"))

        # Verify sequence
        start_steps = [p for p in parts if isinstance(p, DataStreamStartStep)]
        text_parts = [p for p in parts if isinstance(p, DataStreamText)]
        tool_calls = [p for p in parts if isinstance(p, DataStreamToolCall)]
        tool_results = [p for p in parts if isinstance(p, DataStreamToolResult)]

        assert len(start_steps) == 1
        assert len(text_parts) >= 1
        assert len(tool_calls) == 1
        assert len(tool_results) == 1

        # Verify content
        assert "calculate the sum" in ''.join(p.body for p in text_parts)
        assert tool_calls[0].body["toolName"] == "calculator"
        assert tool_results[0].body["result"] == 4  # JSON parses "4" to integer

    @patch('ai_datastream.agent.llamaindex.streamer.isinstance')
    def test_multi_turn_conversation_scenario(self, mock_isinstance):
        """Test a realistic multi-turn conversation."""
        # Setup isinstance to return True for StreamingAgentChatResponse check
        mock_isinstance.side_effect = lambda obj, cls: (
            cls.__name__ == 'StreamingAgentChatResponse'
            if hasattr(cls, '__name__')
            else False
        )

        agent = create_mock_agent()
        agent.add_response(MockStreamingResponse([
            "Python is a high-level, ",
            "interpreted programming language ",
            "known for its simplicity."
        ]))

        streamer = LlamaIndexStreamer(agent)

        # First turn
        history = [
            ChatMessage(role=MessageRole.USER, content="What is Python?"),
            ChatMessage(
                role=MessageRole.ASSISTANT,
                content="Python is a programming language."
            ),
        ]

        # Execute with history
        parts = list(streamer.stream("Can you elaborate?", messages=history))

        # Verify response acknowledges context
        text_parts = [p for p in parts if isinstance(p, DataStreamText)]
        full_text = ''.join(p.body for p in text_parts)

        assert "high-level" in full_text
        assert "interpreted" in full_text
        assert "simplicity" in full_text

    @patch('ai_datastream.agent.llamaindex.streamer.isinstance')
    def test_parallel_tool_calls_scenario(self, mock_isinstance):
        """Model makes multiple tool calls in parallel."""
        # Setup isinstance to return True for StreamingAgentChatResponse check
        mock_isinstance.side_effect = lambda obj, cls: (
            cls.__name__ == 'StreamingAgentChatResponse'
            if hasattr(cls, '__name__')
            else False
        )

        agent = create_mock_agent()
        agent.add_response(MockStreamingResponse(
            tokens=["Fetching weather for both cities..."],
            tool_calls=[
                {
                    "id": "weather_1",
                    "name": "get_weather",
                    "arguments": {"city": "Tokyo"},
                    "result": "Sunny, 25°C"
                },
                {
                    "id": "weather_2",
                    "name": "get_weather",
                    "arguments": {"city": "London"},
                    "result": "Rainy, 15°C"
                }
            ]
        ))

        streamer = LlamaIndexStreamer(agent)

        # Execute
        parts = list(streamer.stream("Compare weather in Tokyo and London"))

        # Verify multiple tool calls
        tool_calls = [p for p in parts if isinstance(p, DataStreamToolCall)]
        tool_results = [p for p in parts if isinstance(p, DataStreamToolResult)]

        assert len(tool_calls) == 2
        assert len(tool_results) == 2

        # Verify each tool call
        assert tool_calls[0].body["toolName"] == "get_weather"
        assert tool_calls[0].body["args"]["city"] == "Tokyo"
        assert tool_calls[1].body["args"]["city"] == "London"

        assert "25°C" in tool_results[0].body["result"]
        assert "15°C" in tool_results[1].body["result"]

    @patch('ai_datastream.agent.llamaindex.streamer.isinstance')
    def test_error_recovery_scenario(self, mock_isinstance):
        """Model handles and recovers from errors gracefully."""
        # Setup isinstance to return True for StreamingAgentChatResponse check
        mock_isinstance.side_effect = lambda obj, cls: (
            cls.__name__ == 'StreamingAgentChatResponse'
            if hasattr(cls, '__name__')
            else False
        )

        agent = create_mock_agent()
        agent.add_response(MockStreamingResponse([
            "I encountered an issue, ",
            "but I can still help you. ",
            "Let me try a different approach."
        ]))

        streamer = LlamaIndexStreamer(agent)

        # Execute
        parts = list(streamer.stream("This might cause an error"))

        # Verify graceful handling
        text_parts = [p for p in parts if isinstance(p, DataStreamText)]
        full_text = ''.join(p.body for p in text_parts)

        assert "encountered an issue" in full_text
        assert "different approach" in full_text

        # Should still have proper structure
        assert isinstance(parts[0], DataStreamStartStep)
        assert isinstance(parts[-2], DataStreamFinishStep)
        assert isinstance(parts[-1], DataStreamFinishRun)

    @patch('ai_datastream.agent.llamaindex.streamer.isinstance')
    @pytest.mark.asyncio
    async def test_async_scenario(self, mock_isinstance):
        """Test async streaming behavior matches sync."""
        # Setup isinstance to return True for StreamingAgentChatResponse check
        mock_isinstance.side_effect = lambda obj, cls: (
            cls.__name__ == 'StreamingAgentChatResponse'
            if hasattr(cls, '__name__')
            else False
        )

        agent = create_mock_agent()
        agent.add_response(MockStreamingResponse([
            "Async response: ",
            "Hello from async!"
        ]))

        streamer = LlamaIndexStreamer(agent)

        # Execute async
        parts = []
        async for part in streamer.async_stream("Test async"):
            parts.append(part)

        # Verify same structure as sync
        assert isinstance(parts[0], DataStreamStartStep)
        text_parts = [p for p in parts if isinstance(p, DataStreamText)]
        assert (
            ''.join(p.body for p in text_parts) == "Async response: Hello from async!"
        )
        assert isinstance(parts[-2], DataStreamFinishStep)
        assert isinstance(parts[-1], DataStreamFinishRun)

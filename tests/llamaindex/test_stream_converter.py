"""
Tests for LlamaIndexStreamConverter focusing on conversion behavior.
"""

from unittest.mock import MagicMock

import pytest

from ai_datastream.agent.llamaindex.stream_converter import LlamaIndexStreamConverter
from ai_datastream.stream_parts import (
    DataStreamFinishStep,
    DataStreamStartStep,
    DataStreamText,
    DataStreamToolCall,
    DataStreamToolCallDelta,
    DataStreamToolCallStart,
    DataStreamToolResult,
)


def create_streaming_response(
    tokens: list[str] = None,
    tool_calls: list[dict] = None,
    source_node_tools: list[dict] = None
) -> MagicMock:
    """Create a realistic mock streaming response."""
    response = MagicMock()
    response.response_gen = iter(tokens or [])
    response.metadata = {"tool_calls": tool_calls} if tool_calls else {}

    # Add source nodes with tool metadata if provided
    response.source_nodes = []
    if source_node_tools:
        for tool in source_node_tools:
            node = MagicMock()
            node.metadata = {"tool_call": tool}
            response.source_nodes.append(node)

    return response


class TestStreamConverterBehavior:
    """Test how the converter transforms different LlamaIndex responses."""

    def test_converts_simple_text_streaming(self):
        """Should convert text tokens into stream parts."""
        # Given: A response that streams text tokens
        response = create_streaming_response(
            tokens=["Hello", " ", "world", "!"]
        )
        converter = LlamaIndexStreamConverter()

        # When: Converting the response
        parts = list(converter.convert_streaming_response(response))

        # Then: Should start with a message and stream text
        assert isinstance(parts[0], DataStreamStartStep)

        text_parts = [p for p in parts if isinstance(p, DataStreamText)]
        assert len(text_parts) == 4
        text = ''.join(p.body for p in text_parts)
        assert text == "Hello world!"

    def test_filters_empty_tokens(self):
        """Should filter out empty string tokens but keep spaces."""
        # Given: A response with mixed empty and valid tokens
        response = create_streaming_response(
            tokens=["", "Hello", "", " ", "", "world", ""]
        )
        converter = LlamaIndexStreamConverter()

        # When: Converting
        parts = list(converter.convert_streaming_response(response))

        # Then: Only non-empty tokens should be converted
        text_parts = [p for p in parts if isinstance(p, DataStreamText)]
        assert len(text_parts) == 3  # "Hello", " ", "world"
        text = ''.join(p.body for p in text_parts)
        assert text == "Hello world"

    def test_handles_tool_calls_from_metadata(self):
        """Should convert tool calls into proper stream format."""
        # Given: A response with tool calls in metadata
        response = create_streaming_response(
            tokens=["I'll help with that"],
            tool_calls=[{
                "id": "calc_123",
                "name": "calculator",
                "arguments": {"expression": "2 + 2"},
                "result": "4"
            }]
        )
        converter = LlamaIndexStreamConverter()

        # When: Converting
        parts = list(converter.convert_streaming_response(response))

        # Then: Should have tool call sequence
        tool_starts = [p for p in parts if isinstance(p, DataStreamToolCallStart)]
        tool_calls = [p for p in parts if isinstance(p, DataStreamToolCall)]
        tool_results = [p for p in parts if isinstance(p, DataStreamToolResult)]

        assert len(tool_starts) == 1
        assert tool_starts[0].body["toolName"] == "calculator"

        assert len(tool_calls) == 1
        assert tool_calls[0].body["args"]["expression"] == "2 + 2"

        assert len(tool_results) == 1
        assert tool_results[0].body["result"] == 4  # JSON parsed to integer

    def test_streams_tool_argument_deltas(self):
        """Should stream tool arguments progressively when available."""
        # Given: A tool call with streaming arguments text
        response = create_streaming_response(
            tokens=["Searching..."],
            tool_calls=[{
                "id": "search_1",
                "name": "web_search",
                "arguments": {"query": "Python tutorials"},
                "args_text": '{"query": "Python tutorials"}',  # Simulates streaming
                "result": "Found 10 results"
            }]
        )
        converter = LlamaIndexStreamConverter()

        # When: Converting
        parts = list(converter.convert_streaming_response(response))

        # Then: Should have progressive argument updates
        deltas = [p for p in parts if isinstance(p, DataStreamToolCallDelta)]
        assert len(deltas) > 0

        # Reconstruct the arguments from deltas
        reconstructed = ''.join(d.body["argsTextDelta"] for d in deltas)
        assert "Python tutorials" in reconstructed

    def test_handles_multiple_tools_in_sequence(self):
        """Should handle multiple tool calls maintaining order."""
        # Given: Multiple tool calls
        response = create_streaming_response(
            tokens=["Processing multiple requests"],
            tool_calls=[
                {
                    "id": "weather_1",
                    "name": "get_weather",
                    "arguments": {"city": "NYC"},
                    "result": "72°F, sunny"
                },
                {
                    "id": "news_1",
                    "name": "get_news",
                    "arguments": {"topic": "tech"},
                    "result": "5 new articles"
                }
            ]
        )
        converter = LlamaIndexStreamConverter()

        # When: Converting
        parts = list(converter.convert_streaming_response(response))

        # Then: Both tools should be processed in order
        tool_calls = [p for p in parts if isinstance(p, DataStreamToolCall)]
        assert len(tool_calls) == 2
        assert tool_calls[0].body["toolName"] == "get_weather"
        assert tool_calls[1].body["toolName"] == "get_news"

    def test_handles_tools_from_source_nodes(self):
        """Should extract tool calls from source nodes."""
        # Given: Tool calls in source nodes (LlamaIndex pattern)
        response = create_streaming_response(
            tokens=["Processing"],
            source_node_tools=[{
                "id": "tool_from_node",
                "name": "node_tool",
                "arguments": {"param": "value"}
            }]
        )
        converter = LlamaIndexStreamConverter()

        # When: Converting
        parts = list(converter.convert_streaming_response(response))

        # Then: Tool from source node should be processed
        tool_calls = [p for p in parts if isinstance(p, DataStreamToolCall)]
        assert len(tool_calls) == 1
        assert tool_calls[0].body["toolName"] == "node_tool"

    def test_maintains_unique_message_ids(self):
        """Should generate and maintain consistent message IDs."""
        converter = LlamaIndexStreamConverter()

        # First conversion
        response1 = create_streaming_response(["First"])
        parts1 = list(converter.convert_streaming_response(response1))
        message_id1 = parts1[0].body["messageId"]

        # Second conversion with same converter
        response2 = create_streaming_response(["Second"])
        parts2 = list(converter.convert_streaming_response(response2))
        message_id2 = parts2[0].body["messageId"]

        # Should reuse same ID until reset
        assert message_id1 == message_id2

        # After reset
        converter.reset_message_id()
        response3 = create_streaming_response(["Third"])
        parts3 = list(converter.convert_streaming_response(response3))
        message_id3 = parts3[0].body["messageId"]

        # Should have new ID
        assert message_id3 != message_id1

    def test_handles_errors_gracefully(self):
        """Should handle streaming errors without breaking."""
        def error_generator():
            yield "Before error"
            raise ValueError("Streaming failed!")

        response = MagicMock()
        response.response_gen = error_generator()
        response.source_nodes = []
        response.metadata = {}

        converter = LlamaIndexStreamConverter()

        # Should not raise, but include error info
        parts = list(converter.convert_streaming_response(response))

        # Should have processed tokens before error
        text_parts = [p for p in parts if isinstance(p, DataStreamText)]
        assert any("Before error" in p.body for p in text_parts)

        # Should include error as comment
        assert any("error" in p.body.lower() for p in text_parts)

    def test_handles_structured_tokens(self):
        """Should handle tokens with delta attribute."""
        # Create tokens with delta attribute
        structured_token = MagicMock()
        structured_token.delta = "Structured content"

        response = create_streaming_response()
        response.response_gen = iter(["Plain text", structured_token])

        converter = LlamaIndexStreamConverter()
        parts = list(converter.convert_streaming_response(response))

        text_parts = [p for p in parts if isinstance(p, DataStreamText)]
        assert len(text_parts) == 2
        assert text_parts[0].body == "Plain text"
        assert text_parts[1].body == "Structured content"

    @pytest.mark.asyncio
    async def test_async_conversion_works_identically(self):
        """Async conversion should produce same results as sync."""
        response = MagicMock()

        # Create async generator
        async def async_gen():
            for token in ["Async", " ", "test"]:
                yield token

        response.async_response_gen = async_gen
        response.source_nodes = []
        response.metadata = {}

        converter = LlamaIndexStreamConverter()

        # Convert async
        parts = []
        async for part in converter.async_convert_streaming_response(response):
            parts.append(part)

        # Should produce same structure
        assert isinstance(parts[0], DataStreamStartStep)

        text_parts = [p for p in parts if isinstance(p, DataStreamText)]
        text = ''.join(p.body for p in text_parts)
        assert text == "Async test"

    def test_never_emits_finish_steps(self):
        """Converter should never emit finish steps (that's the streamer's job)."""
        # Try various response types
        responses = [
            create_streaming_response(["Simple text"]),
            create_streaming_response(
                ["With tools"],
                tool_calls=[{"id": "1", "name": "tool", "arguments": {}}]
            ),
            create_streaming_response([])  # Empty
        ]

        converter = LlamaIndexStreamConverter()

        for response in responses:
            parts = list(converter.convert_streaming_response(response))
            finish_steps = [p for p in parts if isinstance(p, DataStreamFinishStep)]
            assert len(finish_steps) == 0, (
                "Converter must not emit finish steps"
            )

"""
Generate mock streaming data for LlamaIndex tests.
This script creates mock data similar to the langchain version.
"""

from typing import List
from unittest.mock import MagicMock

from ai_datastream.stream_parts import (
    DataStreamFinishRun,
    DataStreamFinishStep,
    DataStreamFinishStepReason,
    DataStreamStartStep,
    DataStreamText,
    DataStreamToolCall,
    DataStreamToolResult,
)


def create_mock_streaming_response(tokens: List[str]) -> MagicMock:
    """Create a mock LlamaIndex streaming response."""
    mock_response = MagicMock()
    mock_response.response_gen = iter(tokens)
    mock_response.source_nodes = []
    mock_response.metadata = {}
    return mock_response


def create_mock_streaming_response_with_tools(
    tokens: List[str],
    tool_calls: List[dict] = None,
    tool_results: List[dict] = None,
) -> MagicMock:
    """Create a mock streaming response with tool calls."""
    mock_response = MagicMock()
    mock_response.response_gen = iter(tokens)

    # Mock source nodes with tool metadata
    mock_source_nodes = []
    if tool_calls or tool_results:
        for _i, (call, result) in enumerate(zip(tool_calls or [], tool_results or [])):
            mock_node = MagicMock()
            mock_node.metadata = {
                "tool_call": call,
                "tool_result": result["result"] if result else None,
            }
            mock_source_nodes.append(mock_node)

    mock_response.source_nodes = mock_source_nodes
    mock_response.metadata = {}
    return mock_response


# Mock streaming tokens for a simple conversation
MOCK_TOKENS_SIMPLE = [
    "Hello",
    "!",
    " How",
    " can",
    " I",
    " help",
    " you",
    " today",
    "?",
]

# Expected data stream parts for simple conversation
MOCK_DATA_STREAM_SIMPLE = [
    DataStreamStartStep("mock-message-id-1"),
    DataStreamText("Hello"),
    DataStreamText("!"),
    DataStreamText(" How"),
    DataStreamText(" can"),
    DataStreamText(" I"),
    DataStreamText(" help"),
    DataStreamText(" you"),
    DataStreamText(" today"),
    DataStreamText("?"),
    DataStreamFinishStep(DataStreamFinishStepReason.STOP),
    DataStreamFinishRun(),
]

# Mock streaming tokens for a conversation with tool calls
MOCK_TOKENS_WITH_TOOLS = [
    "I'll",
    " help",
    " you",
    " with",
    " the",
    " weather",
    ".",
]

# Mock tool calls and results
MOCK_TOOL_CALLS = [
    {
        "id": "call_weather_123",
        "name": "get_weather",
        "arguments": {"city": "San Francisco"},
    }
]

MOCK_TOOL_RESULTS = [
    {
        "tool_call_id": "call_weather_123",
        "result": "The weather in San Francisco is sunny and 72°F",
    }
]

# Expected data stream parts for conversation with tools
MOCK_DATA_STREAM_WITH_TOOLS = [
    DataStreamStartStep("mock-message-id-2"),
    DataStreamText("I'll"),
    DataStreamText(" help"),
    DataStreamText(" you"),
    DataStreamText(" with"),
    DataStreamText(" the"),
    DataStreamText(" weather"),
    DataStreamText("."),
    DataStreamToolCall(
        tool_call_id="call_weather_123",
        tool_name="get_weather",
        args={"city": "San Francisco"},
    ),
    DataStreamToolResult(
        tool_call_id="call_weather_123",
        result="The weather in San Francisco is sunny and 72°F",
    ),
    DataStreamFinishStep(DataStreamFinishStepReason.TOOL_CALLS),
    DataStreamFinishRun(),
]


class MockLlamaIndexAgent:
    """Mock LlamaIndex agent for testing."""

    def __init__(self, response_tokens: List[str] = None):
        self.response_tokens = response_tokens or MOCK_TOKENS_SIMPLE
        self.memory = MagicMock()

    def stream_chat(self, message: str) -> MagicMock:
        return create_mock_streaming_response(self.response_tokens)

    async def astream_chat(self, message: str) -> MagicMock:
        return self.stream_chat(message)


class MockLlamaIndexAgentWithTools:
    """Mock LlamaIndex agent that supports tool calls."""

    def __init__(self):
        self.memory = MagicMock()

    def stream_chat(self, message: str) -> MagicMock:
        return create_mock_streaming_response_with_tools(
            MOCK_TOKENS_WITH_TOOLS,
            MOCK_TOOL_CALLS,
            MOCK_TOOL_RESULTS,
        )

    async def astream_chat(self, message: str) -> MagicMock:
        return self.stream_chat(message)


if __name__ == "__main__":
    # Example usage
    agent = MockLlamaIndexAgent()
    response = agent.stream_chat("Hello!")

    print("Mock tokens:", list(response.response_gen))
    print("Expected stream parts:", len(MOCK_DATA_STREAM_SIMPLE))

    agent_with_tools = MockLlamaIndexAgentWithTools()
    response_with_tools = agent_with_tools.stream_chat("What's the weather?")

    print("Mock tokens with tools:", list(response_with_tools.response_gen))
    print("Expected stream parts with tools:", len(MOCK_DATA_STREAM_WITH_TOOLS))

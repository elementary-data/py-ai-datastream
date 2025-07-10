import pytest

from ai_datastream.agent.llamaindex.message_parser import LlamaIndexMessageParser
from ai_datastream.messages import ChatMessage, MessageRole, ToolInvocation


@pytest.fixture
def parser() -> LlamaIndexMessageParser:
    return LlamaIndexMessageParser()


def test_parse_user_message(parser: LlamaIndexMessageParser) -> None:
    message = ChatMessage(role=MessageRole.USER, content="Hello, how are you?")

    result = parser.parse(message)
    assert result is not None
    assert result.role.value == "user"
    assert result.content == "Hello, how are you?"


def test_parse_user_message_without_content(parser: LlamaIndexMessageParser) -> None:
    message = ChatMessage(role=MessageRole.USER, content="")

    result = parser.parse(message)
    assert result is None


def test_parse_assistant_message_without_tools(parser: LlamaIndexMessageParser) -> None:
    message = ChatMessage(
        role=MessageRole.ASSISTANT, content="I'm doing well!", tool_invocations=[]
    )

    result = parser.parse(message)
    assert result is not None
    assert result.role.value == "assistant"
    assert result.content == "I'm doing well!"


def test_parse_assistant_message_with_tools(parser: LlamaIndexMessageParser) -> None:
    message = ChatMessage(
        role=MessageRole.ASSISTANT,
        content="Let me help you with that.",
        tool_invocations=[
            ToolInvocation(
                tool_call_id="call_1",
                tool_name="search",
                args={"query": "test"},
                result="Search results",
            )
        ],
    )

    # Test parsing the main message
    result = parser.parse(message)
    assert result is not None
    assert result.role.value == "assistant"
    assert result.content == "Let me help you with that."

    # Test parsing tool messages separately
    tool_messages = parser._parse_tool_messages(message.tool_invocations)
    assert len(tool_messages) == 2  # call + result
    assert tool_messages[0].role.value == "tool"
    assert "Tool call: search" in tool_messages[0].content
    assert tool_messages[1].role.value == "tool"
    assert "Tool result: Search results" in tool_messages[1].content


def test_parse_assistant_message_multiple_tools(
    parser: LlamaIndexMessageParser,
) -> None:
    message = ChatMessage(
        role=MessageRole.ASSISTANT,
        content="Processing multiple tools",
        tool_invocations=[
            ToolInvocation(
                tool_call_id="call_1",
                tool_name="tool1",
                args={"arg1": "value1"},
                result="Result 1",
            ),
            ToolInvocation(
                tool_call_id="call_2",
                tool_name="tool2",
                args={"arg2": "value2"},
                result="Result 2",
            ),
        ],
    )

    # Test main message
    result = parser.parse(message)
    assert result is not None
    assert result.content == "Processing multiple tools"

    # Test tool messages
    tool_messages = parser._parse_tool_messages(message.tool_invocations)
    assert len(tool_messages) == 4  # 2 calls + 2 results


def test_parse_assistant_message_without_content(
    parser: LlamaIndexMessageParser,
) -> None:
    message = ChatMessage(role=MessageRole.ASSISTANT, content="", tool_invocations=[])

    result = parser.parse(message)
    assert result is None


def test_parse_assistant_message_only_tools(parser: LlamaIndexMessageParser) -> None:
    message = ChatMessage(
        role=MessageRole.ASSISTANT,
        content="",
        tool_invocations=[
            ToolInvocation(
                tool_call_id="call_1",
                tool_name="search",
                args={"query": "test"},
                result="Search results",
            )
        ],
    )

    # Main message should return a message with empty content when there are
    # tool invocations
    result = parser.parse(message)
    assert result is not None
    assert result.role.value == "assistant"
    # LlamaIndex converts empty strings to None
    assert result.content in ("", None)

    # Tool messages should still be parseable
    tool_messages = parser._parse_tool_messages(message.tool_invocations)
    assert len(tool_messages) == 2


def test_parse_many_messages(parser: LlamaIndexMessageParser) -> None:
    messages = [
        ChatMessage(role=MessageRole.USER, content="Hello!"),
        ChatMessage(
            role=MessageRole.ASSISTANT,
            content="Hi there!",
            tool_invocations=[
                ToolInvocation(
                    tool_call_id="call_1",
                    tool_name="greet",
                    args={},
                    result="Greeting completed",
                )
            ],
        ),
        ChatMessage(role=MessageRole.USER, content="How are you?"),
    ]

    results = parser.parse_many(messages)
    # Should have: user message + assistant message + 2 tool messages + user message = 5
    assert len(results) == 5

    # Check first user message
    assert results[0].role.value == "user"
    assert results[0].content == "Hello!"

    # Check assistant message
    assert results[1].role.value == "assistant"
    assert results[1].content == "Hi there!"

    # Check tool messages
    assert results[2].role.value == "tool"
    assert "Tool call: greet" in results[2].content
    assert results[3].role.value == "tool"
    assert "Tool result: Greeting completed" in results[3].content

    # Check second user message
    assert results[4].role.value == "user"
    assert results[4].content == "How are you?"


def test_parse_other_role_fallback(parser: LlamaIndexMessageParser) -> None:
    """Test that unknown roles are handled as user messages."""
    message = ChatMessage(role=MessageRole.USER, content="Test")
    # Simulate unknown role
    message.role = "unknown_role"  # type: ignore[assignment]

    result = parser.parse(message)
    assert result is not None
    assert result.role.value == "user"  # Falls back to user
    assert result.content == "Test"


def test_parse_tool_invocations_without_result(parser: LlamaIndexMessageParser) -> None:
    """Test parsing tool invocations without results."""
    tool_invocations = [
        ToolInvocation(
            tool_call_id="call_1",
            tool_name="test_tool",
            args={"param": "value"},
            result=None,  # No result
        )
    ]

    tool_messages = parser._parse_tool_messages(tool_invocations)
    assert len(tool_messages) == 1  # Only tool call, no result
    assert tool_messages[0].role.value == "tool"
    assert "Tool call: test_tool" in tool_messages[0].content

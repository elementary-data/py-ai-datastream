import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ai_datastream.agent.langgraph.message_parser import LanggraphMessageParser
from ai_datastream.messages import ChatMessage, MessageRole, ToolInvocation


@pytest.fixture
def parser() -> LanggraphMessageParser:
    return LanggraphMessageParser()


def test_parse_user_message(parser: LanggraphMessageParser) -> None:
    message = ChatMessage(role=MessageRole.USER, content="Hello, how are you?")

    messages = list(parser.parse(message))
    assert len(messages) == 1
    assert isinstance(messages[0], HumanMessage)
    assert messages[0].content == "Hello, how are you?"


def test_parse_ai_message_without_tools(parser: LanggraphMessageParser) -> None:
    message = ChatMessage(
        role=MessageRole.ASSISTANT, content="I'm doing well!", tool_invocations=[]
    )

    messages = list(parser.parse(message))
    assert len(messages) == 1
    assert isinstance(messages[0], AIMessage)
    assert messages[0].content == "I'm doing well!"
    assert messages[0].tool_calls == []


def test_parse_ai_message_with_tools(parser: LanggraphMessageParser) -> None:
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

    messages = list(parser.parse(message))
    assert len(messages) == 2

    # Check AI message
    assert isinstance(messages[0], AIMessage)
    assert messages[0].content == "Let me help you with that."
    assert len(messages[0].tool_calls) == 1
    tool_call = messages[0].tool_calls[0]
    assert tool_call["id"] == "call_1"
    assert tool_call["name"] == "search"
    assert tool_call["args"] == {"query": "test"}

    # Check tool message
    assert isinstance(messages[1], ToolMessage)
    assert messages[1].content == "Search results"
    assert messages[1].tool_call_id == "call_1"


def test_parse_ai_message_multiple_tools(parser: LanggraphMessageParser) -> None:
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

    messages = list(parser.parse(message))
    assert len(messages) == 3  # 1 AI message + 2 tool messages

    assert isinstance(messages[0], AIMessage)
    assert len(messages[0].tool_calls) == 2

    assert isinstance(messages[1], ToolMessage)
    assert messages[1].tool_call_id == "call_1"
    assert messages[1].content == "Result 1"

    assert isinstance(messages[2], ToolMessage)
    assert messages[2].tool_call_id == "call_2"
    assert messages[2].content == "Result 2"


def test_parse_user_message_without_content(parser: LanggraphMessageParser) -> None:
    message = ChatMessage(role=MessageRole.USER, content="")

    messages = list(parser.parse(message))
    assert len(messages) == 0


def test_parse_ai_message_without_content(parser: LanggraphMessageParser) -> None:
    message = ChatMessage(role=MessageRole.ASSISTANT, content="", tool_invocations=[])

    messages = list(parser.parse(message))
    assert len(messages) == 0

from dataclasses import dataclass
from enum import Enum
from typing import List, Union

DEFAULT_TOOL_RESULT = """
Tool call does not contain a result.
This might indicate that the user has interrupted the conversation.
"""


class MessageRole(str, Enum):
    USER = "user"
    ASSISTANT = "assistant"


@dataclass
class ToolInvocation:
    tool_call_id: str
    tool_name: str
    args: dict
    result: str

    @classmethod
    def from_dict(cls, data: dict) -> "ToolInvocation":
        return cls(
            tool_call_id=data["toolCallId"],
            tool_name=data["toolName"],
            args=data["args"],
            result=data.get("result") or DEFAULT_TOOL_RESULT,
        )


@dataclass
class ChatMessage:
    role: MessageRole
    content: Union[str, None] = None
    tool_invocations: Union[List[ToolInvocation], None] = None

    @classmethod
    def from_dict(cls, data: dict) -> "ChatMessage":
        return cls(
            role=MessageRole(data["role"]),
            content=data["content"],
            tool_invocations=[
                ToolInvocation.from_dict(tool_invocation)
                for tool_invocation in data.get("toolInvocations", [])
            ],
        )

"""
Implements vercel ai data stream protocol.
Documentation: https://sdk.vercel.ai/docs/ai-sdk-ui/stream-protocol#data-stream-protocol
NOTE - Not all part types are implemented, for full list of available part types see the documentation.
"""  # noqa: E501

import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Union


class DataStreamType:
    START_STEP = "f"
    FINISH_STEP = "e"
    FINISH_RUN = "d"
    TOOL_CALL_START = "b"
    TOOL_CALL = "9"
    TOOL_RESULT = "a"
    TEXT = "0"


@dataclass
class DataStreamPart:
    type: str
    body: Any

    def format(self) -> str:
        return f"{self.type}:{json.dumps(self.body)}\n"


class DataStreamStartStep(DataStreamPart):
    def __init__(self, message_id: str):
        super().__init__(DataStreamType.START_STEP, {"messageId": message_id})


class DataStreamFinishStepReason(Enum):
    STOP = "stop"
    TOOL_CALLS = "tool-calls"


class DataStreamFinishStep(DataStreamPart):
    def __init__(
        self,
        finish_reason: DataStreamFinishStepReason,
        usage: Union[dict, None] = None,
        is_continued: bool = False,
    ):
        super().__init__(
            DataStreamType.FINISH_STEP,
            {
                "finishReason": finish_reason.value,
                "usage": usage or {"promptTokens": None, "completionTokens": None},
                "isContinued": is_continued,
            },
        )


class DataStreamFinishRun(DataStreamPart):
    def __init__(self, usage: Union[dict, None] = None):
        super().__init__(
            DataStreamType.FINISH_RUN,
            {
                "finishReason": "stop",
                "usage": usage or {"promptTokens": None, "completionTokens": None},
            },
        )


class DataStreamToolCallStart(DataStreamPart):
    def __init__(self, tool_call_id: str, tool_name: str):
        super().__init__(
            DataStreamType.TOOL_CALL_START,
            {"toolCallId": tool_call_id, "toolName": tool_name},
        )


class DataStreamToolCall(DataStreamPart):
    def __init__(self, tool_call_id: str, tool_name: str, args: Any):
        super().__init__(
            DataStreamType.TOOL_CALL,
            {"toolCallId": tool_call_id, "toolName": tool_name, "args": args},
        )


class DataStreamToolResult(DataStreamPart):
    def __init__(self, tool_call_id: str, result: Any):
        try:
            result = json.loads(result)
        except json.JSONDecodeError:
            pass
        super().__init__(
            DataStreamType.TOOL_RESULT,
            {
                "toolCallId": tool_call_id,
                "result": result,
            },
        )


class DataStreamText(DataStreamPart):
    def __init__(self, text: str):
        super().__init__(DataStreamType.TEXT, text)

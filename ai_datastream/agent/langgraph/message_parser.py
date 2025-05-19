from typing import Iterable, Iterator, Union

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    ToolCall,
    ToolMessage,
)

from ai_datastream.messages import ChatMessage, MessageRole, ToolInvocation


class LanggraphMessageParser:
    def _parse_user_message(self, message: ChatMessage) -> Union[HumanMessage, None]:
        if not message.content:
            return None
        return HumanMessage(content=message.content)

    def _parse_tool_call(self, tool_invocation: ToolInvocation) -> ToolCall:
        return ToolCall(
            id=tool_invocation.tool_call_id,
            name=tool_invocation.tool_name,
            args=tool_invocation.args,
        )

    def _parse_ai_message(self, message: ChatMessage) -> Union[AIMessage, None]:
        tool_calls = []
        if message.tool_invocations:
            tool_calls = [
                self._parse_tool_call(tool_invocation)
                for tool_invocation in message.tool_invocations
            ]
        if not tool_calls and not message.content:
            return None
        return AIMessage(
            content=message.content or "",
            tool_calls=tool_calls,
        )

    def _parse_tool_message(self, tool_invocation: ToolInvocation) -> ToolMessage:
        return ToolMessage(
            content=tool_invocation.result,
            tool_call_id=tool_invocation.tool_call_id,
        )

    def parse(self, message: ChatMessage) -> Iterator[BaseMessage]:
        if message.role == MessageRole.USER:
            user_message = self._parse_user_message(message)
            if user_message:
                yield user_message
        elif message.role == MessageRole.ASSISTANT:
            ai_message = self._parse_ai_message(message)
            if ai_message:
                yield ai_message
            if message.tool_invocations:
                for tool_invocation in message.tool_invocations:
                    yield self._parse_tool_message(tool_invocation)
        else:
            raise ValueError(f"Invalid message role: {message.role}")

    def parse_many(self, messages: Iterable[ChatMessage]) -> Iterator[BaseMessage]:
        for message in messages:
            yield from self.parse(message)

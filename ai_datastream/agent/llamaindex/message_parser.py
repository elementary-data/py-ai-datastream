from typing import List, Union

from llama_index.core.base.llms.types import ChatMessage as LlamaChatMessage
from llama_index.core.base.llms.types import MessageRole as LlamaMessageRole

from ai_datastream.messages import ChatMessage, MessageRole, ToolInvocation


class LlamaIndexMessageParser:
    """Converts ChatMessage objects to LlamaIndex ChatMessage format."""

    def _parse_user_message(
        self, message: ChatMessage
    ) -> Union[LlamaChatMessage, None]:
        """Convert user message to LlamaIndex format."""
        if not message.content:
            return None

        return LlamaChatMessage(
            role=LlamaMessageRole.USER,
            content=message.content,
        )

    def _parse_assistant_message(
        self, message: ChatMessage
    ) -> Union[LlamaChatMessage, None]:
        """Convert assistant message to LlamaIndex format."""
        if not message.content and not message.tool_invocations:
            return None

        # For now, just use the content. Tool invocations could be handled
        # separately or combined into the message content
        content = message.content or ""

        return LlamaChatMessage(
            role=LlamaMessageRole.ASSISTANT,
            content=content,
        )

    def _parse_tool_messages(
        self, tool_invocations: List[ToolInvocation]
    ) -> List[LlamaChatMessage]:
        """Convert tool invocations to LlamaIndex tool messages."""
        messages = []

        for tool_invocation in tool_invocations:
            # Add tool call message
            tool_call_content = (
                f"Tool call: {tool_invocation.tool_name}({tool_invocation.args})"
            )
            messages.append(
                LlamaChatMessage(
                    role=LlamaMessageRole.TOOL,
                    content=tool_call_content,
                )
            )

            # Add tool result message if available
            if tool_invocation.result:
                tool_result_content = f"Tool result: {tool_invocation.result}"
                messages.append(
                    LlamaChatMessage(
                        role=LlamaMessageRole.TOOL,
                        content=tool_result_content,
                    )
                )

        return messages

    def parse(self, message: ChatMessage) -> Union[LlamaChatMessage, None]:
        """Convert a single ChatMessage to LlamaIndex format."""
        if message.role == MessageRole.USER:
            return self._parse_user_message(message)
        elif message.role == MessageRole.ASSISTANT:
            return self._parse_assistant_message(message)
        else:
            # Handle other message types as user messages by default
            return LlamaChatMessage(
                role=LlamaMessageRole.USER,
                content=message.content or "",
            )

    def parse_many(self, messages: List[ChatMessage]) -> List[LlamaChatMessage]:
        """Convert multiple ChatMessages to LlamaIndex format."""
        result = []

        for message in messages:
            # Parse the main message
            parsed_message = self.parse(message)
            if parsed_message:
                result.append(parsed_message)

            # Handle tool invocations if present
            if message.tool_invocations:
                tool_messages = self._parse_tool_messages(message.tool_invocations)
                result.extend(tool_messages)

        return result

from typing import Annotated

from pydantic import BaseModel, BeforeValidator

from ai_datastream.messages import ChatMessage

FastApiChatMessage = Annotated[ChatMessage, BeforeValidator(ChatMessage.from_dict)]


class FastApiDataStreamRequest(BaseModel):
    messages: list[FastApiChatMessage]

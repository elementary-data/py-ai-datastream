import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ai_datastream.api.fastapi import (
    AiChatDataStreamAsyncResponse,
    FastApiDataStreamRequest,
)
from ai_datastream.api.fastapi.response import AiChatDataStreamSyncResponse
from ai_datastream.stream_parts import (
    DataStreamFinishRun,
    DataStreamFinishStep,
    DataStreamFinishStepReason,
    DataStreamStartStep,
    DataStreamText,
)
from tests.mock_streamer import MockStreamer

app = FastAPI()

STREAMER = MockStreamer(
    [
        DataStreamStartStep(message_id="1"),
        DataStreamText(text="Hello, world!"),
        DataStreamFinishStep(
            finish_reason=DataStreamFinishStepReason.STOP,
        ),
        DataStreamFinishRun(),
    ]
)


@app.post("/ai/chat-async")
async def chat_async(
    request: FastApiDataStreamRequest,
) -> AiChatDataStreamAsyncResponse:
    return AiChatDataStreamAsyncResponse(
        streamer=STREAMER,
        prompt="You are a helpful assistant.",
        messages=request.messages,
    )


@app.post("/ai/chat-sync")
def chat_sync(request: FastApiDataStreamRequest) -> AiChatDataStreamSyncResponse:
    return AiChatDataStreamSyncResponse(
        streamer=STREAMER,
        prompt="You are a helpful assistant.",
        messages=request.messages,
    )


@pytest.mark.parametrize("endpoint", ["/ai/chat-async", "/ai/chat-sync"])
def test_ai_chat_data_stream_endpoint(endpoint: str) -> None:
    client = TestClient(app)
    response = client.post(
        endpoint,
        json={"messages": [{"role": "user", "content": "Hello, world!"}]},
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/event-stream"
    assert response.headers["transfer-encoding"] == "chunked"
    assert response.headers["x-vercel-ai-data-stream"] == "v1"
    for line, expected_part in zip(response.iter_lines(), STREAMER.stream_parts):
        assert line + "\n" == expected_part.format()

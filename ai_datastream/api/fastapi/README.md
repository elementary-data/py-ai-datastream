# FastAPI Framework

This module provides integration between FastAPI and the Vercel AI Data Stream protocol, allowing you to create streaming AI chat endpoints.

## Installation

To use the FastAPI framework, install it with pip:

```bash
pip install ai-datastream[fastapi]
```

## Usage

Here's how to use the FastAPI framework with a LangGraph agent:

```python
from fastapi import FastAPI
from ai_datastream.api.fastapi import AiChatDataStreamAsyncResponse, FastApiDataStreamRequest
from ai_datastream.streamer import AsyncStreamer

app = FastAPI()

@app.post("/ai/chat")
async def chat(request: FastApiDataStreamRequest):
    # Initialize a streamer for your framework
    streamer: AsyncStreamer = ...

    prompt = "You are a helpful assistant."

    # Return the streaming response
    return AiChatDataStreamAsyncResponse(streamer, prompt, request.messages)
```

## Features

- Supports both synchronous and asynchronous streaming responses
- Handles the Vercel AI Data Stream protocol format
- Provides request and response types for FastAPI integration
- Works with any agent framework that implements the Streamer interface

## Requirements

- Python 3.9+
- FastAPI 0.113.0 or higher
- Any agent framework (e.g., LangGraph)

## Documentation

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Vercel AI Data Stream Protocol](https://sdk.vercel.ai/docs/ai-sdk-ui/stream-protocol#data-stream-protocol)
- [LangGraph Framework Documentation](../agent/langgraph/README.md)

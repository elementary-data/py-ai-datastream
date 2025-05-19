# AI Data Stream

This package contains an implementation of the Vercel AI SDK's "Data Stream Protocol".

*Note: This is not an official package by Vercel, but rather a community-driven implementation of the protocol for use with python frameworks, it is not affiliated nor endorsed by Vercel. All credit for the protocol design and specification belongs to Vercel. This implementation exists solely to empower developers with additional framework options, with no intention of competing with Vercel's offerings.*

## Data Stream Protocol

The Data Stream Protocol is a protocol for streaming ai chat response to the client.
The protocol is described in the [Vercel AI SDK](https://sdk.vercel.ai/docs/ai-sdk-ui/stream-protocol#data-stream-protocol) documentation.

The basic idea is that the server sends a stream of messages to the client, in the following format:

```
{type}:{data}
```

- `type` is a single character that describes the type of the message.
- `data` is a json-serialized object that contains the message data.

An example of a full stream can look something like this:

```
f:{"messageId": "123"}
0:"Hello, i am a help"
0:"full Assistant that can ch"
0:"eck the weather, let me do that"
9:{"toolCallId": "tool-123", "name": "get_weather", "args": {"location": "San Francisco"}}
a:{"toolCallId": "tool-123", "result": "The weather in San Francisco is sunny"}
e:{"finishReason": "tool-calls", "usage": {"promptTokens": 10, "completionTokens": 20}, "isContinued": false}
f:{"messageId": "456"}
0:"The weather in San Francisco is sunny"
e:{"finishReason": "stop", "usage": {"promptTokens": 10, "completionTokens": 20}, "isContinued": false}
d:{"finishReason": "stop", "usage": {"promptTokens": 10, "completionTokens": 20}}
```

## Installation

```bash
pip install ai-datastream
```

## Usage

Example usage of usage with FastAPI and LangGraph:

```python
from fastapi import FastAPI
from langgraph.graph import StateGraph
from langgraph.prebuilt import create_react_agent
from langchain_openai import ChatOpenAI

from ai_datastream.api.fastapi import AiChatDataStreamAsyncResponse, FastApiDataStreamRequest
from ai_datastream.agent.langgraph import LanggraphStreamer


app = FastAPI()

@app.post("/ai/chat")
async def chat(request: FastApiDataStreamRequest):
    model = ChatOpenAI(model="gpt-4o")
    tools = [...]  # your tools
    prompt = "You are a helpful assistant."
    agent = create_react_agent(model, tools)
    streamer = LanggraphStreamer(agent)
    return AiChatDataStreamAsyncResponse(streamer, prompt, request.messages)
```

Then, in your frontend, you can use the `useChat` hook with the `/ai/chat` endpoint:

```ts
const { messages, input, handleInputChange, handleSubmit, isLoading } = useChat({
  api: "http://your-api-server/ai/chat",
});
```

# Supported Frameworks

## Agent Frameworks

- [LangGraph](/ai_datastream/agent/langgraph)

## API Frameworks

- [FastAPI](/ai_datastream/api/fastapi)

# Contributing

Contributions are welcome! Please see the [CONTRIBUTING.md](CONTRIBUTING.md) file for details.

# Development

## Setup

1. Install Poetry (version 2.1.2):
```bash
curl -sSL https://install.python-poetry.org | python3 - --version 2.1.2
```

2. Install dependencies:
```bash
poetry install --with dev
```

3. Install pre-commit hooks:
```bash
poetry run pre-commit install
```

## Pre-commit Hooks

This project uses pre-commit hooks to ensure code quality. The hooks run automatically on each commit and include:

- Code formatting and linting (ruff)
- Type checking (mypy)
- Basic file checks (trailing whitespace, file endings, etc.)

To run the hooks manually:
```bash
poetry run pre-commit run --all-files
```

## Testing

Run the test suite:
```bash
poetry run pytest
```

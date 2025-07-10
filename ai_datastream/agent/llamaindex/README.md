# LlamaIndex Agent Datastream

This module provides streaming datastream support for LlamaIndex agents, compatible with Vercel's AI SDK Data Stream Protocol.

## Overview

The `LlamaIndexStreamer` enables real-time streaming of LlamaIndex agent responses in a format compatible with frontend frameworks using the Vercel AI SDK. It supports both synchronous and asynchronous streaming patterns.

## Features

- **Universal Agent Support**: Works with `AgentRunner` and `BaseAgentWorker` instances
- **Streaming Protocol Compliance**: Outputs data stream parts compatible with Vercel AI SDK
- **Memory Management**: Automatically handles conversation history through LlamaIndex memory buffers
- **Tool Call Support**: Properly streams tool calls and results
- **Async/Sync Support**: Provides both synchronous and asynchronous streaming interfaces

## Basic Usage

```python
from ai_datastream.agent.llamaindex import LlamaIndexStreamer
from llama_index.core.agent import ReActAgent
from llama_index.core.tools import FunctionTool

# Define tools
def multiply(a: int, b: int) -> int:
    """Multiply two integers."""
    return a * b

multiply_tool = FunctionTool.from_defaults(fn=multiply)

# Create LlamaIndex agent
agent = ReActAgent.from_tools([multiply_tool], verbose=True)

# Create streamer
streamer = LlamaIndexStreamer(agent)

# Stream responses
for stream_part in streamer.stream("What is 5 times 7?"):
    print(stream_part.format(), end="")
```

## FastAPI Integration

```python
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from ai_datastream.api.fastapi import convert_to_response
from ai_datastream.agent.llamaindex import LlamaIndexStreamer

app = FastAPI()

@app.post("/chat")
async def chat_endpoint(request: ChatRequest):
    # Initialize your LlamaIndex agent
    agent = create_your_agent()
    streamer = LlamaIndexStreamer(agent)
    
    # Stream the response
    stream = streamer.stream(request.prompt, request.messages)
    return convert_to_response(stream)
```

## Advanced Configuration

### Custom Memory Management

```python
from llama_index.core.memory import ChatMemoryBuffer

# Create agent with custom memory
custom_memory = ChatMemoryBuffer.from_defaults(token_limit=2000)
agent = ReActAgent.from_tools(tools, memory=custom_memory, verbose=True)

streamer = LlamaIndexStreamer(agent, session_id="user-123")
```

### Multi-turn Conversations

```python
from ai_datastream.messages import ChatMessage, MessageRole

# Previous conversation history
messages = [
    ChatMessage(role=MessageRole.USER, content="Hello!"),
    ChatMessage(role=MessageRole.ASSISTANT, content="Hi there! How can I help?"),
]

# Continue the conversation
stream = streamer.stream("What's the weather like?", messages)
for part in stream:
    print(part.format(), end="")
```

## Architecture

### Core Components

1. **LlamaIndexStreamer**: Main streaming interface that orchestrates the conversion process
2. **LlamaIndexStreamConverter**: Converts LlamaIndex responses to data stream protocol format
3. **LlamaIndexMessageParser**: Handles message format conversion between ai-datastream and LlamaIndex

### Stream Processing Flow

1. Input messages are converted to LlamaIndex format using `LlamaIndexMessageParser`
2. Messages are added to agent memory for conversation continuity
3. Agent processes the input and generates a streaming response
4. `LlamaIndexStreamConverter` transforms the response into data stream parts
5. Stream parts are yielded in real-time to the client

## Supported Agent Types

- **ReActAgent**: Full support with tool calls and reasoning
- **OpenAIAgent**: Compatible with tool streaming
- **Custom AgentRunner**: Any subclass of `AgentRunner`
- **BaseAgentWorker**: Basic compatibility (limited streaming features)

## Data Stream Protocol

The module outputs stream parts following the Vercel AI SDK Data Stream Protocol:

- `0:` - Text content
- `9:` - Tool calls
- `a:` - Tool results
- `b:` - Tool call start
- `e:` - Step finish
- `f:` - Step start
- `d:` - Run finish

## Error Handling

The streamer includes robust error handling for:

- Malformed agent responses
- Streaming interruptions
- Tool execution failures
- Memory buffer overflows

Errors are gracefully converted to text stream parts to maintain stream continuity.

## Performance Considerations

- Use async methods (`async_stream`) for better performance in async environments
- Memory buffers are automatically managed but can be customized for memory-constrained environments
- Tool calls are streamed incrementally to reduce perceived latency

## Limitations

- Some LlamaIndex agent types may not support full streaming capabilities
- Tool results are streamed as they complete, which may not be real-time for long-running tools
- Complex multi-modal responses may require additional configuration
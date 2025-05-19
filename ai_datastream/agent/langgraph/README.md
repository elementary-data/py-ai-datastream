# LangGraph Framework

This module provides integration between LangGraph and the Vercel AI Data Stream protocol, allowing you to stream LangGraph agent responses to clients.

## Installation

To use the LangGraph framework, install it with pip:

```bash
pip install ai-datastream[langgraph]
```

## Usage

Here's a basic example of how to use the LangGraph framework:

```python
from langgraph.graph import StateGraph
from langgraph.prebuilt import create_react_agent
from langchain_openai import ChatOpenAI
from ai_datastream.agent.langgraph import LanggraphStreamer
from ai_datastream.messages import ChatMessage, MessageRole

# Initialize your LangGraph agent
model = ChatOpenAI(model="gpt-4")
tools = [...]  # your tools
prompt = "You are a helpful assistant."
agent = create_react_agent(model, tools)

# Create a streamer for the agent
streamer = LanggraphStreamer(agent)

# Create a message to send to the agent
message = ChatMessage(
    role=MessageRole.USER,
    content="What's the weather like in San Francisco?"
)

# Stream the response
for chunk in streamer.stream(prompt, [message]):
    print(chunk)  # Each chunk will be in the Vercel AI Data Stream protocol format
```

## Features

- Supports both synchronous and asynchronous streaming
- Handles tool calls and their results
- Converts LangGraph messages to the Vercel AI Data Stream protocol format
- Maintains conversation state and message history

## Requirements

- Python 3.9+
- LangGraph 0.3.0 or higher
- LangChain (for the agent implementation)

## Documentation

- [LangGraph Documentation](https://python.langchain.com/docs/langgraph)
- [Vercel AI Data Stream Protocol](https://sdk.vercel.ai/docs/ai-sdk-ui/stream-protocol#data-stream-protocol)
- [LangChain Documentation](https://python.langchain.com/docs/get_started/introduction)

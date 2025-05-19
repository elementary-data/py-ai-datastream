# Contributing to AI Data Stream

Thank you for your interest in contributing to AI Data Stream! This project implements the Vercel AI Data Stream protocol, which enables streaming AI chat responses to clients. There are two main types of contributions you can make:

## Framework Requirements

Regardless of whether you're implementing an agent framework or an API framework, you must:

1. Add a dependency group in `pyproject.toml` for your framework:
   ```toml
   [tool.poetry.group.your-framework]
   optional = true

   [tool.poetry.group.your-framework.dependencies]
   your-framework = ">=1.0.0,<2.0.0"
   ```

2. Create a README.md file in your framework's directory (e.g., `ai_datastream/agent/your-framework/README.md` or `ai_datastream/api/your-framework/README.md`) that includes:
   - Installation instructions
   - Basic usage examples
   - Any framework-specific configuration or requirements
   - Links to relevant documentation

## 1. Agent Framework Contributions

Agent frameworks are responsible for handling the AI agent's logic and streaming its responses. Currently, we support LangGraph as an agent framework.

### Implementing a New Agent Framework

To implement a new agent framework, you'll need to:

1. Create a new directory under `ai_datastream/agent/` for your framework
2. Implement at least one of the following interfaces (implementing both is highly preferred):
   ```python
   class Streamer(abc.ABC):
       @abc.abstractmethod
       def stream(self, prompt: str, messages: Sequence[ChatMessage]) -> Generator[DataStreamPart, None, None]:
           pass

   class AsyncStreamer(abc.ABC):
       @abc.abstractmethod
       def async_stream(self, prompt: str, messages: Sequence[ChatMessage]) -> AsyncGenerator[DataStreamPart, None]:
           pass
   ```

Your implementation should handle:
- Message parsing and conversion
- Tool calls and their results
- Streaming text responses
- Proper start and finish steps for each message

Note: While the LangGraph implementation uses additional classes like message parsers and stream converters, these are implementation details and not mandatory for new agent frameworks. You can structure your implementation in whatever way best suits your framework's needs, as long as you implement at least one of the above interfaces.

## 2. API Framework Contributions

API frameworks provide the web interface for the streaming functionality. Currently, we support FastAPI as an API framework.

### Implementing a New API Framework

To implement a new API framework, you'll need to implement all the necessary code to handle requests in the following format:

```
# Request
POST <URL>
Content-Type: application/json
{
    "messages": [{"role": "user", "content": "Hello, how are you?"}]
}

# Response
Content-Type: text/event-stream
Transfer-Encoding: chunked
x-ai-data-stream: 1
{type}:{data}
{type}:{data}
...
```

To do so, you'll need to:
1. Offer a request type that converts from the framework's request format to our internal `ChatMessage` format
2. Offer a way to stream the response of the `Streamer` or `AsyncStreamer` implementation (preferably both)
3. Set the headers to the correct values

## Development Setup

1. Install dependencies:
   ```bash
   poetry install
   ```

2. Install development dependencies:
   ```bash
   poetry install --with dev
   ```

3. Run tests and linting:
   ```bash
   poetry run ruff check .
   ```

## Pull Request Process

1. Fork the repository
2. Create a new branch for your feature
3. Make your changes
4. Add tests for your changes
5. Ensure all tests pass and linting is clean
6. Submit a pull request with a clear description of your changes

## License

By contributing to this project, you agree that your contributions will be licensed under the project's Apache 2.0 license.

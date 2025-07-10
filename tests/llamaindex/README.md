# LlamaIndex Streaming Tests

This directory contains tests for the LlamaIndex agent streaming implementation, focusing on real-world behavior rather than protocol details.

## Test Organization

### Core Test Files

- **`test_scenarios.py`** - Comprehensive scenario-based tests showing how different model behaviors translate to streaming output
- **`test_streamer.py`** - Tests for the main `LlamaIndexStreamer` class behavior
- **`test_stream_converter.py`** - Tests for converting LlamaIndex responses to stream parts
- **`test_message_parser.py`** - Tests for parsing chat messages to LlamaIndex format

### Testing Philosophy

Our tests prioritize:

1. **Readability**: Test names clearly describe the behavior being tested
2. **Minimal Mocking**: Use simple, realistic mock objects instead of complex mocking
3. **Behavior Focus**: Tests demonstrate real-world scenarios rather than implementation details
4. **Clear Assertions**: Each test verifies specific, meaningful outcomes

## Running Tests

```bash
# Run all LlamaIndex tests
poetry run pytest tests/llamaindex/ -v

# Run specific test file
poetry run pytest tests/llamaindex/test_scenarios.py -v

# Run with coverage
poetry run pytest tests/llamaindex/ --cov=ai_datastream.agent.llamaindex
```

## Test Scenarios

The scenario tests cover:

- **Reasoning then text**: Model thinks through a problem before answering
- **Text, tool, text**: Model explains, uses tools, then summarizes
- **Multi-turn conversations**: Proper handling of conversation history
- **Parallel tool calls**: Multiple tools called in one response
- **Error recovery**: Graceful handling of errors
- **Async behavior**: Ensuring async matches sync behavior

## Mocking Strategy

### Key Lessons from LangChain Tests

1. **Mock isinstance checks**: Use `@patch` to mock framework type checks
   ```python
   mock_isinstance.side_effect = lambda obj, cls: (
       cls.__name__ == 'StreamingAgentChatResponse' 
       if hasattr(cls, '__name__') 
       else False
   )
   ```

2. **Simple mock objects**: Create minimal mocks that simulate behavior
   ```python
   class MockStreamingResponse:
       @property
       def response_gen(self):
           for token in self._tokens:
               yield token
   ```

3. **Predictable responses**: Use factory functions to create consistent mocks
   ```python
   def create_mock_agent():
       agent = MagicMock()
       agent.stream_chat = lambda msg: MockStreamingResponse(...)
       return agent
   ```

### Mock Response Structure

```python
MockStreamingResponse(
    tokens=["Hello", " ", "world"],  # Text tokens to stream
    tool_calls=[{                    # Optional tool calls
        "id": "tool_123",
        "name": "calculator",
        "arguments": {"expr": "2+2"},
        "result": "4"
    }]
)
```

## Protocol Compliance

Tests verify Vercel AI SDK Data Stream Protocol compliance:

- `f:` - Start step (message begins)
- `0:` - Text content 
- `b:` - Tool call start
- `c:` - Tool call argument delta
- `9:` - Tool call complete
- `a:` - Tool result
- `e:` - Finish step (message ends)
- `d:` - Finish run (conversation ends)

## Common Patterns

### Testing Text Streaming
```python
text_parts = [p for p in parts if isinstance(p, DataStreamText)]
full_text = ''.join(p.body for p in text_parts)
assert "expected content" in full_text
```

### Testing Tool Calls
```python
tool_calls = [p for p in parts if isinstance(p, DataStreamToolCall)]
assert len(tool_calls) == 1
assert tool_calls[0].body["toolName"] == "expected_tool"
```

### Testing Stream Structure
```python
assert isinstance(parts[0], DataStreamStartStep)
assert isinstance(parts[-2], DataStreamFinishStep)
assert isinstance(parts[-1], DataStreamFinishRun)
```

## Known Limitations

1. **Tool call finish reason**: Current implementation doesn't track emitted tool calls for finish reason determination
2. **Memory handling**: Tests don't fully exercise agent memory features
3. **Complex streaming**: Some edge cases around interrupted streams not covered

## Contributing

When adding new tests:

1. Follow the scenario-based approach
2. Use descriptive test names
3. Minimize mocking complexity
4. Add comments explaining the scenario
5. Verify protocol compliance 
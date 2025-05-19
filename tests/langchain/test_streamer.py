from typing import Any, AsyncGenerator, Generator
from unittest.mock import MagicMock

import pytest
from langchain_core.messages import AIMessage, AIMessageChunk, ToolMessage

from ai_datastream.agent.langgraph.streamer import LanggraphStreamer
from ai_datastream.stream_parts import (
    DataStreamFinishRun,
    DataStreamFinishStep,
    DataStreamFinishStepReason,
    DataStreamStartStep,
    DataStreamText,
    DataStreamToolCall,
    DataStreamToolResult,
)
from tests.compare import compare_stream_parts

# Generated using generate_mock_stream.py
_MOCK_STREAM: list[dict[str, Any]] = [
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content="",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content="Got",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" it",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content="!",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" Before",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" I",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" fetch",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" the",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" current",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" weather",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" in",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" Tokyo",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" for",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" you",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=",",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" I'm",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" going",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" to",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" use",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" a",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" tool",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" call",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" to",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" obtain",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" the",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" latest",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" data",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=".",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" This",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" involves",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" connecting",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" to",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" a",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" service",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" that",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" provides",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" weather",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" information",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=",",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" specifically",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" for",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" the",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" city",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" you're",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" interested",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" in",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=".",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" Now",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=",",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" I'll",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" perform",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" the",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" tool",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" call",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" to",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" get",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" the",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" weather",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" in",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" Tokyo",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=".",
                additional_kwargs={},
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content="",
                additional_kwargs={
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": "call_TeWMRmlCYAjYehoHxliW1jPS",
                            "function": {"arguments": "", "name": "get_weather"},
                            "type": "function",
                        }
                    ]
                },
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
                tool_calls=[
                    {
                        "name": "get_weather",
                        "args": {},
                        "id": "call_TeWMRmlCYAjYehoHxliW1jPS",
                        "type": "tool_call",
                    }
                ],
                tool_call_chunks=[
                    {
                        "name": "get_weather",
                        "args": "",
                        "id": "call_TeWMRmlCYAjYehoHxliW1jPS",
                        "index": 0,
                        "type": "tool_call_chunk",
                    }
                ],
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content="",
                additional_kwargs={
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": None,
                            "function": {"arguments": '{"', "name": None},
                            "type": None,
                        }
                    ]
                },
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
                tool_calls=[{"name": "", "args": {}, "id": None, "type": "tool_call"}],
                tool_call_chunks=[
                    {
                        "name": None,
                        "args": '{"',
                        "id": None,
                        "index": 0,
                        "type": "tool_call_chunk",
                    }
                ],
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content="",
                additional_kwargs={
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": None,
                            "function": {"arguments": "city", "name": None},
                            "type": None,
                        }
                    ]
                },
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
                invalid_tool_calls=[
                    {
                        "name": None,
                        "args": "city",
                        "id": None,
                        "error": None,
                        "type": "invalid_tool_call",
                    }
                ],
                tool_call_chunks=[
                    {
                        "name": None,
                        "args": "city",
                        "id": None,
                        "index": 0,
                        "type": "tool_call_chunk",
                    }
                ],
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content="",
                additional_kwargs={
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": None,
                            "function": {"arguments": '":"', "name": None},
                            "type": None,
                        }
                    ]
                },
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
                invalid_tool_calls=[
                    {
                        "name": None,
                        "args": '":"',
                        "id": None,
                        "error": None,
                        "type": "invalid_tool_call",
                    }
                ],
                tool_call_chunks=[
                    {
                        "name": None,
                        "args": '":"',
                        "id": None,
                        "index": 0,
                        "type": "tool_call_chunk",
                    }
                ],
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content="",
                additional_kwargs={
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": None,
                            "function": {"arguments": "Tokyo", "name": None},
                            "type": None,
                        }
                    ]
                },
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
                invalid_tool_calls=[
                    {
                        "name": None,
                        "args": "Tokyo",
                        "id": None,
                        "error": None,
                        "type": "invalid_tool_call",
                    }
                ],
                tool_call_chunks=[
                    {
                        "name": None,
                        "args": "Tokyo",
                        "id": None,
                        "index": 0,
                        "type": "tool_call_chunk",
                    }
                ],
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content="",
                additional_kwargs={
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": None,
                            "function": {"arguments": '"}', "name": None},
                            "type": None,
                        }
                    ]
                },
                response_metadata={},
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
                invalid_tool_calls=[
                    {
                        "name": None,
                        "args": '"}',
                        "id": None,
                        "error": None,
                        "type": "invalid_tool_call",
                    }
                ],
                tool_call_chunks=[
                    {
                        "name": None,
                        "args": '"}',
                        "id": None,
                        "index": 0,
                        "type": "tool_call_chunk",
                    }
                ],
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content="",
                additional_kwargs={},
                response_metadata={
                    "finish_reason": "tool_calls",
                    "model_name": "gpt-4o-2024-08-06",
                    "system_fingerprint": "fp_90122d973c",
                },
                id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
            ),
            {
                "langgraph_step": 1,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "checkpoint_ns": "agent:a0bf1dda-c767-626d-28ba-21475a2b6a2b",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "updates",
        "step": {
            "agent": {
                "messages": [
                    AIMessage(
                        content=(
                            "Got it! Before I fetch the current weather in Tokyo for"
                            " you, I'm going to use a tool call to obtain the "
                            "latest data. This involves connecting to a service "
                            "that provides weather information, specifically for "
                            "the city you're interested in. Now, I'll perform the "
                            "tool call to get the weather in Tokyo."
                        ),
                        additional_kwargs={
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "call_TeWMRmlCYAjYehoHxliW1jPS",
                                    "function": {
                                        "arguments": '{"city":"Tokyo"}',
                                        "name": "get_weather",
                                    },
                                    "type": "function",
                                }
                            ]
                        },
                        response_metadata={
                            "finish_reason": "tool_calls",
                            "model_name": "gpt-4o-2024-08-06",
                            "system_fingerprint": "fp_90122d973c",
                        },
                        id="run-a2cddfba-ee24-429f-ad83-f9c789eb6dc0",
                        tool_calls=[
                            {
                                "name": "get_weather",
                                "args": {"city": "Tokyo"},
                                "id": "call_TeWMRmlCYAjYehoHxliW1jPS",
                                "type": "tool_call",
                            }
                        ],
                    )
                ]
            }
        },
    },
    {
        "mode": "messages",
        "step": (
            ToolMessage(
                content="The weather in Tokyo is sunny",
                name="get_weather",
                id="c34cb25d-a1cb-4ec5-923b-acbe9eb191f4",
                tool_call_id="call_TeWMRmlCYAjYehoHxliW1jPS",
            ),
            {
                "langgraph_step": 2,
                "langgraph_node": "tools",
                "langgraph_triggers": ("branch:to:tools",),
                "langgraph_path": ("__pregel_pull", "tools"),
                "langgraph_checkpoint_ns": "tools:13f38b4b-78a1-0615-ee16-cc461a990435",
            },
        ),
    },
    {
        "mode": "updates",
        "step": {
            "tools": {
                "messages": [
                    ToolMessage(
                        content="The weather in Tokyo is sunny",
                        name="get_weather",
                        id="c34cb25d-a1cb-4ec5-923b-acbe9eb191f4",
                        tool_call_id="call_TeWMRmlCYAjYehoHxliW1jPS",
                    )
                ]
            }
        },
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content="",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content="The",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" weather",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" in",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" Tokyo",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" is",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" currently",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" sunny",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=".",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" If",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" you",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" need",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" more",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" details",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" or",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" have",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" other",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" questions",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=",",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" feel",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" free",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" to",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content=" ask",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content="!",
                additional_kwargs={},
                response_metadata={},
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "messages",
        "step": (
            AIMessageChunk(
                content="",
                additional_kwargs={},
                response_metadata={
                    "finish_reason": "stop",
                    "model_name": "gpt-4o-2024-08-06",
                    "system_fingerprint": "fp_90122d973c",
                },
                id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
            ),
            {
                "langgraph_step": 3,
                "langgraph_node": "agent",
                "langgraph_triggers": ("branch:to:agent",),
                "langgraph_path": ("__pregel_pull", "agent"),
                "langgraph_checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "checkpoint_ns": "agent:779c70f5-55ff-d18d-0502-ecf7ed827954",
                "ls_provider": "openai",
                "ls_model_name": "gpt-4o",
                "ls_model_type": "chat",
                "ls_temperature": None,
            },
        ),
    },
    {
        "mode": "updates",
        "step": {
            "agent": {
                "messages": [
                    AIMessage(
                        content=(
                            "The weather in Tokyo is currently sunny. "
                            "If you need more details or have other questions, "
                            "feel free to ask!"
                        ),
                        additional_kwargs={},
                        response_metadata={
                            "finish_reason": "stop",
                            "model_name": "gpt-4o-2024-08-06",
                            "system_fingerprint": "fp_90122d973c",
                        },
                        id="run-56b61190-e66d-49d8-bc7e-f0d939f78cc0",
                    )
                ]
            }
        },
    },
]

_MOCK_DATA_STREAM = [
    DataStreamStartStep("b13fde6b6ae9420891bfad3e6126a92d"),
    DataStreamText("Got"),
    DataStreamText(" it"),
    DataStreamText("!"),
    DataStreamText(" Before"),
    DataStreamText(" I"),
    DataStreamText(" fetch"),
    DataStreamText(" the"),
    DataStreamText(" current"),
    DataStreamText(" weather"),
    DataStreamText(" in"),
    DataStreamText(" Tokyo"),
    DataStreamText(" for"),
    DataStreamText(" you"),
    DataStreamText(","),
    DataStreamText(" I'm"),
    DataStreamText(" going"),
    DataStreamText(" to"),
    DataStreamText(" use"),
    DataStreamText(" a"),
    DataStreamText(" tool"),
    DataStreamText(" call"),
    DataStreamText(" to"),
    DataStreamText(" obtain"),
    DataStreamText(" the"),
    DataStreamText(" latest"),
    DataStreamText(" data"),
    DataStreamText("."),
    DataStreamText(" This"),
    DataStreamText(" involves"),
    DataStreamText(" connecting"),
    DataStreamText(" to"),
    DataStreamText(" a"),
    DataStreamText(" service"),
    DataStreamText(" that"),
    DataStreamText(" provides"),
    DataStreamText(" weather"),
    DataStreamText(" information"),
    DataStreamText(","),
    DataStreamText(" specifically"),
    DataStreamText(" for"),
    DataStreamText(" the"),
    DataStreamText(" city"),
    DataStreamText(" you're"),
    DataStreamText(" interested"),
    DataStreamText(" in"),
    DataStreamText("."),
    DataStreamText(" Now"),
    DataStreamText(","),
    DataStreamText(" I'll"),
    DataStreamText(" perform"),
    DataStreamText(" the"),
    DataStreamText(" tool"),
    DataStreamText(" call"),
    DataStreamText(" to"),
    DataStreamText(" get"),
    DataStreamText(" the"),
    DataStreamText(" weather"),
    DataStreamText(" in"),
    DataStreamText(" Tokyo"),
    DataStreamText("."),
    DataStreamToolCall(
        tool_call_id="call_TeWMRmlCYAjYehoHxliW1jPS",
        tool_name="get_weather",
        args={"city": "Tokyo"},
    ),
    DataStreamToolResult(
        tool_call_id="call_TeWMRmlCYAjYehoHxliW1jPS",
        result="The weather in Tokyo is sunny",
    ),
    DataStreamFinishStep(
        finish_reason=DataStreamFinishStepReason.TOOL_CALLS,
    ),
    DataStreamStartStep("ac4e84aa6d474e11b60b19a002c4854f"),
    DataStreamText("The"),
    DataStreamText(" weather"),
    DataStreamText(" in"),
    DataStreamText(" Tokyo"),
    DataStreamText(" is"),
    DataStreamText(" currently"),
    DataStreamText(" sunny"),
    DataStreamText("."),
    DataStreamText(" If"),
    DataStreamText(" you"),
    DataStreamText(" need"),
    DataStreamText(" more"),
    DataStreamText(" details"),
    DataStreamText(" or"),
    DataStreamText(" have"),
    DataStreamText(" other"),
    DataStreamText(" questions"),
    DataStreamText(","),
    DataStreamText(" feel"),
    DataStreamText(" free"),
    DataStreamText(" to"),
    DataStreamText(" ask"),
    DataStreamText("!"),
    DataStreamFinishStep(
        finish_reason=DataStreamFinishStepReason.STOP,
    ),
    DataStreamFinishRun(),
]


class MockAgent:
    def stream(
        self, *args: Any, **kwargs: Any
    ) -> Generator[tuple[str, Any], None, None]:
        for stream_part in _MOCK_STREAM:
            mode = stream_part["mode"]
            step = stream_part["step"]
            yield mode, step

    async def astream(
        self, *args: Any, **kwargs: Any
    ) -> AsyncGenerator[tuple[str, Any], None]:
        for mode, step in self.stream(*args, **kwargs):
            yield mode, step

    def get_state(self, *args: Any, **kwargs: Any) -> MagicMock:
        state = MagicMock()
        state.next = False
        return state


@pytest.fixture
def streamer() -> LanggraphStreamer:
    return LanggraphStreamer(MockAgent())  # type: ignore[arg-type]


def test_stream(streamer: LanggraphStreamer) -> None:
    data_stream = list(streamer.stream("You are a helpful assistant", []))
    assert len(data_stream) == len(_MOCK_DATA_STREAM)
    for a, b in zip(data_stream, _MOCK_DATA_STREAM):
        assert compare_stream_parts(a, b)


@pytest.mark.asyncio
async def test_async_stream(streamer: LanggraphStreamer) -> None:
    parts = []
    async for part in streamer.async_stream("You are a helpful assistant", []):
        parts.append(part)
    assert len(parts) == len(_MOCK_DATA_STREAM)
    for a, b in zip(parts, _MOCK_DATA_STREAM):
        assert compare_stream_parts(a, b)

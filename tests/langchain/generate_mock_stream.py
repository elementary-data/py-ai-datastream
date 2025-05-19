from langchain_core.messages import HumanMessage
from langchain_core.tools import tool
from langchain_openai.chat_models import ChatOpenAI
from langgraph.prebuilt import create_react_agent


@tool
def get_weather(city: str) -> str:
    """
    Get the weather in a city
    """
    return f"The weather in {city} is sunny"


def main() -> None:
    stream = []
    model = ChatOpenAI(model="gpt-4o")
    agent = create_react_agent(model, tools=[get_weather])
    for mode, step in agent.stream(
        {
            "messages": [
                HumanMessage(
                    content=(
                        "What is the weather in Tokyo? great me and tell me what "
                        "you are doing before berforming tool calls"
                    )
                )
            ]
        },
        stream_mode=["messages", "updates"],
    ):
        stream.append({"mode": mode, "step": step})
    print(stream)


if __name__ == "__main__":
    main()

from openai import OpenAI
from .config import get_oauth_token, get_workspace_host, SERVING_ENDPOINT


def get_llm_client() -> OpenAI:
    host = get_workspace_host()
    token = get_oauth_token()
    return OpenAI(api_key=token, base_url=f"{host}/serving-endpoints")


def chat_completion(messages: list[dict], temperature: float = 0.0) -> str:
    client = get_llm_client()
    response = client.chat.completions.create(
        model=SERVING_ENDPOINT,
        messages=messages,
        max_tokens=4096,
        temperature=temperature,
    )
    return response.choices[0].message.content

from openai import OpenAI


class OpenAIService:
    """
    Centralized OpenAI helper methods used by data analysis services.
    """

    @staticmethod
    def get_client(api_key: str) -> OpenAI:
        return OpenAI(api_key=api_key)

    @staticmethod
    def get_chat_completion(client, settings, system_prompt: str, user_prompt: str, max_tokens: int = 0) -> str:
        params = {
            "model": settings.chatgpt_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": max_tokens if max_tokens > 0 else None,
            "temperature": settings.chatgpt_temperature,
            "top_p": settings.chatgpt_top_p,
        }

        response = client.chat.completions.create(
            **{key: value for key, value in params.items() if value is not None}
        )

        content = response.choices[0].message.content
        return content.strip() if content else ""

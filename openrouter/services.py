import requests


class OpenRouterService:
    BASE_URL = "https://openrouter.ai/api/v1"
    MODELS_ENDPOINT = f"{BASE_URL}/models"
    CHAT_COMPLETIONS_ENDPOINT = f"{BASE_URL}/chat/completions"
    TIMEOUT_SECONDS = 30

    @staticmethod
    def list_models():
        response = requests.get(
            OpenRouterService.MODELS_ENDPOINT,
            timeout=OpenRouterService.TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def filter_models_by_name(models_data, name: str):
        search_term = (name or "").strip().lower()
        if not search_term:
            return models_data

        if isinstance(models_data, dict):
            models = models_data.get("data")
            if isinstance(models, list):
                filtered = [
                    model
                    for model in models
                    if search_term in str(model.get("name", "")).lower()
                    or search_term in str(model.get("id", "")).lower()
                ]
                filtered_data = dict(models_data)
                filtered_data["data"] = filtered
                return filtered_data
            return models_data

        if isinstance(models_data, list):
            return [
                model
                for model in models_data
                if search_term in str(model.get("name", "")).lower()
                or search_term in str(model.get("id", "")).lower()
            ]

        return models_data

    @staticmethod
    def get_chat_completion(
        bearer_token: str,
        model: str,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 0,
        temperature: float = 0.7,
    ) -> str:
        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json",
            "X-OpenRouter-Experimental-Metadata": "enabled",
        }

        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": max_tokens if max_tokens > 0 else None,
            "temperature": temperature,
        }

        request_body = {key: value for key, value in payload.items() if value is not None}

        try:
            response = requests.post(
                OpenRouterService.CHAT_COMPLETIONS_ENDPOINT,
                headers=headers,
                json=request_body,
                timeout=OpenRouterService.TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            print(
                f"[OpenRouter] connection error model={model!r} "
                f"system_prompt_len={len(system_prompt or '')} "
                f"user_prompt_len={len(user_prompt or '')} error={exc}"
            )
            raise

        if not response.ok:
            try:
                upstream_error = response.json()
            except ValueError:
                upstream_error = response.text

            print(
                f"[OpenRouter] HTTP {response.status_code} model={model!r} "
                f"system_prompt_len={len(system_prompt or '')} "
                f"user_prompt_len={len(user_prompt or '')} "
                f"max_tokens={request_body.get('max_tokens')} "
                f"temperature={request_body.get('temperature')} "
                f"upstream_response={upstream_error!r}"
            )
            response.raise_for_status()

        response_data = response.json()
        choices = response_data.get("choices", [])
        if not choices:
            print(f"[OpenRouter] empty choices model={model!r} response={response_data!r}")
            return ""

        message = choices[0].get("message", {})
        content = message.get("content")
        return content.strip() if isinstance(content, str) else ""

    @staticmethod
    def get_chat_completion_with_transcripts(
        bearer_token: str,
        model: str,
        system_prompt: str,
        transcripts: list[dict[str, str]],
        max_tokens: int = 1000,
        temperature: float = 0.7,
    ) -> str:
        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json",
            "X-OpenRouter-Experimental-Metadata": "enabled",
        }

        content_blocks = [
            {"type": "text", "text": f"{t['title']}:\n{t['text']}"}
            for t in transcripts
        ]

        payload: dict = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": content_blocks},
            ],
        }
        if max_tokens is not None and max_tokens > 0:
            payload["max_tokens"] = max_tokens
        if temperature is not None:
            payload["temperature"] = temperature

        request_body = payload

        transcript_total_len = sum(len(str(t.get("text") or "")) for t in transcripts)

        try:
            response = requests.post(
                OpenRouterService.CHAT_COMPLETIONS_ENDPOINT,
                headers=headers,
                json=request_body,
                timeout=OpenRouterService.TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            print(
                f"[OpenRouter] connection error model={model!r} "
                f"system_prompt_len={len(system_prompt or '')} "
                f"transcript_count={len(transcripts)} "
                f"transcript_total_len={transcript_total_len} error={exc}"
            )
            raise

        if not response.ok:
            try:
                upstream_error = response.json()
            except ValueError:
                upstream_error = response.text

            print(
                f"[OpenRouter] HTTP {response.status_code} model={model!r} "
                f"system_prompt_len={len(system_prompt or '')} "
                f"transcript_count={len(transcripts)} "
                f"transcript_total_len={transcript_total_len} "
                f"max_tokens={request_body.get('max_tokens')} "
                f"temperature={request_body.get('temperature')} "
                f"upstream_response={upstream_error!r}"
            )
            response.raise_for_status()

        response_data = response.json()
        choices = response_data.get("choices", [])
        if not choices:
            print(f"[OpenRouter] empty choices model={model!r} response={response_data!r}")
            return ""

        message = choices[0].get("message", {})
        content = message.get("content")
        return content.strip() if isinstance(content, str) else ""

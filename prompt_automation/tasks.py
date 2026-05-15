from celery import shared_task

from .utils import execute_prompt_run_llm


@shared_task
def run_prompt_run_llm_task(prompt_run_id: int, max_tokens: int = 1000) -> int:
    execute_prompt_run_llm(prompt_run_id, max_tokens=max_tokens)
    return prompt_run_id

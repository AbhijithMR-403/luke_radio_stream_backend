from __future__ import annotations

from typing import Iterable, List, Optional, Sequence

from django.core.exceptions import ValidationError
from django.db import transaction
from django.db.models import QuerySet

from audio_flags.models import FlagCondition


class FlagConditionRepository:
    """Data Access Object for `FlagCondition` model."""

    # ------------------------------------------------------------------ #
    # Basic getters
    # ------------------------------------------------------------------ #
    @staticmethod
    def get_by_id(condition_id: int) -> Optional[FlagCondition]:
        try:
            return FlagCondition.objects.get(id=condition_id)
        except FlagCondition.DoesNotExist:
            return None

    @staticmethod
    def get_all() -> QuerySet[FlagCondition]:
        return FlagCondition.objects.all()

    @staticmethod
    def get_by_channel(
        channel_id: int,
        *,
        active_only: bool = False,
    ) -> QuerySet[FlagCondition]:
        qs = FlagCondition.objects.filter(channel_id=channel_id)
        if active_only:
            qs = qs.filter(is_active=True)
        return qs

    @staticmethod
    def get_by_name(name: str) -> Optional[FlagCondition]:
        try:
            return FlagCondition.objects.get(name=name)
        except FlagCondition.DoesNotExist:
            return None

    # ------------------------------------------------------------------ #
    # Mutations
    # ------------------------------------------------------------------ #
    @staticmethod
    def create(**condition_data) -> FlagCondition:
        condition = FlagCondition(**condition_data)
        condition.full_clean()
        condition.save()
        return condition

    @staticmethod
    def update(condition_id: int, **changes) -> Optional[FlagCondition]:
        condition = FlagConditionRepository.get_by_id(condition_id)
        if not condition:
            return None

        for field, value in changes.items():
            if hasattr(condition, field):
                setattr(condition, field, value)

        condition.full_clean()
        condition.save()
        return condition

    @staticmethod
    def delete(condition_id: int) -> bool:
        condition = FlagConditionRepository.get_by_id(condition_id)
        if not condition:
            return False
        condition.delete()
        return True

from __future__ import annotations

from datetime import datetime
from typing import Iterable, List, Literal, Optional, Sequence

from django.core.exceptions import ValidationError
from django.db import transaction
from django.db.models import Q, QuerySet

from data_analysis.models import AudioSegments


class AudioSegmentDAO:
    """Data Access Object for `AudioSegments` model."""

    # ------------------------------------------------------------------ #
    # Basic getters
    # ------------------------------------------------------------------ #
    @staticmethod
    def get_by_id(segment_id: int) -> Optional[AudioSegments]:
        try:
            return AudioSegments.objects.get(id=segment_id)
        except AudioSegments.DoesNotExist:
            return None

    @staticmethod
    def get_all() -> QuerySet[AudioSegments]:
        return AudioSegments.objects.all()

    @staticmethod
    def get_by_channel(
        channel_id: int,
        *,
        include_deleted: bool = False,
        active_only: bool = False,
    ) -> QuerySet[AudioSegments]:
        qs = AudioSegments.objects.filter(channel_id=channel_id)
        if not include_deleted:
            qs = qs.filter(is_delete=False)
        if active_only:
            qs = qs.filter(is_active=True)
        return qs

    @staticmethod
    def get_by_file_path(file_path: str) -> Optional[AudioSegments]:
        return AudioSegments.objects.filter(file_path=file_path).first()

    @staticmethod
    def filter_by_date_range(
        *,
        channel_id: Optional[int] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        include_deleted: bool = False,
    ) -> QuerySet[AudioSegments]:
        qs = AudioSegments.objects.all()
        if channel_id is not None:
            qs = qs.filter(channel_id=channel_id)
        if start is not None:
            qs = qs.filter(start_time__gte=start)
        if end is not None:
            qs = qs.filter(end_time__lte=end)
        if not include_deleted:
            qs = qs.filter(is_delete=False)
        return qs

    @staticmethod
    def _build_filtered_queryset(
        *,
        channel_id: Optional[int] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        is_active: Optional[bool] = None,
        is_recognized: Optional[bool] = None,
        recognition_status: Optional[Literal['all', 'recognized', 'unrecognized']] = None,
        has_content: Optional[bool] = None,
        is_delete: bool = False,
    ) -> QuerySet[AudioSegments]:
        """
        Private helper method to build a filtered queryset.
        This contains the common filtering logic used by both filter() and count().
        
        Returns a base queryset with all filters applied, but without:
        - select_related/prefetch_related (added by filter() if needed)
        - order_by (added by filter() if needed)
        """
        # Start with base queryset - filter by channel first for index optimization
        if channel_id is not None:
            qs = AudioSegments.objects.filter(channel_id=channel_id)
        else:
            qs = AudioSegments.objects.all()
        
        # Apply is_delete filter early (uses ['channel', 'is_delete'] index if channel_id provided)
        qs = qs.filter(is_delete=is_delete)
        
        # Apply start_time filter (uses ['channel', 'start_time'] index if channel_id provided)
        if start_time is not None:
            qs = qs.filter(start_time__gte=start_time)
        
        # Apply end_time filter
        if end_time is not None:
            # Use start_time <= end_time for better index usage
            # This ensures we use the ['channel', 'start_time'] index
            qs = qs.filter(start_time__lte=end_time)
        
        # Apply is_active filter (uses ['channel', 'is_active'] index if channel_id provided)
        if is_active is not None:
            qs = qs.filter(is_active=is_active)
        
        # Apply recognition status filter
        # recognition_status takes precedence over is_recognized if both are provided
        if recognition_status is not None:
            if recognition_status == 'recognized':
                qs = qs.filter(is_recognized=True)
            elif recognition_status == 'unrecognized':
                qs = qs.filter(is_recognized=False)
            # 'all' means no filter on is_recognized
        elif is_recognized is not None:
            # Fall back to is_recognized if recognition_status not provided
            qs = qs.filter(is_recognized=is_recognized)
        
        # Apply content filter (checks if transcription_detail exists)
        if has_content is not None:
            if has_content:
                # With Content: transcription_detail must exist
                qs = qs.filter(transcription_detail__isnull=False)
            else:
                # No Content: transcription_detail must not exist
                qs = qs.filter(transcription_detail__isnull=True)
        
        return qs

    @staticmethod
    def filter(
        *,
        channel_id: Optional[int] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        is_active: Optional[bool] = None,
        is_recognized: Optional[bool] = None,
        recognition_status: Optional[Literal['all', 'recognized', 'unrecognized']] = None,
        has_content: Optional[bool] = None,
        is_delete: bool = False,
    ) -> QuerySet[AudioSegments]:
        """
        Optimized filter method for AudioSegments with maximum performance.
        
        This method is designed to use database indexes efficiently:
        - Uses ['channel', 'start_time'] index when channel_id and start_time are provided
        - Uses ['channel', 'is_active'] index when channel_id and is_active are provided
        - Uses ['channel', 'is_delete'] index when channel_id and is_delete are provided
        
        Automatically fetches related data from:
        - channel (ForeignKey)
        - transcription_detail (OneToOneField)
        - transcription_detail.analysis (OneToOneField via TranscriptionDetail)
        
        Args:
            channel_id: Filter by channel ID (highly recommended for index usage)
            start_time: Filter segments with start_time >= start_time
            end_time: Filter segments with start_time <= end_time
            is_active: Filter by active status (True/False/None for all)
            is_recognized: Filter by recognized status (True/False/None for all)
                Note: If recognition_status is provided, it will override this parameter
            recognition_status: Filter by recognition status:
                - 'all': All segments (recognized and unrecognized)
                - 'recognized': Only recognized segments (is_recognized=True)
                - 'unrecognized': Only unrecognized segments (is_recognized=False)
            has_content: Filter by transcription content:
                - True: Only segments with transcription_detail (With Content)
                - False: Only segments without transcription_detail (No Content)
                - None: All segments regardless of content
            is_delete: Filter by delete status (default: False)
        
        Returns:
            QuerySet[AudioSegments]: Optimized queryset ordered by start_time with related data prefetched
        
        Examples:
            # Get all segments (recognized and unrecognized)
            segments = AudioSegmentDAO.filter(
                channel_id=1,
                recognition_status='all'
            )
            
            # Get only recognized segments
            segments = AudioSegmentDAO.filter(
                channel_id=1,
                recognition_status='recognized'
            )
            
            # Get unrecognized segments with content
            segments = AudioSegmentDAO.filter(
                channel_id=1,
                recognition_status='unrecognized',
                has_content=True
            )
            
            # Get recognized segments without content
            segments = AudioSegmentDAO.filter(
                channel_id=1,
                recognition_status='recognized',
                has_content=False
            )
        """
        # Build base filtered queryset using shared logic
        qs = AudioSegmentDAO._build_filtered_queryset(
            channel_id=channel_id,
            start_time=start_time,
            end_time=end_time,
            is_active=is_active,
            is_recognized=is_recognized,
            recognition_status=recognition_status,
            has_content=has_content,
            is_delete=is_delete,
        )
        
        # Optimize with select_related for ForeignKey and OneToOne relationships
        # This fetches channel, transcription_detail, and transcription_detail.analysis in a single query
        qs = qs.select_related(
            'channel',
            'transcription_detail',
            'transcription_detail__analysis'
        )
        
        # Order by start_time (uses the same index)
        qs = qs.order_by('start_time')
        
        return qs

    @staticmethod
    def filter_with_q(
        q_objects: Q | List[Q],
        *,
        channel_id: Optional[int] = None,
        is_recognized: Optional[bool] = None,
        has_content: bool = True,
        is_active: bool = True,
        is_delete: bool = False,
    ) -> QuerySet[AudioSegments]:
        """
        Filter AudioSegments using Q objects (typically for multiple time ranges) with optimized defaults.
        
        This method is designed for complex queries where you need to filter by multiple time ranges
        using Q objects. It applies sensible defaults and combines Q objects with OR logic.
        
        Default filter values:
        - has_content=True: Only segments with transcription_detail
        - is_active=True: Only active segments
        - is_delete=False: Exclude deleted segments
        
        This method is designed to use database indexes efficiently:
        - Uses ['channel', 'start_time'] index when channel_id is provided
        - Uses ['channel', 'is_active'] index when channel_id and is_active are provided
        - Uses ['channel', 'is_delete'] index when channel_id and is_delete are provided
        
        Automatically fetches related data from:
        - channel (ForeignKey)
        - transcription_detail (OneToOneField)
        - transcription_detail.analysis (OneToOneField via TranscriptionDetail)
        
        Args:
            q_objects: Q object or list of Q objects containing time range queries 
                (e.g., Q(start_time__gte=start, start_time__lte=end))
                Multiple Q objects are combined with OR logic
            channel_id: Filter by channel ID (highly recommended for index usage)
            is_recognized: Filter by recognized status (True/False/None for all)
                Note: If recognition_status is provided, it will override this parameter
            recognition_status: Filter by recognition status:
                - 'all': All segments (recognized and unrecognized)
                - 'recognized': Only recognized segments (is_recognized=True)
                - 'unrecognized': Only unrecognized segments (is_recognized=False)
            has_content: Filter by transcription content (default: True)
                - True: Only segments with transcription_detail (With Content)
                - False: Only segments without transcription_detail (No Content)
            is_active: Filter by active status (default: True)
            is_delete: Filter by delete status (default: False)
        
        Returns:
            QuerySet[AudioSegments]: Optimized queryset ordered by start_time with related data prefetched
        
        Examples:
            from django.db.models import Q
            from datetime import datetime
            
            # Filter by multiple time ranges (list of Q objects)
            q1 = Q(start_time__gte=datetime(2024, 1, 1), start_time__lte=datetime(2024, 1, 2))
            q2 = Q(start_time__gte=datetime(2024, 1, 5), start_time__lte=datetime(2024, 1, 6))
            segments = AudioSegmentDAO.filter_with_q(
                q_objects=[q1, q2],
                channel_id=1,
                recognition_status='recognized'
            )
            
            # Single time range with Q object (can pass single Q or list)
            q = Q(start_time__gte=datetime(2024, 1, 1), start_time__lte=datetime(2024, 1, 2))
            segments = AudioSegmentDAO.filter_with_q(
                q_objects=q,
                channel_id=1
            )
        """
        # Normalize q_objects to a list
        if isinstance(q_objects, Q):
            q_objects = [q_objects]
        
        if not q_objects:
            # If no Q objects provided, return empty queryset
            return AudioSegments.objects.none()
        
        # Start with base queryset - filter by channel first for index optimization
        if channel_id is not None:
            qs = AudioSegments.objects.filter(channel_id=channel_id)
        else:
            qs = AudioSegments.objects.all()
        
        # Apply default filters
        qs = qs.filter(is_delete=is_delete)
        qs = qs.filter(is_active=is_active)
        
        # Apply content filter
        if has_content:
            qs = qs.filter(transcription_detail__isnull=False)
        
        # recognition_status takes precedence over is_recognized if both are provided
        if is_recognized is not None:
            # Fall back to is_recognized if recognition_status not provided
            qs = qs.filter(is_recognized=is_recognized)
        
        # Combine all Q objects with OR logic
        combined_q = q_objects[0]
        for q_obj in q_objects[1:]:
            combined_q |= q_obj
        
        # Apply the combined Q object
        qs = qs.filter(combined_q)
        
        # Optimize with select_related for ForeignKey and OneToOne relationships
        # This fetches channel, transcription_detail, and transcription_detail.analysis in a single query
        qs = qs.select_related(
            'channel',
            'transcription_detail',
            'transcription_detail__analysis'
        )
        
        # Order by start_time (uses the same index)
        qs = qs.order_by('start_time')
        
        return qs

    @staticmethod
    def count(
        *,
        channel_id: Optional[int] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        is_active: Optional[bool] = None,
        is_recognized: Optional[bool] = None,
        recognition_status: Optional[Literal['all', 'recognized', 'unrecognized']] = None,
        has_content: Optional[bool] = None,
        is_delete: bool = False,
    ) -> int:
        """
        Optimized count method for AudioSegments with maximum performance.
        
        This method uses the same filter logic as filter() but optimized for counting:
        - Skips select_related/prefetch_related (not needed for count)
        - Skips order_by (not needed for count)
        - Uses Django's optimized .count() method
        
        This is faster than calling .count() on the filter() result because it avoids
        unnecessary joins and ordering operations.
        
        Args:
            channel_id: Filter by channel ID (highly recommended for index usage)
            start_time: Filter segments with start_time >= start_time
            end_time: Filter segments with start_time <= end_time
            is_active: Filter by active status (True/False/None for all)
            is_recognized: Filter by recognized status (True/False/None for all)
                Note: If recognition_status is provided, it will override this parameter
            recognition_status: Filter by recognition status:
                - 'all': All segments (recognized and unrecognized)
                - 'recognized': Only recognized segments (is_recognized=True)
                - 'unrecognized': Only unrecognized segments (is_recognized=False)
            has_content: Filter by transcription content:
                - True: Only segments with transcription_detail (With Content)
                - False: Only segments without transcription_detail (No Content)
                - None: All segments regardless of content
            is_delete: Filter by delete status (default: False)
        
        Returns:
            int: Count of segments matching the filters
        
        Examples:
            # Count all segments
            count = AudioSegmentDAO.count(channel_id=1, recognition_status='all')
            
            # Count recognized segments with content
            count = AudioSegmentDAO.count(
                channel_id=1,
                recognition_status='recognized',
                has_content=True
            )
        """
        # Build base filtered queryset using shared logic
        qs = AudioSegmentDAO._build_filtered_queryset(
            channel_id=channel_id,
            start_time=start_time,
            end_time=end_time,
            is_active=is_active,
            is_recognized=is_recognized,
            recognition_status=recognition_status,
            has_content=has_content,
            is_delete=is_delete,
        )
        
        # Use count() - optimized by Django, no need for select_related or order_by
        return qs.count()

    # ------------------------------------------------------------------ #
    # Mutations
    # ------------------------------------------------------------------ #
    @staticmethod
    def create(**segment_data) -> AudioSegments:
        segment = AudioSegments(**segment_data)
        segment.full_clean()
        segment.save()
        return segment

    @staticmethod
    def bulk_create(
        segments_data: Iterable[dict],
        *,
        skip_existing_by_file_path: bool = True,
    ) -> List[AudioSegments]:
        created_segments: List[AudioSegments] = []
        if not isinstance(segments_data, Iterable):
            raise ValidationError("segments_data must be an iterable of dicts")

        with transaction.atomic():
            for data in segments_data:
                if skip_existing_by_file_path and data.get("file_path"):
                    existing = AudioSegmentDAO.get_by_file_path(data["file_path"])
                    if existing:
                        created_segments.append(existing)
                        continue
                created_segments.append(AudioSegmentDAO.create(**data))

        return created_segments

    @staticmethod
    def update(segment_id: int, **changes) -> Optional[AudioSegments]:
        segment = AudioSegmentDAO.get_by_id(segment_id)
        if not segment:
            return None

        for field, value in changes.items():
            if hasattr(segment, field):
                setattr(segment, field, value)

        segment.full_clean()
        segment.save()
        return segment

    @staticmethod
    def bulk_update(segment_ids: Sequence[int], **changes) -> int:
        if not changes:
            return 0
        return AudioSegments.objects.filter(id__in=segment_ids).update(**changes)

    @staticmethod
    def soft_delete(segment_id: int) -> Optional[AudioSegments]:
        return AudioSegmentDAO.update(segment_id, is_delete=True, is_active=False)

    @staticmethod
    def restore(segment_id: int) -> Optional[AudioSegments]:
        return AudioSegmentDAO.update(segment_id, is_delete=False, is_active=True)

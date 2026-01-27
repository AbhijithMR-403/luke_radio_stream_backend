from typing import Dict, Set, Tuple, Optional, List
from datetime import datetime, timedelta
from django.db.models import QuerySet, Q
from django.utils import timezone
from collections import defaultdict

from data_analysis.models import TranscriptionAnalysis, ReportFolder
from core_admin.models import WellnessBucket, Channel
from shift_analysis.models import Shift
from dashboard.repositories import AudioSegmentDAO


class BucketCountService:
    """
    Service class for counting audio segments analyzed from bucket_prompt,
    classified by WellnessBucket categories (personal, community, spiritual).
    """

    @staticmethod
    def _parse_bucket_prompt_line(line: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Parse a single bucket prompt line and return (primary, secondary) topic names.
        Handles formats like:
          - "FUN, 85, RELATIONSHIPS, 75"
          - "MENTAL, 100, FAITH JOURNEY, 0"
          - "RELATIONSHIPS, 90%, FUN, 85%"
          - "GENEROSITY, 90%, CHRISTIAN COMMUNITY, 10%"
          - "PHYSICAL, 85, undefined, 0"
        
        Returns tuple of (primary_or_None, secondary_or_None)
        """
        if not line:
            return None, None
        text = line.strip()
        if not text:
            return None, None
        
        # Skip AI output prefixes
        ai_prefixes = ["empty result", "output:", "result:", "analysis:", "response:"]
        text_lower = text.lower()
        for prefix in ai_prefixes:
            if text_lower.startswith(prefix):
                text = text[len(prefix):].strip()
                if text.startswith('\n'):
                    text = text[1:].strip()
                break
        
        # Normalize separators to comma
        parts = [p.strip() for p in text.replace("\t", ",").replace("|", ",").split(",")]
        
        # Must have at least 4 comma-separated values to determine primary/secondary
        if len(parts) < 4:
            return None, None
        
        def is_undefined(token):
            if token is None:
                return True
            t = str(token).strip().lower()
            return t in {"undefined", "undef", "none", "null", "na", "n/a", "", "empty result", "output"}
        
        def is_score(token):
            if token is None:
                return False
            t = str(token).strip().replace("%", "")
            if not t:
                return False
            try:
                float(t)
                return True
            except ValueError:
                return False
        
        # Extract first two valid topics (skip scores)
        topics = []
        i = 0
        while i < len(parts) and len(topics) < 2:
            token = parts[i]
            # Skip empty tokens
            if token == "":
                i += 1
                continue
            # If looks like a score, skip and move on
            if is_score(token):
                i += 1
                continue
            # This token is a candidate topic; ensure it's not undefined
            if not is_undefined(token):
                topics.append(token)
            # Advance; also skip next token if it's a score paired with this topic
            if i + 1 < len(parts) and is_score(parts[i+1]):
                i += 2
            else:
                i += 1
        
        # Return primary and secondary only if we found both
        primary = topics[0] if len(topics) > 0 else None
        secondary = topics[1] if len(topics) > 1 else None
        return primary, secondary

    @staticmethod
    def _get_bucket_title_to_category_mapping() -> Dict[str, str]:
        """
        Get mapping from WellnessBucket title (uppercase) to category.
        
        Returns:
            Dictionary mapping bucket title (uppercase) to category
        """
        wellness_buckets = WellnessBucket.objects.filter(
            general_setting__is_active=True,
            is_deleted=False
        )
        bucket_title_to_category = {}
        for bucket in wellness_buckets:
            bucket_title_to_category[bucket.title.upper()] = bucket.category
        return bucket_title_to_category

    @staticmethod
    def _map_bucket_name_to_categories(
        bucket_name: str,
        bucket_title_to_category: Dict[str, str]
    ) -> Set[str]:
        """
        Map a bucket name to its category(ies) using exact and fuzzy matching.
        
        Args:
            bucket_name: The bucket name to map
            bucket_title_to_category: Mapping from bucket title to category
        
        Returns:
            Set of category names that match the bucket name
        """
        found_categories = set()
        
        if not bucket_name:
            return found_categories
        
        # Normalize bucket name: uppercase and strip whitespace
        bucket_name_normalized = ' '.join(bucket_name.upper().split())
        
        # Try exact match first
        if bucket_name_normalized in bucket_title_to_category:
            found_categories.add(bucket_title_to_category[bucket_name_normalized])
        else:
            # Try partial/fuzzy match (case-insensitive)
            # Normalize titles for comparison
            for title, category in bucket_title_to_category.items():
                title_normalized = ' '.join(title.split())
                # Check if bucket name matches title (exact or contains)
                if (bucket_name_normalized == title_normalized or 
                    bucket_name_normalized in title_normalized or 
                    title_normalized in bucket_name_normalized):
                    found_categories.add(category)
                    break
        
        return found_categories

    @staticmethod
    def _extract_categories_from_bucket_prompt(
        bucket_text: str,
        bucket_title_to_category: Dict[str, str]
    ) -> Set[str]:
        """
        Extract categories from bucket_prompt text by parsing and mapping bucket names.
        
        Args:
            bucket_text: The bucket_prompt text from TranscriptionAnalysis
            bucket_title_to_category: Mapping from bucket title to category
        
        Returns:
            Set of category names found in the bucket_prompt
        """
        found_categories = set()
        
        if not bucket_text:
            return found_categories
        
        # Parse bucket_prompt to extract bucket names
        lines = [l for l in str(bucket_text).split('\n') if l.strip()]
        if not lines:
            lines = [str(bucket_text)]
        
        for line in lines:
            primary, secondary = BucketCountService._parse_bucket_prompt_line(line)
            
            # Map bucket names to categories
            for bucket_name in [primary, secondary]:
                categories = BucketCountService._map_bucket_name_to_categories(
                    bucket_name,
                    bucket_title_to_category
                )
                found_categories.update(categories)
        
        return found_categories


    @staticmethod
    def _get_last_6_months() -> List[Tuple[str, datetime, datetime]]:
        """
        Calculate the last 6 months (excluding current month).
        
        Returns:
            List of tuples: (month_key, month_start, month_end) for last 6 months
        """
        current_time = timezone.now()
        current_month_start = current_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        months = []
        for i in range(1, 7):  # Last 6 months (1 to 6 months ago)
            # Calculate target month and year
            target_month = current_time.month - i
            if target_month > 0:
                year = current_time.year
                month = target_month
            else:
                # Need to go to previous year
                year = current_time.year - 1
                month = 12 + target_month  # target_month is negative (e.g., -1 becomes 11)
            
            month_start = datetime(year, month, 1, 0, 0, 0, 0, tzinfo=current_time.tzinfo)
            
            # Calculate month end (last day of the month)
            if month == 12:
                next_month_start = datetime(year + 1, 1, 1, 0, 0, 0, 0, tzinfo=current_time.tzinfo)
            else:
                next_month_start = datetime(year, month + 1, 1, 0, 0, 0, 0, tzinfo=current_time.tzinfo)
            month_end = next_month_start - timedelta(microseconds=1)
            
            month_key = month_start.strftime('%Y-%m')
            months.append((month_key, month_start, month_end))
        
        return months

    @staticmethod
    def get_bucket_counts(
        start_dt: datetime,
        end_dt: datetime,
        channel_id: Optional[int] = None,
        shift_id: Optional[int] = None,
        report_folder_id: Optional[int] = None,
    ) -> Dict[str, any]:
        """
        Get count of audio segments analyzed from bucket_prompt, classified by category.
        Includes overall counts and monthly breakdowns for last 6 months (excluding current month).
        
        Args:
            start_dt: Start datetime (timezone-aware) - used for overall counts only
            end_dt: End datetime (timezone-aware) - used for overall counts only
            channel_id: Channel ID to filter by (required if report_folder_id not provided)
            shift_id: Optional shift ID to filter by
            report_folder_id: Optional report folder ID to filter by (required if channel_id not provided)
        
        Returns:
            Dictionary containing:
            - Category objects (personal, community, spiritual) with count and percentage
            - total: Total count across all categories
            - monthly_breakdown: Monthly breakdown for last 6 months (excluding current month)
        """
        # Validate filter inputs
        if channel_id is None and report_folder_id is None:
            raise ValueError("Either channel_id or report_folder_id must be provided")

        # Get bucket title to category mapping
        bucket_title_to_category = BucketCountService._get_bucket_title_to_category_mapping()
        
        # Get last 6 months (excluding current month)
        last_6_months = BucketCountService._get_last_6_months()
        
        # Handle report folder case - get channel_id from folder
        if report_folder_id is not None:
            try:
                report_folder = ReportFolder.objects.select_related('channel').get(id=report_folder_id)
                channel_id = report_folder.channel.id
            except ReportFolder.DoesNotExist:
                # If report folder doesn't exist, return empty result
                return {
                    'personal': {'count': 0, 'percentage': 0.0},
                    'community': {'count': 0, 'percentage': 0.0},
                    'spiritual': {'count': 0, 'percentage': 0.0},
                    'total': 0,
                    'monthly_breakdown': {}
                }
        
        # Get audio segments using AudioSegmentDAO.filter()
        audio_segments_query = AudioSegmentDAO.filter(
            channel=channel_id,
            report_folder_id=report_folder_id,
            start_time=start_dt,
            end_time=end_dt
        )
        
        # Apply shift filtering if shift_id is provided
        if shift_id is not None:
            try:
                shift = Shift.objects.get(id=shift_id, channel_id=channel_id)
                # Get Q object from shift's get_datetime_filter method (for AudioSegments)
                shift_q = shift.get_datetime_filter(utc_start=start_dt, utc_end=end_dt)
                # Apply shift filter directly to AudioSegments query
                audio_segments_query = audio_segments_query.filter(shift_q)
            except Shift.DoesNotExist:
                # If shift doesn't exist, return empty result
                return {
                    'personal': {'count': 0, 'percentage': 0.0},
                    'community': {'count': 0, 'percentage': 0.0},
                    'spiritual': {'count': 0, 'percentage': 0.0},
                    'total': 0,
                    'monthly_breakdown': {}
                }
        
        # Get audio segments - bucket_prompt data is already loaded via select_related in AudioSegmentDAO.filter()
        # Access it safely via: getattr(getattr(segment, 'transcription_detail', None), 'analysis', None).bucket_prompt
        audio_segments = list(audio_segments_query)
        
        # Get audio segments for last 6 months (independent of start_dt/end_dt)
        # Optimize: Get all 6 months in a single query instead of 6 separate queries
        if last_6_months:
            # Find the earliest start and latest end across all 6 months
            earliest_month_start = min(month_start for _, month_start, _ in last_6_months)
            latest_month_end = max(month_end for _, _, month_end in last_6_months)
            
            # Get all audio segments for the entire 6-month period in one query
            monthly_segments_query = AudioSegmentDAO.filter(
                channel=channel_id,
                report_folder_id=report_folder_id,
                start_time=earliest_month_start,
                end_time=latest_month_end
            )
            
            # Apply shift filtering if shift_id is provided
            # Combine shift Q objects for all months with OR (since shift windows may vary by month)
            if shift_id is not None:
                try:
                    shift = Shift.objects.get(id=shift_id, channel_id=channel_id)
                    # Build combined shift Q object for all months
                    combined_shift_q = Q()
                    for month_key, month_start, month_end in last_6_months:
                        month_shift_q = shift.get_datetime_filter(utc_start=month_start, utc_end=month_end)
                        combined_shift_q |= month_shift_q
                    monthly_segments_query = monthly_segments_query.filter(combined_shift_q)
                except Shift.DoesNotExist:
                    # If shift doesn't exist, monthly breakdown will be empty
                    monthly_segments_query = AudioSegmentDAO.filter(
                        channel=channel_id,
                        report_folder_id=report_folder_id,
                        start_time=earliest_month_start,
                        end_time=latest_month_end
                    ).none()  # Return empty queryset
            
            # Get audio segments for the 6-month period - bucket_prompt data is already loaded
            monthly_segments = list(monthly_segments_query)
        else:
            monthly_segments = []
        
        # Initialize category counts for overall
        category_counts = {
            'personal': 0,
            'community': 0,
            'spiritual': 0
        }
        
        # Initialize monthly breakdown structure
        # Format: { 'YYYY-MM': { 'personal': count, 'community': count, 'spiritual': count } }
        monthly_counts = {}
        for month_key, _, _ in last_6_months:
            monthly_counts[month_key] = {
                'personal': 0,
                'community': 0,
                'spiritual': 0
            }
        
        # Process each audio segment for overall counts
        # Bucket prompt data is already loaded via select_related
        for segment in audio_segments:
            # Access bucket_prompt from the already-loaded analysis using getattr for safety
            transcription_detail = getattr(segment, 'transcription_detail', None)
            analysis = getattr(transcription_detail, 'analysis', None) if transcription_detail else None
            if not analysis:
                continue
            
            bucket_text = getattr(analysis, 'bucket_prompt', None)
            if not bucket_text:
                continue
            
            # Extract categories from bucket_prompt
            found_categories = BucketCountService._extract_categories_from_bucket_prompt(
                bucket_text,
                bucket_title_to_category
            )
            
            # Count each found category (a segment can belong to multiple categories)
            for category in found_categories:
                if category in category_counts:
                    category_counts[category] += 1
        
        # Process monthly segments - group by month in Python
        # Create a mapping of month_key to (month_start, month_end) for efficient lookup
        month_ranges = {month_key: (month_start, month_end) for month_key, month_start, month_end in last_6_months}
        
        for segment in monthly_segments:
            # Access bucket_prompt from the already-loaded analysis using getattr for safety
            transcription_detail = getattr(segment, 'transcription_detail', None)
            analysis = getattr(transcription_detail, 'analysis', None) if transcription_detail else None
            if not analysis:
                continue
            
            bucket_text = getattr(analysis, 'bucket_prompt', None)
            if not bucket_text:
                continue
            
            # Get the segment's start_time to determine which month it belongs to
            segment_start_time = segment.start_time
            segment_month_key = segment_start_time.strftime('%Y-%m')
            
            # Only process if it's in our last 6 months list
            if segment_month_key not in monthly_counts:
                continue
            
            # Verify the segment is actually within the month's date range
            # (important when using combined shift filters)
            month_start, month_end = month_ranges[segment_month_key]
            if not (month_start <= segment_start_time <= month_end):
                continue
            
            # Extract categories from bucket_prompt
            found_categories = BucketCountService._extract_categories_from_bucket_prompt(
                bucket_text,
                bucket_title_to_category
            )
            
            # Count each found category for this month
            for category in found_categories:
                if category in monthly_counts[segment_month_key]:
                    monthly_counts[segment_month_key][category] += 1
        
        # Calculate total and percentages for overall
        total = sum(category_counts.values())
        
        # Build response with count and percentage grouped by category
        result = {}
        for category, count in category_counts.items():
            if total > 0:
                percentage = round((count / total) * 100, 2)
            else:
                percentage = 0.0
            
            result[category] = {
                'count': count,
                'percentage': percentage
            }
        
        # Build monthly breakdown for last 6 months
        monthly_breakdown = {}
        for month_key, _, _ in last_6_months:
            month_data = monthly_counts[month_key]
            month_total = sum(month_data.values())
            
            month_result = {}
            for category, count in month_data.items():
                if month_total > 0:
                    percentage = round((count / month_total) * 100, 2)
                else:
                    percentage = 0.0
                month_result[category] = {
                    'count': count,
                    'percentage': percentage
                }
            
            monthly_breakdown[month_key] = {
                **month_result,
                'total': month_total
            }
        
        return {
            **result,
            'total': total,
            'monthly_breakdown': monthly_breakdown
        }

    @staticmethod
    def _extract_bucket_titles_for_category(
        bucket_text: str,
        category_name: str,
        bucket_title_to_category: Dict[str, str]
    ) -> List[str]:
        """
        Extract bucket titles from bucket_prompt that belong to the specified category.
        
        Args:
            bucket_text: The bucket_prompt text from TranscriptionAnalysis
            category_name: The category to filter by (personal, community, spiritual)
            bucket_title_to_category: Mapping from bucket title to category
        
        Returns:
            List of bucket titles that belong to the specified category
        """
        found_buckets = []
        
        if not bucket_text:
            return found_buckets
        
        # Parse bucket_prompt to extract bucket names
        lines = [l for l in str(bucket_text).split('\n') if l.strip()]
        if not lines:
            lines = [str(bucket_text)]
        
        for line in lines:
            primary, secondary = BucketCountService._parse_bucket_prompt_line(line)
            
            # Check each bucket name to see if it belongs to the specified category
            for bucket_name in [primary, secondary]:
                if not bucket_name:
                    continue
                
                # Normalize bucket name
                bucket_name_normalized = ' '.join(bucket_name.upper().split())
                
                # Check if this bucket belongs to the specified category
                if bucket_name_normalized in bucket_title_to_category:
                    if bucket_title_to_category[bucket_name_normalized] == category_name:
                        found_buckets.append(bucket_name_normalized)
                else:
                    # Try fuzzy matching
                    for title, cat in bucket_title_to_category.items():
                        title_normalized = ' '.join(title.split())
                        if (bucket_name_normalized == title_normalized or 
                            bucket_name_normalized in title_normalized or 
                            title_normalized in bucket_name_normalized):
                            if cat == category_name:
                                found_buckets.append(title_normalized)
                            break
        
        return found_buckets

    @staticmethod
    def get_category_bucket_counts(
        start_dt: datetime,
        end_dt: datetime,
        category_name: str,
        channel_id: Optional[int] = None,
        shift_id: Optional[int] = None,
        report_folder_id: Optional[int] = None
    ) -> Dict[str, any]:
        """
        Get count and percentage of each bucket within a specific category.
        
        Args:
            start_dt: Start datetime (timezone-aware)
            end_dt: End datetime (timezone-aware)
            category_name: Category to filter by (personal, community, spiritual)
            channel_id: Channel ID to filter by (required if report_folder_id not provided)
            shift_id: Optional shift ID to filter by
            report_folder_id: Report folder ID to filter by (required if channel_id not provided)
        
        Returns:
            Dictionary containing:
            - buckets: Dictionary mapping bucket title to {count, percentage}
            - total: Total count of buckets in the specified category
            - category: The category name
        """
        # Validate category name using WellnessBucket.CATEGORY_CHOICES
        valid_categories = [choice[0] for choice in WellnessBucket.CATEGORY_CHOICES]
        if category_name not in valid_categories:
            raise ValueError(f'category_name must be one of: {", ".join(valid_categories)}')
        
        # Handle report folder case - get channel_id from folder
        if report_folder_id is not None:
            try:
                report_folder = ReportFolder.objects.select_related('channel').get(id=report_folder_id)
                channel_id = report_folder.channel.id
            except ReportFolder.DoesNotExist:
                # If report folder doesn't exist, return empty result
                return {
                    'buckets': {},
                    'total': 0,
                    'category': category_name,
                    'total_time_period_seconds': (end_dt - start_dt).total_seconds(),
                    'total_time_period_hours': round((end_dt - start_dt).total_seconds() / 3600, 2),
                    'total_filtered_duration_seconds': 0,
                    'total_filtered_duration_hours': 0
                }
        
        # Check if channel exists and is active
        try:
            channel = Channel.objects.get(id=channel_id, is_active=True, is_deleted=False)
        except Channel.DoesNotExist:
            # If channel doesn't exist or is not active, return empty result
            return {
                'buckets': {},
                'total': 0,
                'category': category_name,
                'total_time_period_seconds': (end_dt - start_dt).total_seconds(),
                'total_time_period_hours': round((end_dt - start_dt).total_seconds() / 3600, 2),
                'total_filtered_duration_seconds': 0,
                'total_filtered_duration_hours': 0
            }
        
        # Get bucket title to category mapping
        bucket_title_to_category = BucketCountService._get_bucket_title_to_category_mapping()
        
        # Get all buckets that belong to this category
        category_buckets = {
            title: cat for title, cat in bucket_title_to_category.items()
            if cat == category_name
        }
        
        # Get audio segments using AudioSegmentDAO.filter()
        audio_segments_query = AudioSegmentDAO.filter(
            channel=channel_id,
            report_folder_id=report_folder_id,
            start_time=start_dt,
            end_time=end_dt
        )
        
        # Apply shift filtering if shift_id is provided
        if shift_id is not None:
            try:
                shift = Shift.objects.get(id=shift_id, channel_id=channel_id)
                # Get Q object from shift's get_datetime_filter method (for AudioSegments)
                shift_q = shift.get_datetime_filter(utc_start=start_dt, utc_end=end_dt)
                # Apply shift filter directly to AudioSegments query
                audio_segments_query = audio_segments_query.filter(shift_q)
            except Shift.DoesNotExist:
                # If shift doesn't exist, return empty result
                return {
                    'buckets': {},
                    'total': 0,
                    'category': category_name,
                    'total_time_period_seconds': (end_dt - start_dt).total_seconds(),
                    'total_time_period_hours': round((end_dt - start_dt).total_seconds() / 3600, 2),
                    'total_filtered_duration_seconds': 0,
                    'total_filtered_duration_hours': 0
                }
        
        # Get audio segments - bucket_prompt data is already loaded via select_related in AudioSegmentDAO.filter()
        audio_segments = list(audio_segments_query)

        # Initialize bucket counts and durations for this category
        bucket_counts = {bucket_title: 0 for bucket_title in category_buckets.keys()}
        bucket_durations = {bucket_title: 0 for bucket_title in category_buckets.keys()}
        
        # Track total filtered duration
        total_filtered_duration = 0
        
        # Process each audio segment
        for segment in audio_segments:
            # Access bucket_prompt from the already-loaded analysis using getattr for safety
            transcription_detail = getattr(segment, 'transcription_detail', None)
            analysis = getattr(transcription_detail, 'analysis', None) if transcription_detail else None
            if not analysis:
                continue
            
            bucket_text = getattr(analysis, 'bucket_prompt', None)
            if not bucket_text:
                continue
            
            # Get duration from audio segment (already loaded)
            duration_seconds = segment.duration_seconds if segment.duration_seconds else 0
            total_filtered_duration += duration_seconds
            
            # Extract bucket titles that belong to the specified category
            found_buckets = BucketCountService._extract_bucket_titles_for_category(
                bucket_text,
                category_name,
                bucket_title_to_category
            )
            
            # Count and accumulate duration for each found bucket
            for bucket_title in found_buckets:
                if bucket_title in bucket_counts:
                    bucket_counts[bucket_title] += 1
                    bucket_durations[bucket_title] += duration_seconds
        
        # Calculate total time period (end_dt - start_dt) in seconds
        total_time_period_seconds = (end_dt - start_dt).total_seconds()
        total_time_period_hours = round(total_time_period_seconds / 3600, 2)
        
        # Calculate total filtered duration in hours
        total_filtered_duration_hours = round(total_filtered_duration / 3600, 2)
        
        # Calculate total and percentages
        total = sum(bucket_counts.values())
        
        # Build response with count, percentage, and duration for each bucket
        result = {}
        for bucket_title in category_buckets.keys():
            count = bucket_counts[bucket_title]
            duration_seconds = bucket_durations[bucket_title]
            duration_hours = round(duration_seconds / 3600, 2)
            
            if total > 0:
                percentage = round((count / total) * 100, 2)
            else:
                percentage = 0.0
            
            # Calculate Content Time percentage: (bucket duration / total filtered duration) * 100
            if total_filtered_duration > 0:
                content_time_percentage = round((duration_seconds / total_filtered_duration) * 100, 2)
            else:
                content_time_percentage = 0.0
            
            result[bucket_title] = {
                'count': count,
                'percentage': percentage,
                'duration_seconds': duration_seconds,
                'duration_hours': duration_hours,
                'content_time_percentage': content_time_percentage
            }
        
        return {
            'buckets': result,
            'total': total,
            'category': category_name,
            'total_time_period_seconds': total_time_period_seconds,
            'total_time_period_hours': total_time_period_hours,
            'total_filtered_duration_seconds': total_filtered_duration,
            'total_filtered_duration_hours': total_filtered_duration_hours
        }


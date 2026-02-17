from typing import Dict, Set, Tuple, Optional, List
from datetime import datetime, timedelta
from django.db.models import Q
from django.utils import timezone

# Import your actual models
from data_analysis.models import ReportFolder
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
        
        topics = []
        i = 0
        while i < len(parts) and len(topics) < 2:
            token = parts[i]
            if token == "":
                i += 1
                continue
            if is_score(token):
                i += 1
                continue
            if not is_undefined(token):
                topics.append(token)
            
            if i + 1 < len(parts) and is_score(parts[i+1]):
                i += 2
            else:
                i += 1
        
        primary = topics[0] if len(topics) > 0 else None
        secondary = topics[1] if len(topics) > 1 else None
        return primary, secondary

    @staticmethod
    def _get_bucket_title_to_category_mapping() -> Dict[str, str]:
        """
        Get mapping from WellnessBucket title (uppercase) to category.
        Optimized to use values_list for lighter DB load.
        """
        # Optimization: Fetch only title and category columns
        rows = WellnessBucket.objects.filter(
            general_setting__is_active=True,
            is_deleted=False
        ).values_list('title', 'category')
        
        return {title.upper(): category for title, category in rows}

    @staticmethod
    def _map_bucket_name_to_categories(
        bucket_name: str,
        bucket_title_to_category: Dict[str, str]
    ) -> Set[str]:
        found_categories = set()
        if not bucket_name:
            return found_categories
        
        bucket_name_normalized = ' '.join(bucket_name.upper().split())
        
        if bucket_name_normalized in bucket_title_to_category:
            found_categories.add(bucket_title_to_category[bucket_name_normalized])
        else:
            for title, category in bucket_title_to_category.items():
                title_normalized = ' '.join(title.split())
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
        found_categories = set()
        if not bucket_text:
            return found_categories
        
        lines = [l for l in str(bucket_text).split('\n') if l.strip()]
        if not lines:
            lines = [str(bucket_text)]
        
        for line in lines:
            primary, secondary = BucketCountService._parse_bucket_prompt_line(line)
            for bucket_name in [primary, secondary]:
                categories = BucketCountService._map_bucket_name_to_categories(
                    bucket_name,
                    bucket_title_to_category
                )
                found_categories.update(categories)
        return found_categories

    @staticmethod
    def _get_last_6_months() -> List[Tuple[str, datetime, datetime]]:
        current_time = timezone.now()
        months = []
        for i in range(1, 7):
            target_month = current_time.month - i
            if target_month > 0:
                year = current_time.year
                month = target_month
            else:
                year = current_time.year - 1
                month = 12 + target_month
            
            month_start = datetime(year, month, 1, 0, 0, 0, 0, tzinfo=current_time.tzinfo)
            
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
        
        if channel_id is None and report_folder_id is None:
            raise ValueError("Either channel_id or report_folder_id must be provided")

        bucket_title_to_category = BucketCountService._get_bucket_title_to_category_mapping()
        last_6_months = BucketCountService._get_last_6_months()
        
        # Optimization: Get only the ID, don't load the whole ReportFolder object
        if report_folder_id is not None:
            try:
                channel_id = ReportFolder.objects.values_list('channel_id', flat=True).get(id=report_folder_id)
            except ReportFolder.DoesNotExist:
                return BucketCountService._get_empty_result()

        # --- 1. OVERALL COUNTS ---
        audio_segments_query = AudioSegmentDAO.filter(
            channel=channel_id,
            report_folder_id=report_folder_id,
            start_time=start_dt,
            end_time=end_dt
        )
        
        if shift_id is not None:
            try:
                shift = Shift.objects.get(id=shift_id, channel_id=channel_id)
                shift_q = shift.get_datetime_filter(utc_start=start_dt, utc_end=end_dt)
                audio_segments_query = audio_segments_query.filter(shift_q)
            except Shift.DoesNotExist:
                return BucketCountService._get_empty_result()
        
        # MEMORY FIX 1: Use .values() to fetch only the text, not full models
        # MEMORY FIX 2: Use .iterator() to stream results
        main_iterator = audio_segments_query.values(
            'transcription_detail__analysis__bucket_prompt'
        ).iterator(chunk_size=2000)

        category_counts = {'personal': 0, 'community': 0, 'spiritual': 0}
        
        for entry in main_iterator:
            bucket_text = entry.get('transcription_detail__analysis__bucket_prompt')
            found_categories = BucketCountService._extract_categories_from_bucket_prompt(
                bucket_text,
                bucket_title_to_category
            )
            for category in found_categories:
                if category in category_counts:
                    category_counts[category] += 1
        
        # --- 2. MONTHLY BREAKDOWN ---
        monthly_breakdown = {}
        
        if last_6_months:
            earliest_month_start = min(m[1] for m in last_6_months)
            latest_month_end = max(m[2] for m in last_6_months)
            
            monthly_query = AudioSegmentDAO.filter(
                channel=channel_id,
                report_folder_id=report_folder_id,
                start_time=earliest_month_start,
                end_time=latest_month_end
            )
            
            if shift_id is not None:
                try:
                    # Shift object is already fetched above if provided
                    combined_shift_q = Q()
                    for _, m_start, m_end in last_6_months:
                        combined_shift_q |= shift.get_datetime_filter(utc_start=m_start, utc_end=m_end)
                    monthly_query = monthly_query.filter(combined_shift_q)
                except Exception:
                    monthly_query = monthly_query.none()
            
            # MEMORY FIX: Stream monthly data, fetch only start_time and prompt
            monthly_iterator = monthly_query.values(
                'start_time',
                'transcription_detail__analysis__bucket_prompt'
            ).iterator(chunk_size=2000)

            # Prepare buckets
            monthly_counts = {k: {'personal': 0, 'community': 0, 'spiritual': 0} for k, _, _ in last_6_months}
            month_ranges = {k: (s, e) for k, s, e in last_6_months}
            
            for entry in monthly_iterator:
                bucket_text = entry.get('transcription_detail__analysis__bucket_prompt')
                if not bucket_text:
                    continue
                
                segment_start = entry['start_time']
                segment_month_key = segment_start.strftime('%Y-%m')
                
                if segment_month_key not in monthly_counts:
                    continue
                
                # Check specific range (crucial for shifts)
                m_start, m_end = month_ranges[segment_month_key]
                if not (m_start <= segment_start <= m_end):
                    continue
                
                found_categories = BucketCountService._extract_categories_from_bucket_prompt(
                    bucket_text,
                    bucket_title_to_category
                )
                
                for category in found_categories:
                    if category in monthly_counts[segment_month_key]:
                        monthly_counts[segment_month_key][category] += 1
            
            # Build breakdown structure
            for m_key, _, _ in last_6_months:
                m_data = monthly_counts[m_key]
                m_total = sum(m_data.values())
                monthly_breakdown[m_key] = {
                    'personal': {'count': m_data['personal'], 'percentage': BucketCountService._calc_pct(m_data['personal'], m_total)},
                    'community': {'count': m_data['community'], 'percentage': BucketCountService._calc_pct(m_data['community'], m_total)},
                    'spiritual': {'count': m_data['spiritual'], 'percentage': BucketCountService._calc_pct(m_data['spiritual'], m_total)},
                    'total': m_total
                }

        total = sum(category_counts.values())
        
        return {
            'personal': {'count': category_counts['personal'], 'percentage': BucketCountService._calc_pct(category_counts['personal'], total)},
            'community': {'count': category_counts['community'], 'percentage': BucketCountService._calc_pct(category_counts['community'], total)},
            'spiritual': {'count': category_counts['spiritual'], 'percentage': BucketCountService._calc_pct(category_counts['spiritual'], total)},
            'total': total,
            'monthly_breakdown': monthly_breakdown
        }

    @staticmethod
    def _extract_bucket_titles_for_category(
        bucket_text: str,
        category_name: str,
        bucket_title_to_category: Dict[str, str]
    ) -> List[str]:
        found_buckets = []
        if not bucket_text:
            return found_buckets
        
        lines = [l for l in str(bucket_text).split('\n') if l.strip()]
        if not lines:
            lines = [str(bucket_text)]
        
        for line in lines:
            primary, secondary = BucketCountService._parse_bucket_prompt_line(line)
            for bucket_name in [primary, secondary]:
                if not bucket_name:
                    continue
                
                bucket_name_normalized = ' '.join(bucket_name.upper().split())
                
                if bucket_name_normalized in bucket_title_to_category:
                    if bucket_title_to_category[bucket_name_normalized] == category_name:
                        found_buckets.append(bucket_name_normalized)
                else:
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
        
        valid_categories = [choice[0] for choice in WellnessBucket.CATEGORY_CHOICES]
        if category_name not in valid_categories:
            raise ValueError(f'category_name must be one of: {", ".join(valid_categories)}')
        
        # Retrieve Channel ID efficiently
        if report_folder_id is not None:
            try:
                channel_id = ReportFolder.objects.values_list('channel_id', flat=True).get(id=report_folder_id)
            except ReportFolder.DoesNotExist:
                return BucketCountService._get_empty_category_result(category_name, start_dt, end_dt)
        
        # Verify Channel Active (Optimized to exists())
        if not Channel.objects.filter(id=channel_id, is_active=True, is_deleted=False).exists():
            return BucketCountService._get_empty_category_result(category_name, start_dt, end_dt)
        
        bucket_title_to_category = BucketCountService._get_bucket_title_to_category_mapping()
        
        category_buckets = {
            title: cat for title, cat in bucket_title_to_category.items()
            if cat == category_name
        }
        
        audio_segments_query = AudioSegmentDAO.filter(
            channel=channel_id,
            report_folder_id=report_folder_id,
            start_time=start_dt,
            end_time=end_dt
        )
        
        if shift_id is not None:
            try:
                shift = Shift.objects.get(id=shift_id, channel_id=channel_id)
                shift_q = shift.get_datetime_filter(utc_start=start_dt, utc_end=end_dt)
                audio_segments_query = audio_segments_query.filter(shift_q)
            except Shift.DoesNotExist:
                return BucketCountService._get_empty_category_result(category_name, start_dt, end_dt)
        
        # MEMORY FIX: Stream results, select only bucket_prompt and duration
        data_iterator = audio_segments_query.values(
            'transcription_detail__analysis__bucket_prompt',
            'duration_seconds'
        ).iterator(chunk_size=2000)

        bucket_counts = {bucket_title: 0 for bucket_title in category_buckets.keys()}
        bucket_durations = {bucket_title: 0 for bucket_title in category_buckets.keys()}
        total_filtered_duration = 0
        
        for entry in data_iterator:
            bucket_text = entry.get('transcription_detail__analysis__bucket_prompt')
            if not bucket_text:
                continue
            
            # Use 0 if None
            duration_seconds = entry.get('duration_seconds') or 0
            total_filtered_duration += duration_seconds
            
            found_buckets = BucketCountService._extract_bucket_titles_for_category(
                bucket_text,
                category_name,
                bucket_title_to_category
            )
            
            for bucket_title in found_buckets:
                if bucket_title in bucket_counts:
                    bucket_counts[bucket_title] += 1
                    bucket_durations[bucket_title] += duration_seconds
        
        # Calculate totals
        total_time_period_seconds = (end_dt - start_dt).total_seconds()
        total = sum(bucket_counts.values())
        
        result = {}
        for bucket_title in category_buckets.keys():
            count = bucket_counts[bucket_title]
            duration_sec = bucket_durations[bucket_title]
            
            result[bucket_title] = {
                'count': count,
                'percentage': BucketCountService._calc_pct(count, total),
                'duration_seconds': duration_sec,
                'duration_hours': round(duration_sec / 3600, 2),
                'content_time_percentage': BucketCountService._calc_pct(duration_sec, total_filtered_duration)
            }
        
        return {
            'buckets': result,
            'total': total,
            'category': category_name,
            'total_time_period_seconds': total_time_period_seconds,
            'total_time_period_hours': round(total_time_period_seconds / 3600, 2),
            'total_filtered_duration_seconds': total_filtered_duration,
            'total_filtered_duration_hours': round(total_filtered_duration / 3600, 2)
        }

    # --- Helper Methods for Cleaner Code ---

    @staticmethod
    def _calc_pct(part, total):
        return round((part / total) * 100, 2) if total > 0 else 0.0

    @staticmethod
    def _get_empty_result():
        return {
            'personal': {'count': 0, 'percentage': 0.0},
            'community': {'count': 0, 'percentage': 0.0},
            'spiritual': {'count': 0, 'percentage': 0.0},
            'total': 0,
            'monthly_breakdown': {}
        }
    
    @staticmethod
    def _get_empty_category_result(category_name, start_dt, end_dt):
        return {
            'buckets': {},
            'total': 0,
            'category': category_name,
            'total_time_period_seconds': (end_dt - start_dt).total_seconds(),
            'total_time_period_hours': round((end_dt - start_dt).total_seconds() / 3600, 2),
            'total_filtered_duration_seconds': 0,
            'total_filtered_duration_hours': 0
        }
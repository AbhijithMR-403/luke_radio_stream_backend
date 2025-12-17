from typing import List, Dict, Optional
from datetime import datetime
from django.db.models import Q
import re
from collections import Counter

from data_analysis.models import TranscriptionDetail
from shift_analysis.models import Shift


class WordCountService:
    """
    Service class for counting word occurrences in transcriptions
    """
    
    # Common stop words to ignore when counting
    STOP_WORDS = {
        'a', 'an', 'and', 'are', 'as', 'at', 'be', 'been', 'by', 'for', 'from', "it's",
        'has', 'he', 'in', 'is', 'it', 'its', 'just', 'like', 'of', 'on', 'or', "yeah",
        'that', 'the', 'this', 'to', 'was', 'we', 'were', 'what', 'when', 'where', "one",
        'which', 'who', 'will', 'with', 'would', 'you', 'your', 'yours', 'yourself', "if",
        'yourselves', 'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', "oh", "uh", "i'm",
        'they', 'them', 'their', 'theirs', 'themselves', 'she', 'her', 'hers', "we're", "us", "i've"
        'herself', 'he', 'him', 'his', 'himself', 'it', 'its', 'itself', 'have', "you've",
        'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'am', 'is', 'are',
        'was', 'were', 'been', 'being', 'can', 'could', 'may', 'might', 'must', "we've", 
        'shall', 'should', 'will', 'would', 'about', 'above', 'across', 'after', "they're",
        'against', 'along', 'among', 'around', 'before', 'behind', 'below', 'beneath', "that's",
        'beside', 'between', 'beyond', 'but', 'by', 'concerning', 'considering',
        'despite', 'down', 'during', 'except', 'for', 'from', 'in', 'inside', 'into',
        'like', 'near', 'of', 'off', 'on', 'onto', 'out', 'outside', 'over', 'past',
        'regarding', 'round', 'since', 'through', 'throughout', 'till', 'to', 'toward',
        'under', 'underneath', 'until', 'unto', 'up', 'upon', 'with', 'within',
        'without', 'all', 'both', 'each', 'few', 'more', 'most', 'other', 'some',
        'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too',
        'very', 's', 't', 'can', 'will', 'just', 'don', 'should', 'now', 'd', 'll',
        'm', 'o', 're', 've', 'y', 'ain', 'aren', 'couldn', 'didn', 'doesn', 'hadn',
        'hasn', 'haven', 'isn', 'ma', 'mightn', 'mustn', 'needn', 'shan', 'shouldn',
        'wasn', 'weren', 'won', 'wouldn'
    }

    @staticmethod
    def _convert_shift_q_for_transcription_detail(q_obj: Q) -> Q:
        """
        Convert a Q object designed for AudioSegments to work with TranscriptionDetail.
        Prefixes field names with 'audio_segment__'

        Args:
            q_obj: Q object with AudioSegments field names

        Returns:
            Q object with prefixed field names for TranscriptionDetail
        """
        if not q_obj.children:
            return q_obj

        # Build a new Q object with converted field names
        converted_children = []
        for child in q_obj.children:
            if isinstance(child, Q):
                # Recursively process nested Q objects
                converted_children.append(WordCountService._convert_shift_q_for_transcription_detail(child))
            elif isinstance(child, tuple):
                # Convert field name: ('start_time__lt', value) -> ('audio_segment__start_time__lt', value)
                field_name, value = child
                new_field_name = f'audio_segment__{field_name}'
                converted_children.append((new_field_name, value))
            else:
                converted_children.append(child)

        # Create new Q object with converted children and same connector
        return Q(*converted_children, _connector=q_obj.connector, _negated=q_obj.negated)

    @staticmethod
    def get_transcriptions_for_word_count(
        start_dt: datetime,
        end_dt: datetime,
        channel_id: int,
        shift_id: Optional[int] = None
    ) -> List[TranscriptionDetail]:
        """
        Get TranscriptionDetail records filtered by date range, channel, and optional shift.
        
        Args:
            start_dt: Start datetime (timezone-aware)
            end_dt: End datetime (timezone-aware)
            channel_id: Channel ID to filter by
            shift_id: Optional shift ID to filter by
        
        Returns:
            QuerySet of TranscriptionDetail records
        """
        # Build Q object for filtering by date range and channel
        base_q = Q(
            audio_segment__start_time__gte=start_dt,
            audio_segment__start_time__lte=end_dt,
            audio_segment__channel_id=channel_id
        )
        
        # Apply shift filtering if shift_id is provided
        if shift_id is not None:
            try:
                shift = Shift.objects.get(id=shift_id, channel_id=channel_id)
                # Get Q object from shift's get_datetime_filter method
                shift_q = shift.get_datetime_filter(utc_start=start_dt, utc_end=end_dt)
                # Convert Q object to work with TranscriptionDetail by prefixing field paths
                shift_q_modified = WordCountService._convert_shift_q_for_transcription_detail(shift_q)
                # Combine with base query
                base_q = base_q & shift_q_modified
            except Shift.DoesNotExist:
                # If shift doesn't exist, return empty queryset
                return TranscriptionDetail.objects.none()
        
        # Get all TranscriptionDetail records with related data
        transcriptions = TranscriptionDetail.objects.filter(
            base_q
        ).select_related(
            'audio_segment',
            'audio_segment__channel'
        ).order_by('audio_segment__start_time')
        
        return transcriptions

    @staticmethod
    def extract_words_from_text(text: str) -> List[str]:
        """
        Extract words from text, ignoring newline characters, numbers, and stop words.
        
        Args:
            text: Text to extract words from
        
        Returns:
            List of words (lowercased, filtered)
        """
        if not text:
            return []
        
        # Replace newline characters with spaces
        text = text.replace('\n', ' ')
        text = text.replace('\r', ' ')
        
        # Use regex to extract words (alphanumeric characters and apostrophes)
        # This handles contractions like "don't", "it's", etc.
        words = re.findall(r"\b[a-zA-Z0-9']+\b", text)
        
        # Convert to lowercase for consistent counting
        words = [word.lower() for word in words]
        
        # Filter out numbers (words that are purely numeric) and stop words
        filtered_words = []
        for word in words:
            # Skip if word is purely numeric
            if word.isdigit():
                continue
            # Skip if word is a stop word
            if word in WordCountService.STOP_WORDS:
                continue
            # Skip if word contains only numbers and apostrophes (like '123')
            if re.match(r"^[\d']+$", word):
                continue
            filtered_words.append(word)
        
        return filtered_words

    @staticmethod
    def count_words(transcriptions: List[TranscriptionDetail]) -> Dict[str, int]:
        """
        Count word occurrences across all transcriptions.
        
        Args:
            transcriptions: List or QuerySet of TranscriptionDetail records
        
        Returns:
            Dictionary mapping words to their counts, sorted by count (descending)
        """
        word_counter = Counter()
        
        for transcription in transcriptions:
            if transcription.transcript:
                words = WordCountService.extract_words_from_text(transcription.transcript)
                word_counter.update(words)
        
        # Convert to regular dict and sort by count (descending)
        word_counts = dict(word_counter)
        
        # Sort by count descending, then by word ascending for consistent ordering
        sorted_word_counts = dict(
            sorted(word_counts.items(), key=lambda x: (-x[1], x[0]))
        )
        
        return sorted_word_counts

    @staticmethod
    def get_word_counts(
        start_dt: datetime,
        end_dt: datetime,
        channel_id: int,
        shift_id: Optional[int] = None
    ) -> Dict[str, any]:
        """
        Get word counts from transcriptions filtered by date range, channel, and optional shift.
        
        Args:
            start_dt: Start datetime (timezone-aware)
            end_dt: End datetime (timezone-aware)
            channel_id: Channel ID to filter by
            shift_id: Optional shift ID to filter by
        
        Returns:
            Dictionary containing:
            - 'word_counts': Dictionary mapping words to their counts
            - 'total_words': Total number of words found
            - 'unique_words': Number of unique words
        """
        # Get transcriptions
        transcriptions = WordCountService.get_transcriptions_for_word_count(
            start_dt=start_dt,
            end_dt=end_dt,
            channel_id=channel_id,
            shift_id=shift_id
        )
        
        # Count words
        word_counts = WordCountService.count_words(transcriptions)
        
        # Calculate statistics
        total_words = sum(word_counts.values())
        unique_words = len(word_counts)
        
        return {
            'word_counts': word_counts,
            'total_words': total_words,
            'unique_words': unique_words
        }

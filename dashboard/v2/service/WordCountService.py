from typing import List, Dict, Optional, Iterator
from datetime import datetime
from django.db.models import Q
from collections import Counter
import re
import nltk
from nltk.corpus import stopwords

from data_analysis.models import TranscriptionDetail
from shift_analysis.models import Shift

# Regex pattern for extracting words (3+ alphabetic characters)
WORD_RE = re.compile(r"[a-zA-Z]{4,}")

# Download required NLTK resources if not already downloaded (idempotent)
# Each resource is checked and downloaded separately to prevent LookupError in minimal environments
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords', quiet=True)


class WordCountService:
    """
    Service class for counting word occurrences in transcriptions
    """
    
    # Low-information words to filter out
    LOW_INFORMATION_WORDS = {
        'also', 'still', 'another', 'else', 'since', 'yet', 'instead',
        'based', 'recently', 'personally', 'alone', 'along', 'across',
        'toward', 'started', 'giving', 'gets', 'tried', 'taken', 'played',
        'helped', 'helping', 'released', 'joining', 'looked', 'seeing',
        'learned', 'realize', 'happened', 'moved', 'sitting', 'flying', 'going', 'need', 'said',
        'know', 'think', 'would', 'could', 'look', 'says',
        'even', 'maybe', 'kind', 'cause', 'thank', 'ever', 'mean',
        'something', 'everything', 'someone', 'anything', 'nothing',
        'stuff', 'sort', 'probably', 'though', 'whatever'
    }
    
    # Use NLTK's comprehensive English stop words, plus additional filters for transcriptions
    STOP_WORDS = set(stopwords.words('english'))
    # Add transcription artifacts
    STOP_WORDS.update({
        'speaker', 'inaudible', 'crosstalk', 'silence', 'applause', 'music', 
        'laugh', 'laughter', 'unknown', 'unidentified'
    })
    # Add conversational fillers and common spoken language patterns
    STOP_WORDS.update({
        'yeah', 'oh', 'uh', 'um', 'hmm', 'ah', 'er', 'well', 'like', 'you know',
        'okay', 'right', 'really', 'actually', 'basically', 'gonna', 'wanna', 'gotta', 'dunno'
    })
    # Add common contractions
    STOP_WORDS.update({
        "i'm", "you're", "we're", "they're", "it's", "that's", "i've", "you've", "we've",
        "don't", "won't", "can't", "wouldn't", "shouldn't", "couldn't", "isn't", "aren't",
        "wasn't", "weren't", "hasn't", "haven't", "hadn't", "doesn't", "didn't"
    })
    # Add low-information words
    STOP_WORDS.update(LOW_INFORMATION_WORDS)

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
    def get_transcription_texts(
        start_dt: datetime,
        end_dt: datetime,
        channel_id: int = None,
        report_folder_id: Optional[int] = None,
        shift_id: Optional[int] = None
    ) -> Iterator[str]:
        """
        Get transcription text strings filtered by date range, channel, and optional shift.
        Returns an iterator for memory-efficient processing.
        
        Args:
            start_dt: Start datetime (timezone-aware)
            end_dt: End datetime (timezone-aware)
            channel_id: Channel ID to filter by (required if report_folder_id not provided)
            report_folder_id: Report folder ID to filter by (required if channel_id not provided)
            shift_id: Optional shift ID to filter by
        
        Returns:
            Iterator of transcription text strings
        """
        # Validate filter inputs
        if channel_id is None and report_folder_id is None:
            raise ValueError("Either channel_id or report_folder_id must be provided")
        
        # Handle report folder case - get channel_id from folder
        if report_folder_id is not None:
            from data_analysis.models import ReportFolder
            try:
                report_folder = ReportFolder.objects.select_related('channel').get(id=report_folder_id)
                channel_id = report_folder.channel.id
            except ReportFolder.DoesNotExist:
                # If report folder doesn't exist, return empty iterator
                return iter([])
        
        # Build Q object for filtering by date range and channel
        base_q = Q(
            audio_segment__start_time__gte=start_dt,
            audio_segment__start_time__lte=end_dt,
            audio_segment__channel_id=channel_id,
        )
        
        # Add report_folder_id filter if provided
        if report_folder_id is not None:
            base_q &= Q(audio_segment__saved_in_folders__folder_id=report_folder_id)
        
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
                # If shift doesn't exist, return empty iterator
                return iter([])
        
        # Get transcription texts as an iterator (memory-efficient)
        transcription_texts = TranscriptionDetail.objects.filter(
            base_q
        )
        
        # Use distinct() when report_folder_id is used to avoid duplicates from the join
        if report_folder_id is not None:
            transcription_texts = transcription_texts.distinct()
        
        transcription_texts = transcription_texts.values_list('transcript', flat=True).iterator()
        
        return transcription_texts

    @staticmethod
    def extract_words_from_text(text: str) -> List[str]:
        """
        Extract meaningful words from text using regex tokenization.
        
        Args:
            text: Text to extract words from
        
        Returns:
            List of words (lowercased, filtered)
        """
        if not text:
            return []
        
        # Extract words using regex (already lowercased, 3+ alphabetic characters)
        tokens = WORD_RE.findall(text.lower())
        
        # Filter out stop words
        filtered_words = [word for word in tokens if word not in WordCountService.STOP_WORDS]
        
        return filtered_words

    @staticmethod
    def count_words(transcription_texts: Iterator[str]) -> Dict[str, int]:
        """
        Count word occurrences across all transcription texts.
        Processes transcriptions in batches of 100 for efficiency.
        
        Args:
            transcription_texts: Iterator of transcription text strings
        
        Returns:
            Dictionary mapping words to their counts, sorted by count (descending)
        """
        word_counter = Counter()
        batch = []
        batch_size = 100
        
        for text in transcription_texts:
            if text:
                batch.append(text)
            
            # Process batch when it reaches 10 transcriptions
            if len(batch) >= batch_size:
                # Combine batch texts with space separator
                combined_text = ' '.join(batch)
                words = WordCountService.extract_words_from_text(combined_text)
                word_counter.update(words)
                batch = []  # Reset batch
        
        # Process remaining transcriptions (if any)
        if batch:
            combined_text = ' '.join(batch)
            words = WordCountService.extract_words_from_text(combined_text)
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
        channel_id: int = None,
        report_folder_id: Optional[int] = None,
        shift_id: Optional[int] = None
    ) -> Dict[str, any]:
        """
        Get word counts from transcriptions filtered by date range, channel, and optional shift.
        
        Args:
            start_dt: Start datetime (timezone-aware)
            end_dt: End datetime (timezone-aware)
            channel_id: Channel ID to filter by (required if report_folder_id not provided)
            report_folder_id: Report folder ID to filter by (required if channel_id not provided)
            shift_id: Optional shift ID to filter by
        
        Returns:
            Dictionary containing:
            - 'word_counts': Dictionary mapping words to their counts
            - 'total_words': Total number of words found
            - 'unique_words': Number of unique words
        """
        # Get transcription texts
        transcription_texts = WordCountService.get_transcription_texts(
            start_dt=start_dt,
            end_dt=end_dt,
            channel_id=channel_id,
            report_folder_id=report_folder_id,
            shift_id=shift_id
        )
        
        # Count words
        word_counts = WordCountService.count_words(transcription_texts)

        # Calculate statistics
        total_words = sum(word_counts.values())
        unique_words = len(word_counts)
        
        return {
            'word_counts': word_counts,
            'total_words': total_words,
            'unique_words': unique_words
        }

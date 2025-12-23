from typing import List, Dict, Optional, Iterator
from datetime import datetime
from django.db.models import Q
import re
from collections import Counter
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.tag import pos_tag

from data_analysis.models import TranscriptionDetail
from shift_analysis.models import Shift

# Download required NLTK resources if not already downloaded (idempotent)
# Each resource is checked and downloaded separately to prevent LookupError in minimal environments
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords', quiet=True)

try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', quiet=True)

try:
    nltk.data.find('taggers/averaged_perceptron_tagger')
except LookupError:
    nltk.download('averaged_perceptron_tagger', quiet=True)

try:
    nltk.data.find('taggers/universal_tagset')
except LookupError:
    nltk.download('universal_tagset', quiet=True)


class WordCountService:
    """
    Service class for counting word occurrences in transcriptions
    """
    
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
        channel_id: int,
        shift_id: Optional[int] = None
    ) -> Iterator[str]:
        """
        Get transcription text strings filtered by date range, channel, and optional shift.
        Returns an iterator for memory-efficient processing.
        
        Args:
            start_dt: Start datetime (timezone-aware)
            end_dt: End datetime (timezone-aware)
            channel_id: Channel ID to filter by
            shift_id: Optional shift ID to filter by
        
        Returns:
            Iterator of transcription text strings
        """
        # Build Q object for filtering by date range and channel
        base_q = Q(
            audio_segment__start_time__gte=start_dt,
            audio_segment__start_time__lte=end_dt,
            audio_segment__channel_id=channel_id,
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
                # If shift doesn't exist, return empty iterator
                return iter([])
        
        # Get transcription texts as an iterator (memory-efficient)
        transcription_texts = TranscriptionDetail.objects.filter(
            base_q
        ).values_list('transcript', flat=True).iterator()
        
        return transcription_texts

    @staticmethod
    def extract_words_from_text(text: str) -> List[str]:
        """
        Extract meaningful words from text using NLTK POS tagging.
        Only keeps nouns and adjectives to focus on content words.
        
        Args:
            text: Text to extract words from
        
        Returns:
            List of words (lowercased, filtered to nouns and adjectives only)
        """
        if not text:
            return []
        
        # Tokenize the text using NLTK
        tokens = word_tokenize(text)
        
        # Tag tokens with POS tags using universal tagset
        tagged_tokens = pos_tag(tokens, tagset='universal')
        
        # Filter words: only keep nouns and adjectives
        filtered_words = []
        for word, tag in tagged_tokens:
            # Only keep nouns (NOUN) and adjectives (ADJ)
            if tag not in ('NOUN', 'ADJ'):
                continue
            
            # Convert to lowercase for consistent counting
            word = word.lower()
            
            # Skip words with fewer than 3 characters
            if len(word) < 3:
                continue
            
            # Skip if word is purely numeric
            if word.isdigit():
                continue
            
            # Skip if word is in stop words
            if word in WordCountService.STOP_WORDS:
                continue
            
            # Skip if word contains only numbers and apostrophes (like '123')
            if re.match(r"^[\d']+$", word):
                continue
            
            filtered_words.append(word)
        
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
        # Get transcription texts
        transcription_texts = WordCountService.get_transcription_texts(
            start_dt=start_dt,
            end_dt=end_dt,
            channel_id=channel_id,
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

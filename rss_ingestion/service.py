import feedparser
from datetime import datetime, timedelta, timezone as dt_timezone
from time import mktime
from typing import List, Dict, Any, Optional
from email.utils import parsedate_to_datetime

from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone

from data_analysis.models import AudioSegments
from core_admin.models import Channel


class RSSAudioSegmentInserter:
    """
    Class to insert RSS feed entries into AudioSegments as podcast segments.
    """

    def __init__(self, channel: Channel):
        """
        Initialize the inserter with a channel.
        
        Args:
            channel: The Channel instance to associate segments with.
        """
        self.channel = channel
        self.created_segments: List[AudioSegments] = []
        self.skipped_entries: List[Dict[str, Any]] = []
        self.errors: List[Dict[str, Any]] = []

    def insert_from_entries(self, entries: List[Dict[str, Any]]) -> 'RSSAudioSegmentInserter':
        """
        Insert multiple RSS feed entries as AudioSegments.
        
        Args:
            entries: List of RSS feed entry dictionaries (from feedparser).
        
        Returns:
            self for method chaining.
        """
        for entry in entries:
            try:
                # Each entry gets its own transaction so failures don't affect other entries
                with transaction.atomic():
                    segment = self._create_segment_from_entry(entry)
                    if segment:
                        self.created_segments.append(segment)
            except ValidationError as e:
                self.errors.append({
                    'entry': entry.get('id') or entry.get('link', 'unknown'),
                    'error': str(e)
                })
            except Exception as e:
                self.errors.append({
                    'entry': entry.get('id') or entry.get('link', 'unknown'),
                    'error': str(e)
                })
        
        return self

    def _create_segment_from_entry(self, entry: Dict[str, Any]) -> Optional[AudioSegments]:
        """
        Create an AudioSegment from a single RSS feed entry.
        
        Args:
            entry: RSS feed entry dictionary.
        
        Returns:
            Created AudioSegments instance or None if skipped.
        """
        # Extract GUID (required for podcast segments)
        rss_guid = entry.get('id') or entry.get('guid') or entry.get('link')
        if not rss_guid:
            self.skipped_entries.append({
                'entry': entry.get('title', 'unknown'),
                'reason': 'No GUID/ID found'
            })
            return None

        # Check if segment with this GUID already exists
        existing_segment = AudioSegments.objects.filter(rss_guid=rss_guid).first()
        if existing_segment:
            return existing_segment

        # Extract audio URL from enclosures
        audio_url = self._extract_audio_url(entry)
        if not audio_url:
            self.skipped_entries.append({
                'entry': rss_guid,
                'reason': 'No audio URL found in enclosures'
            })
            return None

        # Extract publication date (required)
        pub_date = self._parse_pub_date(entry)
        if not pub_date:
            self.skipped_entries.append({
                'entry': rss_guid,
                'reason': 'No publication date found'
            })
            return None
        
        # Filter based on channel's rss_start_date
        if self.channel.rss_start_date:
            if pub_date < self.channel.rss_start_date:
                self.skipped_entries.append({
                    'entry': rss_guid,
                    'reason': f'Published before rss_start_date ({pub_date} < {self.channel.rss_start_date})'
                })
                return None
        
        # Extract title
        title = entry.get('title', '')
        if not title:
            self.skipped_entries.append({
                'entry': rss_guid,
                'reason': 'No title found'
            })
            return None

        # Extract duration (if available)
        duration_seconds = self._extract_duration(entry)
        
        # Generate file name from GUID or title
        file_name = self._generate_file_name(rss_guid, title)

        # Create the segment
        segment = AudioSegments(
            segment_type='podcast',
            channel=self.channel,
            rss_guid=rss_guid,
            audio_url=audio_url,
            pub_date=pub_date,
            start_time=pub_date,
            end_time=pub_date + timedelta(seconds=duration_seconds),
            duration_seconds=duration_seconds,
            title=title,
            is_recognized=True,  # Podcasts are considered recognized (they have titles)
            is_active=True,
            file_name=file_name,
            metadata_json=self._build_metadata(entry)
        )
        
        segment.save()
        return segment

    def _extract_audio_url(self, entry: Dict[str, Any]) -> Optional[str]:
        """Extract audio URL from RSS entry enclosures or links."""
        # Check enclosures first (standard for podcasts)
        enclosures = entry.get('enclosures', [])
        for enclosure in enclosures:
            url = enclosure.get('href') or enclosure.get('url')
            enc_type = enclosure.get('type', '')
            if url and ('audio' in enc_type or url.endswith(('.mp3', '.m4a', '.wav', '.ogg', '.aac'))):
                return url

        # Check links as fallback
        links = entry.get('links', [])
        for link in links:
            url = link.get('href') or link.get('url')
            link_type = link.get('type', '')
            if url and ('audio' in link_type or url.endswith(('.mp3', '.m4a', '.wav', '.ogg', '.aac'))):
                return url

        # Check media content (common in some feeds)
        media_content = entry.get('media_content', [])
        for media in media_content:
            url = media.get('url')
            media_type = media.get('type', '')
            if url and ('audio' in media_type or url.endswith(('.mp3', '.m4a', '.wav', '.ogg', '.aac'))):
                return url

        return None

    def _ensure_timezone_aware(self, dt: datetime) -> datetime:
        """Ensure datetime is timezone-aware, converting to UTC if naive."""
        if timezone.is_naive(dt):
            return timezone.make_aware(dt, dt_timezone.utc)
        return dt

    def _parse_pub_date(self, entry: Dict[str, Any]) -> Optional[datetime]:
        """Parse publication date from RSS entry."""
        # Try published_parsed first (feedparser pre-parsed)
        if entry.get('published_parsed'):
            try:
                dt = datetime.fromtimestamp(mktime(entry['published_parsed']), tz=dt_timezone.utc)
                return self._ensure_timezone_aware(dt)
            except (ValueError, TypeError, OverflowError):
                pass

        # Try published string
        if entry.get('published'):
            try:
                dt = parsedate_to_datetime(entry['published'])
                return self._ensure_timezone_aware(dt)
            except (ValueError, TypeError):
                pass

        # Try updated as fallback
        if entry.get('updated_parsed'):
            try:
                dt = datetime.fromtimestamp(mktime(entry['updated_parsed']), tz=dt_timezone.utc)
                return self._ensure_timezone_aware(dt)
            except (ValueError, TypeError, OverflowError):
                pass

        return None

    def _extract_duration(self, entry: Dict[str, Any]) -> int:
        """Extract duration in seconds from RSS entry."""
        # Check itunes:duration
        itunes_duration = entry.get('itunes_duration')
        if itunes_duration:
            return self._parse_duration_string(itunes_duration)

        # Check enclosure length (some feeds include this)
        enclosures = entry.get('enclosures', [])
        for enclosure in enclosures:
            length = enclosure.get('length')
            if length:
                try:
                    # Length is typically file size, not duration
                    # So we skip this unless it's explicitly duration
                    pass
                except (ValueError, TypeError):
                    pass

        # Default duration (will be updated after download/processing)
        return 0

    def _parse_duration_string(self, duration_str: str) -> int:
        """Parse duration string (HH:MM:SS or MM:SS or seconds) to seconds."""
        if not duration_str:
            return 0

        try:
            # If it's just a number, assume seconds
            if duration_str.isdigit():
                return int(duration_str)

            parts = duration_str.split(':')
            if len(parts) == 3:
                # HH:MM:SS
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            elif len(parts) == 2:
                # MM:SS
                return int(parts[0]) * 60 + int(parts[1])
            else:
                return int(float(duration_str))
        except (ValueError, TypeError):
            return 0

    def _generate_file_name(self, rss_guid: str, title: str) -> str:
        """Generate a file name for the audio segment."""
        import re
        import hashlib
        
        # Create a clean version of the title
        clean_title = re.sub(r'[^\w\s-]', '', title)[:50].strip()
        clean_title = re.sub(r'[\s]+', '_', clean_title)
        
        # Add a hash of the GUID for uniqueness
        guid_hash = hashlib.md5(rss_guid.encode()).hexdigest()[:8]
        
        return f"podcast_{clean_title}_{guid_hash}"

    def _build_metadata(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Build metadata JSON from RSS entry."""
        metadata = {
            'source': 'rss_feed',
            # 'summary': entry.get('summary', '')[:500] if entry.get('summary') else None,
            'author': entry.get('author') or entry.get('itunes_author'),
            'image': None,
            'episode_number': entry.get('itunes_episode'),
            'season_number': entry.get('itunes_season'),
        }

        # Extract image
        if entry.get('image'):
            metadata['image'] = entry['image'].get('href') or entry['image'].get('url')
        elif entry.get('itunes_image'):
            metadata['image'] = entry['itunes_image'].get('href')

        # Remove None values
        return {k: v for k, v in metadata.items() if v is not None}

    def get_results(self) -> Dict[str, Any]:
        """
        Get the results of the insertion operation.
        
        Returns:
            Dictionary with created_count, skipped_count, error_count, and details.
        """
        return {
            'created_count': len(self.created_segments),
            'skipped_count': len(self.skipped_entries),
            'error_count': len(self.errors),
            'created_segments': self.created_segments,
            'skipped_entries': self.skipped_entries,
            'errors': self.errors
        }


class RSSIngestionService:

    def __init__(self, url: str):
        self.url = url
        self.entries: List[Dict[str, Any]] = []
    
    def fetch(self) -> 'RSSIngestionService':
        """
        Fetch and parse the RSS feed from the URL.
        """
        feed = feedparser.parse(self.url)
        entries = feed.get('entries', [])
        
        # Validate entries is a list
        if isinstance(entries, list):
            self.entries = entries
        else:
            self.entries = []
        
        return self
    
    def has_entries(self) -> bool:
        return isinstance(self.entries, list) and len(self.entries) > 0

    def insert_to_audio_segments(self, channel: Channel) -> Dict[str, Any]:
        """
        Insert fetched RSS entries into AudioSegments as podcast segments.
        
        Args:
            channel: The Channel instance to associate segments with.
        
        Returns:
            Dictionary with insertion results (created_count, skipped_count, etc.)
        """
        if not self.has_entries():
            return {
                'created_count': 0,
                'skipped_count': 0,
                'error_count': 0,
                'created_segments': [],
                'skipped_entries': [],
                'errors': [{'error': 'No entries to insert. Call fetch() first.'}]
            }
        
        inserter = RSSAudioSegmentInserter(channel)
        inserter.insert_from_entries(self.entries)
        return inserter.get_results()


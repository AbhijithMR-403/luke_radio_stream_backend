from typing import Any, Dict, List, Tuple, DefaultDict, Optional, Set

from collections import defaultdict

from datetime import timedelta, datetime, date, time

from django.utils.dateparse import parse_datetime
from django.utils import timezone

from zoneinfo import ZoneInfo

from segmentor.models import TitleMappingRule
from data_analysis.models import AudioSegments
from shift_analysis.models import Shift


def _safe_parse_datetime(value: Any):
    if not value:
        return None
    dt = parse_datetime(str(value))
    if dt is None:
        return None
    if timezone.is_naive(dt):
        # Assume UTC if naive
        dt = timezone.make_aware(dt, timezone.utc)
    return dt


def _merge_intervals(intervals: List[Tuple[Any, Any]]) -> List[Tuple[Any, Any]]:
    if not intervals:
        return []
    intervals.sort(key=lambda x: x[0])
    merged: List[Tuple[Any, Any]] = []
    current_start, current_end = intervals[0]
    for start, end in intervals[1:]:
        if start <= current_end:
            if end > current_end:
                current_end = end
        else:
            merged.append((current_start, current_end))
            current_start, current_end = start, end
    merged.append((current_start, current_end))
    return merged


def _deactivate_segments_without_analysis(segments: List[Dict[str, Any]]) -> None:
    segments_to_deactivate: Set[int] = set()
    for seg in segments:
        if seg.get("requires_analysis") is not False:
            continue
        seg_id = seg.get("id")
        if seg_id is None:
            continue
        try:
            seg_id_int = int(seg_id)
        except (TypeError, ValueError):
            continue
        segments_to_deactivate.add(seg_id_int)

    if segments_to_deactivate:
        AudioSegments.objects.filter(id__in=segments_to_deactivate, is_active=True).update(is_active=False)


def update_audio_segment_title(segment_id: Any, category_name: Any) -> bool:
    """Update a single AudioSegments.title if possible; returns True if updated.

    Safe-casts id to int and category_name to str; ignores invalid inputs.
    """
    try:
        seg_id_int = int(segment_id)
    except (TypeError, ValueError):
        return False
    if category_name is None:
        return False
    new_title = str(category_name)[:500]
    try:
        obj = AudioSegments.objects.filter(id=seg_id_int).only("id", "title").first()
        if not obj:
            return False
        if obj.title == new_title:
            return False
        obj.title = new_title
        obj.save(update_fields=["title"])
        return True
    except Exception:
        # Avoid raising from utility; caller can ignore failures
        return False


def mark_requires_analysis(
    segments: List[Dict[str, Any]],
    suppression_duration: Optional[timedelta] = timedelta(minutes=10),
) -> List[Dict[str, Any]]:
    """
    Annotate each segment dict with `requires_analysis` based on unrecognized audio rules and shift configurations.

    Rules:
      - When a segment's `title` equals a rule's `before_title`, mark segments from that
        point up to the next segment where `title` equals the rule's `after_title` as
        requires_analysis=False.
      - If the matching `after_title` does not occur within the configured suppression duration from the
        `before_title` segment's `start_time`, only mark up to that duration (default 10 minutes).
      - If a rule has empty `after_title`, only the `before_title` segment is marked.
      - For segments that do NOT fall within any active shift time window (based on start_time, end_time, and days),
        mark as requires_analysis=False. Shift times are processed in the channel's timezone.
      - Default for all other segments: requires_analysis=True.

    Each segment is expected to include: `title`, `start_time`, `end_time`, `channel_id`.
    The function mutates and returns the same list for convenience.
    Args:
        segments: List of segment dicts to annotate.
        suppression_duration: Time delta to cap suppression after `before_title`. Defaults to 10 minutes.

    """

    # Default all to True first, then apply per-segment immediate suppression rules
    for seg in segments:
        seg["requires_analysis"] = True
        # Immediate suppression rules:
        # 1) If already recognized, do not analyze
        if seg.get("is_recognized") is True:
            seg["requires_analysis"] = False
            continue
        # 2) If very short clip (<10 seconds), do not analyze
        duration_val = seg.get("duration_seconds")
        try:
            if duration_val is not None and int(duration_val) < 10:
                seg["requires_analysis"] = False
                continue
        except (TypeError, ValueError):
            # If not parseable, ignore and keep default
            pass

    # Group segments by channel_id and prepare parsed timeline (keep refs to original dicts)
    channel_to_segments: DefaultDict[int, List[Dict[str, Any]]] = defaultdict(list)
    for seg in segments:
        channel_id = seg.get("channel_id")
        if channel_id is None:
            continue
        seg_start = _safe_parse_datetime(seg.get("start_time"))
        seg_end = _safe_parse_datetime(seg.get("end_time"))
        if seg_start is None or seg_end is None:
            # Leave requires_analysis=True; skip timeline processing
            continue
        channel_to_segments[int(channel_id)].append({
            "_ref": seg,
            "title": seg.get("title"),
            "_parsed_start": seg_start,
            "_parsed_end": seg_end,
        })

    if not channel_to_segments:
        _deactivate_segments_without_analysis(segments)
        return segments

    # Compute intervals and handle renaming using TitleMappingRule helpers
    channel_ids = list(channel_to_segments.keys())
    channel_to_intervals = build_suppression_intervals_from_title_rules(
        channel_to_segments,
        channel_ids,
        suppression_duration,
    )
    
    # Check if segments fall within any shift - if not, mark as requires_analysis=False
    check_segments_against_shifts(channel_to_segments, channel_ids)
    
    rename_titles_from_title_rules(channel_to_segments, channel_ids)

    # Apply title rule intervals back to original segments
    for seg in segments:
        channel_id = seg.get("channel_id")
        if channel_id is None:
            continue
        seg_start = _safe_parse_datetime(seg.get("start_time"))
        seg_end = _safe_parse_datetime(seg.get("end_time"))
        if seg_start is None or seg_end is None:
            continue

        intervals = channel_to_intervals.get(int(channel_id), [])
        if not intervals:
            continue

        # Overlap check: [seg_start, seg_end] intersects any interval [s, e]
        for s, e in intervals:
            if seg_start <= e and seg_end > s:
                seg["requires_analysis"] = False
                break

    _deactivate_segments_without_analysis(segments)

    return segments


def get_active_title_rules(channel_ids: List[int]) -> DefaultDict[int, List[TitleMappingRule]]:
    """Fetch active TitleMappingRule objects grouped by channel id."""
    rules = (
        TitleMappingRule.objects
        .filter(is_active=True, category__channel_id__in=channel_ids)
        .select_related("category", "category__channel")
    )
    channel_to_rules: DefaultDict[int, List[TitleMappingRule]] = defaultdict(list)
    for rule in rules:
        channel_to_rules[rule.category.channel_id].append(rule)
    return channel_to_rules


def check_segments_against_shifts(
    channel_to_segments: DefaultDict[int, List[Dict[str, Any]]],
    channel_ids: List[int],
) -> None:
    """
    Check if segments fall within any active shift time window.
    If a segment's time range (start_time, end_time) and day does NOT fall within any shift,
    mark it as requires_analysis=False.
    """
    # Fetch all active shifts for the channels
    shifts = (
        Shift.objects
        .filter(is_active=True, channel__in=channel_ids)
        .select_related("channel")
    )
    
    # Group shifts by channel and also map channel_id to timezone
    channel_to_shifts: DefaultDict[int, List[Shift]] = defaultdict(list)
    channel_id_to_timezone: Dict[int, ZoneInfo] = {}
    
    for shift in shifts:
        channel_to_shifts[shift.channel.id].append(shift)
        # Store timezone for each channel
        if shift.channel.id not in channel_id_to_timezone:
            channel_id_to_timezone[shift.channel.id] = ZoneInfo(shift.channel.timezone or "UTC")
    
    # Import the helper function for building UTC windows
    from shift_analysis.utils import _build_utc_windows_for_local_day
    
    # For each channel, check each segment against shifts
    for channel_id, segs in channel_to_segments.items():
        if not segs:
            continue
        
        # Get channel timezone
        tz = channel_id_to_timezone.get(channel_id)
        if tz is None:
            # If no timezone, mark all segments as requires_analysis=False (no shifts to check)
            for seg_data in segs:
                seg_data["_ref"]["requires_analysis"] = False
            continue
        
        shifts_for_channel = channel_to_shifts.get(channel_id, [])
        if not shifts_for_channel:
            # If no shifts for this channel, mark all segments as requires_analysis=False
            for seg_data in segs:
                seg_data["_ref"]["requires_analysis"] = False
            continue
        
        # Check each segment against all shifts
        for seg_data in segs:
            seg_ref = seg_data["_ref"]
            seg_start = seg_data["_parsed_start"]
            seg_end = seg_data["_parsed_end"]
            
            # Convert segment times to local timezone to get the days
            seg_start_local = seg_start.astimezone(tz)
            seg_end_local = seg_end.astimezone(tz)
            seg_start_day = seg_start_local.date()
            seg_end_day = seg_end_local.date()
            
            # Get all days the segment spans (in case it crosses midnight)
            days_to_check = [seg_start_day]
            if seg_end_day != seg_start_day:
                current_day = seg_start_day + timedelta(days=1)
                while current_day <= seg_end_day:
                    days_to_check.append(current_day)
                    current_day += timedelta(days=1)
            
            # Check if segment overlaps with any shift window
            segment_in_shift = False
            
            for shift in shifts_for_channel:
                # Parse shift days
                shift_days = [day.strip().lower() for day in shift.days.split(',')] if shift.days else []
                
                # Check each day the segment spans
                for seg_day in days_to_check:
                    seg_day_name = seg_day.strftime('%A').lower()
                    
                    # Check if the segment's day matches any of the shift's days
                    if seg_day_name not in shift_days:
                        continue
                    
                    # Build UTC windows for this shift on the segment's day
                    shift_windows = _build_utc_windows_for_local_day(
                        shift.start_time, shift.end_time, seg_day, tz
                    )
                    
                    # Check if segment overlaps with any shift window
                    for window_start, window_end in shift_windows:
                        # Overlap check: [seg_start, seg_end] intersects [window_start, window_end]
                        if seg_start < window_end and seg_end > window_start:
                            segment_in_shift = True
                            break
                    
                    if segment_in_shift:
                        break
                
                if segment_in_shift:
                    break
            
            # If segment does NOT fall within any shift, mark as requires_analysis=False
            if not segment_in_shift:
                seg_ref["requires_analysis"] = False


def build_suppression_intervals_from_title_rules(
    channel_to_segments: DefaultDict[int, List[Dict[str, Any]]],
    channel_ids: List[int],
    suppression_duration: Optional[timedelta],
) -> DefaultDict[int, List[Tuple[Any, Any]]]:
    """Create per-channel suppression intervals from TitleMappingRule configuration."""
    effective_duration = suppression_duration or timedelta(minutes=10)
    channel_to_rules = get_active_title_rules(channel_ids)
    channel_to_intervals: DefaultDict[int, List[Tuple[Any, Any]]] = defaultdict(list)

    for channel_id, segs in channel_to_segments.items():
        sorted_segs = sorted(segs, key=lambda s: s["_parsed_start"])  # timeline order
        rules_for_channel = channel_to_rules.get(channel_id, [])
        if not rules_for_channel:
            continue

        # title -> indices map
        title_to_indices: DefaultDict[str, List[int]] = defaultdict(list)
        for idx, s in enumerate(sorted_segs):
            t = s.get("title")
            if isinstance(t, str):
                title_to_indices[t].append(idx)

        for rule in rules_for_channel:
            before_title = rule.before_title
            after_title = (rule.after_title or "").strip()

            before_indices = title_to_indices.get(before_title, [])
            if not before_indices:
                continue

            if not after_title:
                for idx in before_indices:
                    s = sorted_segs[idx]
                    channel_to_intervals[channel_id].append((s["_parsed_start"], s["_parsed_end"]))
                continue

            after_indices = title_to_indices.get(after_title, [])
            for b_idx in before_indices:
                before_seg = sorted_segs[b_idx]
                start_at = before_seg["_parsed_start"]
                cap_end = start_at + effective_duration

                next_after_start = None
                for a_idx in after_indices:
                    if a_idx > b_idx:
                        next_after_start = sorted_segs[a_idx]["_parsed_start"]
                        break

                if next_after_start is not None:
                    end_at = next_after_start if next_after_start < cap_end else cap_end
                else:
                    end_at = cap_end

                if end_at > start_at:
                    channel_to_intervals[channel_id].append((start_at, end_at))

        # Merge intervals per channel
        channel_to_intervals[channel_id] = _merge_intervals(channel_to_intervals[channel_id])

    return channel_to_intervals


def rename_titles_from_title_rules(
    channel_to_segments: DefaultDict[int, List[Dict[str, Any]]],
    channel_ids: List[int],
) -> None:
    """Rename titles in-place for the immediate segment after before_title if unrecognized."""
    channel_to_rules = get_active_title_rules(channel_ids)

    for channel_id, segs in channel_to_segments.items():
        sorted_segs = sorted(segs, key=lambda s: s["_parsed_start"])  # timeline order
        rules_for_channel = channel_to_rules.get(channel_id, [])
        if not rules_for_channel:
            continue

        title_to_indices: DefaultDict[str, List[int]] = defaultdict(list)
        for idx, s in enumerate(sorted_segs):
            t = s.get("title")
            if isinstance(t, str):
                title_to_indices[t].append(idx)

        for rule in rules_for_channel:
            before_title = rule.before_title
            before_indices = title_to_indices.get(before_title, [])
            if not before_indices:
                continue
            for b_idx in before_indices:
                next_idx = b_idx + 1
                if next_idx >= len(sorted_segs):
                    continue
                next_seg = sorted_segs[next_idx]["_ref"]
                if next_seg.get("is_recognized") is True:
                    continue
                category_name = rule.category.name
                next_seg["title"] = category_name
                seg_id = next_seg.get("id")
                if seg_id is not None:
                    update_audio_segment_title(seg_id, category_name)


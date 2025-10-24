from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from django.db.models import Q, QuerySet
from django.utils import timezone as dj_timezone

from .models import Shift, PredefinedFilter, FilterSchedule
from data_analysis.models import AudioSegments


def _weekday_str(dt: date) -> str:
    return dt.strftime("%A").lower()


def _build_utc_windows_for_local_day(start_t: time, end_t: time, local_day: date, tz: ZoneInfo):
    """
    Build one or two UTC windows corresponding to a local wall-clock interval.
    Allows overnight intervals when start_t > end_t.
    Returns list of tuples: [(start_utc_dt, end_utc_dt), ...]
    """
    start_local = datetime.combine(local_day, start_t, tzinfo=tz)
    if start_t <= end_t:
        end_local = datetime.combine(local_day, end_t, tzinfo=tz)
        return [(start_local.astimezone(ZoneInfo("UTC")), end_local.astimezone(ZoneInfo("UTC")))]

    # Overnight: split across midnight
    end_local_first = datetime.combine(local_day, time(23, 59, 59, 999999), tzinfo=tz)
    start_local_second = datetime.combine(local_day + timedelta(days=1), time(0, 0), tzinfo=tz)
    end_local_second = datetime.combine(local_day + timedelta(days=1), end_t, tzinfo=tz)
    return [
        (start_local.astimezone(ZoneInfo("UTC")), end_local_first.astimezone(ZoneInfo("UTC"))),
        (start_local_second.astimezone(ZoneInfo("UTC")), end_local_second.astimezone(ZoneInfo("UTC"))),
    ]


def _build_q_for_windows(windows_utc):
    q = Q()
    for start_utc, end_utc in windows_utc:
        q |= Q(start_time__lt=end_utc, end_time__gt=start_utc)
    return q


def filter_segments_by_shift(shift_id: int, utc_start: datetime, utc_end: datetime) -> QuerySet:
    """
    Return queryset of AudioSegments overlapping the shift windows between utc_start and utc_end.
    shift.times are treated as wall-clock in shift.channel.timezone for every day in the range.
    Only includes days that match the shift's days_of_week field.
    """
    if utc_start.tzinfo is None or utc_end.tzinfo is None:
        raise ValueError("utc_start and utc_end must be timezone-aware in UTC")
    if utc_end <= utc_start:
        return AudioSegments.objects.none()

    shift = Shift.objects.get(pk=shift_id)
    tz = ZoneInfo(shift.channel.timezone or "UTC")

    # Parse the shift's days_of_week field
    shift_days = [day.strip().lower() for day in shift.days.split(',')] if shift.days else []

    # Iterate days in local tz covering the UTC range
    start_local = utc_start.astimezone(tz)
    end_local = utc_end.astimezone(tz)

    windows = []
    day = start_local.date()
    while day <= end_local.date():
        # Check if this day matches any of the shift's specified days
        current_day_name = day.strftime('%A').lower()
        
        # If shift has specific days defined, only process matching days
        if not shift_days or current_day_name in shift_days:
            windows.extend(_build_utc_windows_for_local_day(shift.start_time, shift.end_time, day, tz))
        
        day += timedelta(days=1)

    q = _build_q_for_windows(windows)
    return AudioSegments.objects.filter(q)


def filter_segments_by_predefined_filter(filter_id: int, utc_start: datetime, utc_end: datetime) -> QuerySet:
    """
    Return queryset of AudioSegments overlapping any FilterSchedule windows between utc_start and utc_end
    for the given PredefinedFilter. Each schedule applies on its day_of_week in the filter's timezone,
    allowing overnight windows.
    """
    if utc_start.tzinfo is None or utc_end.tzinfo is None:
        raise ValueError("utc_start and utc_end must be timezone-aware in UTC")
    if utc_end <= utc_start:
        return AudioSegments.objects.none()

    pf = PredefinedFilter.objects.get(pk=filter_id)
    tz = ZoneInfo(pf.timezone or "UTC")

    start_local = utc_start.astimezone(tz)
    end_local = utc_end.astimezone(tz)

    windows = []
    day = start_local.date()
    while day <= end_local.date():
        dow = _weekday_str(day)
        schedules = pf.schedules.filter(day_of_week=dow, predefined_filter=pf)
        for sched in schedules:
            windows.extend(_build_utc_windows_for_local_day(sched.start_time, sched.end_time, day, tz))

        # Include tail from previous day's overnight schedules
        prev = day - timedelta(days=1)
        prev_dow = _weekday_str(prev)
        overnight_schedules = pf.schedules.filter(day_of_week=prev_dow, predefined_filter=pf)
        for sched in overnight_schedules:
            if sched.start_time > sched.end_time:
                windows.extend(_build_utc_windows_for_local_day(sched.start_time, sched.end_time, prev, tz))

        day += timedelta(days=1)

    q = _build_q_for_windows(windows)
    return AudioSegments.objects.filter(q)



from typing import Tuple
from datetime import datetime, timedelta
from datetime import time as dt_time
import re
import time
from cachetools import cached, TTLCache
from backend.dpp import get_route_info


def get_next_meetup_time(target_weekday: int, target_hour: int) -> datetime:
    start_dt = datetime.now()

    current_weekday = start_dt.weekday()
    days_ahead = target_weekday - current_weekday

    if days_ahead == 0:
        if start_dt.time() >= dt_time(target_hour, 0):
            days_ahead = 7
        else:
            days_ahead = 0
    elif days_ahead < 0:
        days_ahead += 7

    next_dt = start_dt + timedelta(days=days_ahead)
    next_dt = next_dt.replace(hour=target_hour, minute=0, second=0, microsecond=0)
    return next_dt


def validate_date_time(date_str: str, time_str: str) -> Tuple[bool, str]:
    try:
        event_datetime = datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M")
    except ValueError:
        return (
            False,
            "Invalid date or time format. Please ensure date is DD/MM/YYYY and time is HH:MM.",
        )

    now = datetime.now()
    three_months_later = now + timedelta(days=90)  # Approximation of 3 months

    if event_datetime <= now:
        return False, "The selected date and time must be in the future."
    if event_datetime > three_months_later:
        return (
            False,
            "The selected date and time must not be more than 3 months in the future.",
        )

    return True, ""


def parse_time_to_minutes(time_str: str) -> int:
    pattern = r"^\s*(?:(\d+)\s*hod)?(?:\s*(\d+)\s*min)?\s*$"
    match = re.match(pattern, time_str, re.IGNORECASE)

    if not match:
        raise ValueError(f"Invalid time format: '{time_str}'")

    hours_str, minutes_str = match.groups()

    hours = int(hours_str) if hours_str else 0
    minutes = int(minutes_str) if minutes_str else 0

    if hours < 0:
        raise ValueError("Hours cannot be negative.")
    if minutes < 0:
        raise ValueError("Minutes cannot be negative.")
    if minutes >= 60:
        raise ValueError("Minutes must be less than 60.")

    total_minutes = hours * 60 + minutes
    return total_minutes


def get_total_minutes(from_stop: str, to_stop: str, dt: datetime) -> int:
    if from_stop == to_stop:
        return 0

    day = dt.day
    month = dt.month
    year = dt.year
    date_str = f"{day}.{month}.{year}"
    time_str = dt.strftime("%H:%M")

    try:
        total_minutes = get_route_info(from_stop, to_stop, date_str, time_str)
        return total_minutes
    except Exception as e:
        raise ValueError(f"Failed to get travel time: {e}") from e


@cached(cache=TTLCache(maxsize=10**6, ttl=24 * 60 * 60))
def get_total_minutes_with_retries(
    from_stop: str,
    to_stop: str,
    dt: datetime,
    max_retries: int = 3,
    retry_delay: int = 2,
) -> int:
    attempt = 0

    while attempt < max_retries:
        try:
            total_minutes = get_total_minutes(from_stop, to_stop, dt)
            return total_minutes
        except Exception as e:
            attempt += 1
            if attempt < max_retries:
                print(
                    f"Error processing pair ({from_stop}, {to_stop}): {e}. Retrying in {retry_delay} seconds... (Attempt {attempt}/{max_retries})"
                )
                time.sleep(retry_delay)
            else:
                print(
                    f"Failed to process pair ({from_stop}, {to_stop}) after {max_retries} attempts."
                )
                return None
    return None
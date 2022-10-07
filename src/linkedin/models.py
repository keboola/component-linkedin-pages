from dataclasses import dataclass
import logging
import re
from datetime import datetime, timedelta, timezone
from enum import Enum, unique
import math
from typing import Iterator

from dateparser import parse
from inflection import underscore

from keboola.utils.date import split_dates_to_chunks

# Time range config dict params:
KEY_START = "date_from"
KEY_END = "date_to"

# Config dict constants:
VAL_LAST_RUN = "last run"

# Other constants:
MAXIMUM_TIME_RANGE_SIZE = timedelta(days=30 * 14)    # i.e. 14 months
LAST_RUN_MISSING_DEFAULT = datetime(year=2003, month=5, day=5, tzinfo=timezone.utc)    # LinkedIn launched

URN_RE = re.compile(r"urn:li:(\w+):(\d+)")


@dataclass(slots=True, frozen=True)
class URN:
    entity_type: str
    id: int

    def __str__(self):
        return f"urn:li:{self.entity_type}:{self.id}"

    @classmethod
    def from_str(cls, s: str):
        matches = URN_RE.match(s)
        if matches:
            return cls(entity_type=matches.group(1), id=int(matches.group(2)))
        else:
            raise ValueError(f"URN string invalid: {s}")


@unique
class StandardizedDataType(Enum):
    # DEGREES = "degrees"  # Not needed
    # FIELDS_OF_STUDY = "fieldsOfStudy"  # Not needed
    FUNCTONS = "functions"
    INDUSTRIES = "industries"
    SENIORITIES = "seniorities"
    # SKILLS = "skills"  # Not needed
    # SUPER_TITLES = "superTitles"  # Does not work due to 403 API error   # Not needed
    # TITLES = "titles"  # Not needed
    # IAB_CATEGORIES = "iabCategories"  # Not needed
    # Locations:
    COUNTRIES = "countries"
    # STATES = "states"  # Not needed
    REGIONS = "regions"

    @property
    def normalized_name(self) -> str:
        return underscore(self.value)


def datetime_to_milliseconds_since_epoch(dt: datetime) -> int:
    return round(dt.timestamp() * 1000)


def milliseconds_since_epoch_to_datetime(milliseconds_since_epoch: int, tz: timezone = timezone.utc) -> datetime:
    return datetime.fromtimestamp(milliseconds_since_epoch / 1000.0, tz=tz)


def parse_date_from_string(s: str):
    dt = parse(s)
    if dt is None:
        raise ValueError(f'Could not parse the string "{s}" into a valid datetime object.'
                         f' Please either use a fixed date such as "1982-09-13" or'
                         f' relative expression such as "7 days ago", "today", etc.')
    tz = dt.tzinfo or timezone.utc
    return dt.replace(hour=0, minute=0, second=0, microsecond=0, fold=0, tzinfo=tz)


@unique
class TimeGranularityType(Enum):
    DAY = "DAY"
    MONTH = "MONTH"


@dataclass(slots=True, frozen=True)
class TimeRange:
    start: datetime
    end: datetime

    def __post_init__(self):
        if self.start > self.end:
            raise ValueError(f"Start value must be earlier than or concurrent with end value."
                             f" Resultant datetimes: {self.to_serializable_dict()}")

    def to_url_string(self) -> str:
        return (f"(start:{datetime_to_milliseconds_since_epoch(self.start)},"
                f"end:{datetime_to_milliseconds_since_epoch(self.end)})")

    @classmethod
    def from_api_dict(cls, d: dict):
        return cls(start=milliseconds_since_epoch_to_datetime(d["start"]),
                   end=milliseconds_since_epoch_to_datetime(d["end"]))

    @classmethod
    def from_config_dict(cls, d: dict, last_run_datetime_str: str | None = None):
        end = parse_date_from_string(d[KEY_END])
        if d[KEY_START] == VAL_LAST_RUN:
            if last_run_datetime_str:
                start = datetime.fromisoformat(last_run_datetime_str)
            else:
                logging.warning("Last run datetime is not specified (in component state)"
                                " despite 'last run' being used as the start of a time range."
                                " Downloading data since LinkedIn launch (2003-05-05).")
                start = LAST_RUN_MISSING_DEFAULT
        else:
            start = parse_date_from_string(d[KEY_START])
        return cls(start=start, end=end)

    def to_serializable_dict(self):
        return {"start": self.start.isoformat(timespec="seconds"), "end": self.end.isoformat(timespec="seconds")}

    def __str__(self):
        return str(self.to_serializable_dict())

    @property
    def timedelta(self):
        return self.end - self.start

    @property
    def length_in_days(self):
        return self.timedelta / timedelta(days=1)

    @classmethod
    def from_chunk_dict(cls, d: dict):
        start = datetime.fromisoformat(d["start_date"])
        end = datetime.fromisoformat(d["end_date"])
        return cls(start=start, end=end)


@dataclass(slots=True, frozen=True)
class TimeIntervals:
    time_granularity_type: TimeGranularityType
    time_range: TimeRange

    def to_url_string(self) -> str:
        return (f"(timeRange:{self.time_range.to_url_string()},"
                f"timeGranularityType:{self.time_granularity_type.value})")

    @property
    def amount(self):
        if self.time_granularity_type is TimeGranularityType.DAY:
            return math.ceil(self.time_range.length_in_days)
        elif self.time_granularity_type is TimeGranularityType.MONTH:
            return math.ceil(self.time_range.length_in_days / 30)
        else:
            raise ValueError(f"Impossible TimeGranularityType: {self.time_granularity_type}")

    def to_downloadable_chunks(self) -> Iterator["TimeIntervals"]:
        if self.time_range.timedelta <= MAXIMUM_TIME_RANGE_SIZE:
            return [self]
        else:
            datetime_chunks = split_dates_to_chunks(start_date=self.time_range.start,
                                                    end_date=self.time_range.end,
                                                    intv=MAXIMUM_TIME_RANGE_SIZE.days,
                                                    generator=True)
            return (self.__class__(time_granularity_type=self.time_granularity_type,
                                   time_range=TimeRange.from_chunk_dict(chunk_dict)) for chunk_dict in datetime_chunks)

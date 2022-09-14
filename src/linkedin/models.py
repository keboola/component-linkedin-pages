from dataclasses import dataclass
import re
from datetime import datetime, timedelta, timezone
from enum import Enum, unique
import math

import dateparser
from inflection import underscore

KEY_START = "start"
KEY_END = "end"

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
    DEGREES = "degrees"
    FIELDS_OF_STUDY = "fieldsOfStudy"
    FUNCTONS = "functions"
    INDUSTRIES = "industries"
    SENIORITIES = "seniorities"
    SKILLS = "skills"
    # SUPER_TITLES = "superTitles"  # Does not work due to 403 API error # TODO: ask LinkedIn support
    TITLES = "titles"
    IAB_CATEGORIES = "iabCategories"
    # Locations:
    COUNTRIES = "countries"
    STATES = "states"
    REGIONS = "regions"

    @property
    def normalized_name(self) -> str:
        return underscore(self.value)


def datetime_to_milliseconds_since_epoch(dt: datetime) -> int:
    return round(dt.timestamp() * 1000)


def milliseconds_since_epoch_to_datetime(milliseconds_since_epoch: int, tz: timezone = timezone.utc) -> datetime:
    return datetime.fromtimestamp(milliseconds_since_epoch / 1000.0, tz=tz)


def parse_date_from_string(s: str):
    dt = dateparser.parse(s)
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
    def from_config_dict(cls, d: dict):
        return cls(start=parse_date_from_string(d["start"]), end=parse_date_from_string(d["end"]))

    def to_serializable_dict(self):
        return {"start": self.start.isoformat(timespec="seconds"), "end": self.end.isoformat(timespec="seconds")}

    def __str__(self):
        return str(self.to_serializable_dict())

    @property
    def length_in_days(self):
        return (self.end - self.start) / timedelta(days=1)


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

from dataclasses import dataclass
import re
from datetime import datetime, timedelta
from enum import Enum
import math

import dateparser

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


def organization_urn(id: str | int):
    return URN(entity_type="organization", id=id)


def datetime_to_milliseconds_since_epoch(dt: datetime) -> int:
    return round(dt.timestamp() * 1000)


def milliseconds_since_epoch_to_datetime(milliseconds_since_epoch: int) -> datetime:
    return datetime.fromtimestamp(milliseconds_since_epoch / 1000.0)


class TimeGranularityType(Enum):
    DAY = "DAY"
    MONTH = "MONTH"


@dataclass(slots=True, frozen=True)
class TimeRange:
    start: datetime
    end: datetime

    def to_url_string(self) -> str:
        return (f"(start:{datetime_to_milliseconds_since_epoch(self.start)},"
                f"end:{datetime_to_milliseconds_since_epoch(self.end)})")

    @classmethod
    def from_api_dict(cls, d: dict):
        return cls(start=milliseconds_since_epoch_to_datetime(d["start"]),
                   end=milliseconds_since_epoch_to_datetime(d["end"]))

    @classmethod
    def from_config_dict(cls, d: dict):
        return cls(start=dateparser.parse(d["start"]), end=dateparser.parse(d["end"]))

    def to_serializable_dict(self):
        return {"start": self.start.isoformat(timespec="seconds"), "end": self.end.isoformat(timespec="seconds")}

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

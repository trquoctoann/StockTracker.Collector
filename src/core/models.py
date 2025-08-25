from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import json
from typing import Any


class DataSource(Enum):
    API = "api"
    LIBRARY = "library"
    WEB_SCRAPING = "web_scraping"


class MessageBrokerType(Enum):
    KAFKA = "kafka"
    RABBITMQ = "rabbitmq"


class APISource(Enum):
    pass


class LibrarySource(Enum):
    VNSTOCK = "vnstock"


class WebScrapingSource(Enum):
    pass


class DataType(Enum):
    COMPANIES = "companies"
    SHAREHOLDERS = "shareholders"
    OFFICERS = "officers"
    SUBSIDIARIES = "subsidiaries"
    AFFILIATES = "affiliates"
    EVENTS = "events"
    NEWS = "news"


@dataclass
class StockData:
    symbol: str
    data_type: DataType
    source: DataSource
    timestamp: datetime = field(default_factory=datetime.now)
    data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "data_type": self.data_type.value,
            "source": self.source.value,
            "timestamp": self.timestamp.isoformat(),
            "data": self.data,
            "metadata": self.metadata,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StockData":
        return cls(
            symbol=data["symbol"],
            data_type=DataType(data["data_type"]),
            source=DataSource(data["source"]),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            data=data.get("data", {}),
            metadata=data.get("metadata", {}),
        )


@dataclass
class CollectorResult:
    success: bool
    data: list[StockData | BaseException] = field(default_factory=list)
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SenderResult:
    success: bool
    sent_count: int = 0
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RateLimitStatus:
    source_id: str
    current_count: int
    limit: int
    period: int
    reset_time: datetime
    is_limited: bool = False

    @property
    def remaining(self) -> int:
        return max(0, self.limit - self.current_count)


@dataclass
class CollectorConfig:
    name: str
    source: DataSource
    enabled: bool = True
    rate_limit: dict[str, int | float] | None = None
    timeout: int = 30
    retry_attempts: int = 3
    custom_config: dict[str, Any] = field(default_factory=dict)

    def get_rate_limit(self, key: str, default: int | float = 0) -> int | float:
        if not self.rate_limit:
            return default
        return self.rate_limit.get(key, default)

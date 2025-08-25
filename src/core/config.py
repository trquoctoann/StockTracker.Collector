from dataclasses import dataclass, field
import os
from typing import Any

from dotenv import load_dotenv

from .models import APISource, DataSource, LibrarySource, MessageBrokerType, WebScrapingSource

load_dotenv()


@dataclass
class RateLimitConfig:
    limit: int
    period: int

    @classmethod
    def default(self) -> "RateLimitConfig":
        return self(
            limit=int(os.getenv("DEFAULT_RATE_LIMIT", "3000")),
            period=int(os.getenv("DEFAULT_RATE_PERIOD", "60")),
        )

    @classmethod
    def for_api_source(self, api_source: APISource) -> "RateLimitConfig":
        api_rate_limits: dict[APISource, RateLimitConfig] = {}
        return api_rate_limits.get(api_source, self.default())

    @classmethod
    def for_library_source(self, library_source: LibrarySource) -> "RateLimitConfig":
        library_rate_limits: dict[LibrarySource, RateLimitConfig] = {
            LibrarySource.VNSTOCK: self(
                limit=int(os.getenv("VNSTOCK_RATE_LIMIT", "3000")),
                period=int(os.getenv("VNSTOCK_RATE_PERIOD", "60")),
            ),
        }
        return library_rate_limits.get(library_source, self.default())

    @classmethod
    def for_web_scraping_source(self, scraping_source: WebScrapingSource) -> "RateLimitConfig":
        scraping_rate_limits: dict[WebScrapingSource, RateLimitConfig] = {}
        return scraping_rate_limits.get(scraping_source, self.default())

    @classmethod
    def for_data_source(self, data_source: DataSource, source_name: str = "") -> "RateLimitConfig":
        if data_source == DataSource.API:
            api_source = APISource(source_name)
            return self.for_api_source(api_source)

        if data_source == DataSource.LIBRARY:
            library_source = LibrarySource(source_name)
            return self.for_library_source(library_source)

        if data_source == DataSource.WEB_SCRAPING:
            scraping_source = WebScrapingSource(source_name)
            return self.for_web_scraping_source(scraping_source)

        raise ValueError(f"Unsupported DataSource: {data_source}")


@dataclass
class DataSourceConfig:
    name: str
    data_source: DataSource
    enabled: bool = True
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig.default)
    timeout: int = 30
    retry_attempts: int = 3
    retry_delay: float = 1.0
    custom_config: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.rate_limit == RateLimitConfig.default():
            self.rate_limit = RateLimitConfig.for_data_source(self.data_source, self.name)

    @classmethod
    def create_api_config(self, api_source: APISource, **kwargs: Any) -> "DataSourceConfig":
        return self(
            name=api_source.value,
            data_source=DataSource.API,
            rate_limit=RateLimitConfig.for_api_source(api_source),
            **kwargs,
        )

    @classmethod
    def create_library_config(self, library_source: LibrarySource, **kwargs: Any) -> "DataSourceConfig":
        return self(
            name=library_source.value,
            data_source=DataSource.LIBRARY,
            rate_limit=RateLimitConfig.for_library_source(library_source),
            **kwargs,
        )

    @classmethod
    def create_web_scraping_config(self, scraping_source: WebScrapingSource, **kwargs: Any) -> "DataSourceConfig":
        return self(
            name=scraping_source.value,
            data_source=DataSource.WEB_SCRAPING,
            rate_limit=RateLimitConfig.for_web_scraping_source(scraping_source),
            **kwargs,
        )


@dataclass
class ConcurrencyConfig:
    max_workers: int = field(default_factory=lambda: int(os.getenv("MAX_WORKERS", "12")))
    max_concurrent_requests: int = field(default_factory=lambda: int(os.getenv("MAX_CONCURRENT_REQUESTS", "100")))
    semaphore_limit: int = field(default_factory=lambda: int(os.getenv("SEMAPHORE_LIMIT", "50")))
    prefix_thread_name: str = field(default_factory=lambda: os.getenv("PREFIX_THREAD_NAME", ""))


@dataclass
class MessageBrokerConfig:
    type: MessageBrokerType = field(
        default_factory=lambda: MessageBrokerType(os.getenv("DEFAULT_BROKER_TYPE", "rabbitmq"))
    )

    batch_size: int = field(default_factory=lambda: int(os.getenv("BROKER_BATCH_SIZE", "100")))
    flush_interval: int = field(default_factory=lambda: int(os.getenv("BROKER_FLUSH_INTERVAL", "5")))

    kafka_hosts: str = field(default_factory=lambda: os.getenv("KAFKA_HOSTS", "localhost:9092"))
    kafka_topic: str = field(default_factory=lambda: os.getenv("KAFKA_TOPIC", "stock_data"))
    kafka_config: dict[str, Any] = field(default_factory=dict)

    rabbitmq_url: str = field(default_factory=lambda: os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"))
    rabbitmq_exchange: str = field(default_factory=lambda: os.getenv("RABBITMQ_EXCHANGE", "stock_exchange"))
    rabbitmq_exchange_type: str = field(default_factory=lambda: os.getenv("RABBITMQ_EXCHANGE_TYPE", "topic"))
    rabbitmq_routing_key: str = field(default_factory=lambda: os.getenv("RABBITMQ_ROUTING_KEY", "stock.data"))
    rabbitmq_queue: str = field(default_factory=lambda: os.getenv("RABBITMQ_QUEUE", "stock_queue"))
    rabbitmq_durable: bool = field(default_factory=lambda: os.getenv("RABBITMQ_DURABLE", "true").lower() == "true")
    rabbitmq_config: dict[str, Any] = field(default_factory=dict)

    hosts: str = field(default_factory=lambda: os.getenv("DEFAULT_BROKER_HOSTS", ""))
    topic: str = field(default_factory=lambda: os.getenv("DEFAULT_BROKER_TOPIC", ""))
    custom_config: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.hosts and not self.kafka_hosts:
            self.kafka_hosts = self.hosts
        if self.topic and not self.kafka_topic:
            self.kafka_topic = self.topic
        if self.custom_config:
            if self.type == MessageBrokerType.KAFKA:
                self.kafka_config.update(self.custom_config)
            elif self.type == MessageBrokerType.RABBITMQ:
                self.rabbitmq_config.update(self.custom_config)

    @property
    def is_kafka(self) -> bool:
        return self.type == MessageBrokerType.KAFKA

    @property
    def is_rabbitmq(self) -> bool:
        return self.type == MessageBrokerType.RABBITMQ


@dataclass
class AppConfig:
    data_sources: dict[str, DataSourceConfig] = field(
        default_factory=lambda: {
            "vnstock": DataSourceConfig.create_library_config(LibrarySource.VNSTOCK),
        }
    )
    concurrency: ConcurrencyConfig = field(default_factory=ConcurrencyConfig)
    message_broker: MessageBrokerConfig = field(default_factory=MessageBrokerConfig)

    def get_data_source_config(self, source_name: str) -> DataSourceConfig | None:
        return self.data_sources.get(source_name)

    def get_or_create_data_source_config(self, source_name: str, data_source: DataSource) -> DataSourceConfig:
        if source_name in self.data_sources:
            return self.data_sources[source_name]

        new_config = DataSourceConfig(
            name=source_name,
            data_source=data_source,
            rate_limit=RateLimitConfig.for_data_source(data_source, source_name),
        )

        self.data_sources[source_name] = new_config
        return new_config

    def get_api_source_config(self, api_source: APISource) -> DataSourceConfig:
        source_name = api_source.value
        if source_name not in self.data_sources:
            self.data_sources[source_name] = DataSourceConfig.create_api_config(api_source)
        return self.data_sources[source_name]

    def get_library_source_config(self, library_source: LibrarySource) -> DataSourceConfig:
        source_name = library_source.value
        if source_name not in self.data_sources:
            self.data_sources[source_name] = DataSourceConfig.create_library_config(library_source)
        return self.data_sources[source_name]

    def get_web_scraping_source_config(self, scraping_source: WebScrapingSource) -> DataSourceConfig:
        source_name = scraping_source.value
        if source_name not in self.data_sources:
            self.data_sources[source_name] = DataSourceConfig.create_web_scraping_config(scraping_source)
        return self.data_sources[source_name]


config = AppConfig()

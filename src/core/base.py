"""
Base classes và interfaces cho hệ thống thu thập dữ liệu
"""

from abc import ABC, abstractmethod
import asyncio
from collections.abc import AsyncGenerator
from datetime import datetime
import time
from typing import Any

from asyncio_throttle import Throttler
from loguru import logger
from tenacity import AsyncRetrying, RetryError, stop_after_attempt, wait_exponential

from .config import DataSourceConfig, config
from .models import CollectorResult, DataSource, RateLimitStatus, SenderResult, StockData


class RateLimiter:
    """Thread-safe rate limiter cho async operations"""

    def __init__(self, source_config: DataSourceConfig):
        self.source_config = source_config
        self.throttler = Throttler(rate_limit=source_config.rate_limit.limit, period=source_config.rate_limit.period)
        self._lock = asyncio.Lock()
        self._request_times: list[float] = []

    async def acquire(self) -> None:
        """Acquire permission để thực hiện request"""
        async with self._lock:
            await self.throttler.acquire()
            self._request_times.append(time.time())

            # Cleanup old request times
            cutoff_time = time.time() - self.source_config.rate_limit.period
            self._request_times = [t for t in self._request_times if t > cutoff_time]

    async def get_status(self) -> RateLimitStatus:
        """Lấy trạng thái rate limit hiện tại"""
        async with self._lock:
            current_time = time.time()
            cutoff_time = current_time - self.source_config.rate_limit.period
            recent_requests = [t for t in self._request_times if t > cutoff_time]

            return RateLimitStatus(
                source_id=self.source_config.name,
                current_count=len(recent_requests),
                limit=self.source_config.rate_limit.limit,
                period=self.source_config.rate_limit.period,
                reset_time=datetime.fromtimestamp(cutoff_time + self.source_config.rate_limit.period),
                is_limited=len(recent_requests) >= self.source_config.rate_limit.limit,
            )


class BaseCollector(ABC):
    """Abstract base class cho tất cả data collectors"""

    def __init__(self, source_config: DataSourceConfig):
        self.source_config = source_config
        self.rate_limiter = RateLimiter(source_config)
        self._semaphore = asyncio.Semaphore(config.concurrency.semaphore_limit)
        self._session_lock = asyncio.Lock()
        self._is_running = False

    @property
    @abstractmethod
    def data_source(self) -> DataSource:
        """Trả về loại data source"""
        pass

    @abstractmethod
    async def _collect_data(self, symbols: list[str], **kwargs: Any) -> list[StockData]:
        """
        Implementation cụ thể cho việc thu thập dữ liệu
        Subclass phải implement method này
        """
        pass

    async def collect(self, symbols: list[str], **kwargs: Any) -> CollectorResult:
        """
        Public method để thu thập dữ liệu với error handling và retry
        """
        if not self.source_config.enabled:
            return CollectorResult(success=False, error=f"Data source {self.source_config.name} is disabled")

        async with self._semaphore:
            try:
                # Rate limiting
                await self.rate_limiter.acquire()

                # Retry mechanism
                async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(self.source_config.retry_attempts),
                    wait=wait_exponential(multiplier=self.source_config.retry_delay, min=1, max=10),
                    reraise=True,
                ):
                    with attempt:
                        logger.info(f"Collecting data from {self.source_config.name} for symbols: {symbols}")
                        data = await self._collect_data(symbols, **kwargs)

                        return CollectorResult(
                            success=True,
                            data=data,
                            metadata={
                                "source": self.source_config.name,
                                "symbols_count": len(symbols),
                                "collected_count": len(data),
                                "timestamp": datetime.now().isoformat(),
                            },
                        )

            except RetryError as e:
                logger.error(f"Failed to collect data from {self.source_config.name} after retries: {e}")
                return CollectorResult(
                    success=False,
                    error=f"Retry failed: {e!s}",
                    metadata={"source": self.source_config.name, "symbols": symbols},
                )
            except Exception as e:
                logger.error(f"Unexpected error in {self.source_config.name}: {e}")
                return CollectorResult(
                    success=False, error=str(e), metadata={"source": self.source_config.name, "symbols": symbols}
                )

    async def collect_stream(self, symbols: list[str], **kwargs: Any) -> AsyncGenerator[StockData, None]:
        """
        Stream data collection - yield data as soon as available
        """
        if not self.source_config.enabled:
            logger.warning(f"Data source {self.source_config.name} is disabled")
            return

        for symbol in symbols:
            try:
                async with self._semaphore:
                    await self.rate_limiter.acquire()
                    data = await self._collect_data([symbol], **kwargs)
                    for item in data:
                        yield item
            except Exception as e:
                logger.error(f"Error collecting data for {symbol} from {self.source_config.name}: {e}")
                continue

    async def health_check(self) -> dict[str, Any]:
        """Kiểm tra trạng thái của collector"""
        rate_status = await self.rate_limiter.get_status()

        return {
            "source_name": self.source_config.name,
            "data_source_type": self.source_config.data_source.value,
            "enabled": self.source_config.enabled,
            "rate_limit_status": {
                "current_count": rate_status.current_count,
                "limit": rate_status.limit,
                "remaining": rate_status.remaining,
                "is_limited": rate_status.is_limited,
                "reset_time": rate_status.reset_time.isoformat(),
            },
            "semaphore_available": self._semaphore._value,
            "is_running": self._is_running,
        }

    async def start(self) -> None:
        """Khởi tạo collector"""
        self._is_running = True
        logger.info(f"Started collector: {self.source_config.name}")

    async def stop(self) -> None:
        """Dừng collector và cleanup resources"""
        self._is_running = False
        logger.info(f"Stopped collector: {self.source_config.name}")


class BaseSender(ABC):
    """Abstract base class cho data senders"""

    def __init__(self, name: str):
        self.name = name
        self._semaphore = asyncio.Semaphore(config.concurrency.semaphore_limit)
        self._batch_lock = asyncio.Lock()
        self._is_running = False

    @abstractmethod
    async def _send_data(self, data: list[StockData]) -> SenderResult:
        """
        Implementation cụ thể cho việc gửi dữ liệu
        Subclass phải implement method này
        """
        pass

    async def send(self, data: list[StockData]) -> SenderResult:
        """
        Public method để gửi dữ liệu với error handling
        """
        if not data:
            return SenderResult(success=True, sent_count=0)

        async with self._semaphore:
            try:
                logger.info(f"Sending {len(data)} items via {self.name}")
                result = await self._send_data(data)

                if result.success:
                    logger.info(f"Successfully sent {result.sent_count} items via {self.name}")
                else:
                    logger.error(f"Failed to send data via {self.name}: {result.error}")

                return result

            except Exception as e:
                logger.error(f"Unexpected error in sender {self.name}: {e}")
                return SenderResult(
                    success=False, error=str(e), metadata={"sender": self.name, "data_count": len(data)}
                )

    async def send_batch(self, data_stream: AsyncGenerator[list[StockData], None]) -> list[SenderResult]:
        """
        Gửi data theo batch
        """
        results = []
        async for batch in data_stream:
            result = await self.send(batch)
            results.append(result)
        return results

    async def health_check(self) -> dict[str, Any]:
        """Kiểm tra trạng thái của sender"""
        return {"sender_name": self.name, "semaphore_available": self._semaphore._value, "is_running": self._is_running}

    async def start(self) -> None:
        """Khởi tạo sender"""
        self._is_running = True
        logger.info(f"Started sender: {self.name}")

    async def stop(self) -> None:
        """Dừng sender và cleanup resources"""
        self._is_running = False
        logger.info(f"Stopped sender: {self.name}")


class CollectorManager:
    """Quản lý và điều phối các collectors"""

    def __init__(self):
        self.collectors: dict[str, BaseCollector] = {}
        self._global_semaphore = asyncio.Semaphore(config.concurrency.max_concurrent_requests)

    def register_collector(self, collector: BaseCollector) -> None:
        """Đăng ký collector"""
        self.collectors[collector.source_config.name] = collector
        logger.info(f"Registered collector: {collector.source_config.name}")

    async def collect_all(self, symbols: list[str], **kwargs: Any) -> dict[str, CollectorResult]:
        """Thu thập dữ liệu từ tất cả collectors song song"""
        tasks = []

        for name, collector in self.collectors.items():
            if collector.source_config.enabled:
                task = asyncio.create_task(collector.collect(symbols, **kwargs), name=f"collect_{name}")
                tasks.append((name, task))

        results = {}
        if tasks:
            completed_tasks = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)

            for (name, _), result in zip(tasks, completed_tasks, strict=False):
                if isinstance(result, Exception):
                    results[name] = CollectorResult(success=False, error=str(result), metadata={"source": name})
                else:
                    results[name] = result

        return results

    async def health_check_all(self) -> dict[str, dict[str, Any]]:
        """Kiểm tra trạng thái tất cả collectors"""
        health_checks = {}

        for name, collector in self.collectors.items():
            try:
                health_checks[name] = await collector.health_check()
            except Exception as e:
                health_checks[name] = {"error": str(e), "status": "unhealthy"}

        return health_checks

    async def start_all(self) -> None:
        """Khởi động tất cả collectors"""
        for collector in self.collectors.values():
            await collector.start()

    async def stop_all(self) -> None:
        """Dừng tất cả collectors"""
        for collector in self.collectors.values():
            await collector.stop()

"""
API Collectors cho việc thu thập dữ liệu từ các REST API
"""

import asyncio
from datetime import datetime
import json
from typing import Any

import aiohttp
from loguru import logger

from ..core.base import BaseCollector
from ..core.config import DataSourceConfig, config
from ..core.models import APISource, DataSource, DataType, StockData


class HTTPAPICollector(BaseCollector):
    """Generic HTTP API Collector"""

    def __init__(self, source_config: DataSourceConfig, base_url: str, headers: dict[str, str] | None = None):
        super().__init__(source_config)
        self.base_url = base_url.rstrip("/")
        self.headers = headers or {}
        self._session: aiohttp.ClientSession | None = None

    @property
    def data_source(self) -> DataSource:
        return DataSource.API

    async def _get_session(self) -> aiohttp.ClientSession:
        """Lazy initialization của HTTP session"""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.source_config.timeout)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers=self.headers,
                connector=aiohttp.TCPConnector(limit=config.concurrency.max_concurrent_requests, limit_per_host=30),
            )
        return self._session

    async def _make_request(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Thực hiện HTTP request với error handling"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        session = await self._get_session()

        try:
            async with session.get(url, params=params) as response:
                response.raise_for_status()

                if response.content_type == "application/json":
                    return await response.json()
                text = await response.text()
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    return {"raw_data": text}

        except aiohttp.ClientError as e:
            logger.error(f"HTTP request failed for {url}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during request to {url}: {e}")
            raise

    async def _collect_data(self, symbols: list[str], **kwargs: Any) -> list[StockData]:
        """
        Base implementation - subclass nên override method này
        """
        data = []
        data_type = kwargs.get("data_type", DataType.COMPANIES)

        for symbol in symbols:
            try:
                # Generic endpoint pattern
                endpoint = kwargs.get("endpoint", f"/stock/{symbol}")
                params = kwargs.get("params", {})

                response_data = await self._make_request(endpoint, params)

                stock_data = StockData(
                    symbol=symbol,
                    data_type=data_type,
                    source=self.data_source,
                    timestamp=datetime.now(),
                    data=response_data,
                    metadata={"source_name": self.source_config.name, "endpoint": endpoint, "params": params},
                )
                data.append(stock_data)

            except Exception as e:
                logger.error(f"Failed to collect data for {symbol}: {e}")
                continue

        return data

    async def stop(self) -> None:
        """Cleanup HTTP session"""
        await super().stop()
        if self._session and not self._session.closed:
            await self._session.close()


class VNStockAPICollector(HTTPAPICollector):
    """Collector cho VNStock API"""

    def __init__(self):
        # Sử dụng cơ chế tự động chọn rate limit config
        source_config = config.get_api_source_config(APISource.VNSTOCK)
        super().__init__(
            source_config=source_config,
            base_url="https://api.vnstock.vn/api/v1",
            headers={"User-Agent": "StockTracker/1.0"},
        )

    async def _collect_data(self, symbols: list[str], **kwargs: Any) -> list[StockData]:
        """Thu thập dữ liệu từ VNStock API"""
        data = []
        data_type = kwargs.get("data_type", DataType.COMPANIES)

        for symbol in symbols:
            try:
                # Lấy thông tin công ty
                if data_type == DataType.COMPANIES:
                    endpoint = f"/stock/{symbol}/overview"
                elif data_type == DataType.NEWS:
                    endpoint = f"/stock/{symbol}/news"
                elif data_type == DataType.EVENTS:
                    endpoint = f"/stock/{symbol}/events"
                else:
                    endpoint = f"/stock/{symbol}/info"

                response_data = await self._make_request(endpoint)

                stock_data = StockData(
                    symbol=symbol,
                    data_type=data_type,
                    source=self.data_source,
                    timestamp=datetime.now(),
                    data=response_data,
                    metadata={"source_name": self.source_config.name, "api_endpoint": endpoint},
                )
                data.append(stock_data)

            except Exception as e:
                logger.error(f"Failed to collect VNStock data for {symbol}: {e}")
                continue

        return data


class FinhubAPICollector(HTTPAPICollector):
    """Collector cho Finhub API"""

    def __init__(self, api_key: str):
        # Sử dụng cơ chế tự động chọn rate limit config
        source_config = config.get_api_source_config(APISource.FINHUB)
        super().__init__(
            source_config=source_config, base_url="https://finnhub.io/api/v1", headers={"X-Finnhub-Token": api_key}
        )

    async def _collect_data(self, symbols: list[str], **kwargs: Any) -> list[StockData]:
        """Thu thập dữ liệu từ Finhub API"""
        data = []
        data_type = kwargs.get("data_type", DataType.COMPANIES)

        for symbol in symbols:
            try:
                # Mapping data types to endpoints
                if data_type == DataType.COMPANIES:
                    endpoint = "/stock/profile2"
                    params = {"symbol": symbol}
                elif data_type == DataType.NEWS:
                    endpoint = "/company-news"
                    params = {
                        "symbol": symbol,
                        "from": kwargs.get("from_date", "2024-01-01"),
                        "to": kwargs.get("to_date", "2024-12-31"),
                    }
                elif data_type == DataType.OFFICERS:
                    endpoint = "/stock/executive"
                    params = {"symbol": symbol}
                else:
                    endpoint = "/quote"
                    params = {"symbol": symbol}

                response_data = await self._make_request(endpoint, params)

                stock_data = StockData(
                    symbol=symbol,
                    data_type=data_type,
                    source=self.data_source,
                    timestamp=datetime.now(),
                    data=response_data,
                    metadata={"source_name": self.source_config.name, "api_endpoint": endpoint, "params": params},
                )
                data.append(stock_data)

            except Exception as e:
                logger.error(f"Failed to collect Finhub data for {symbol}: {e}")
                continue

        return data


class AlphaVantageAPICollector(HTTPAPICollector):
    """Collector cho Alpha Vantage API"""

    def __init__(self, api_key: str):
        source_config = config.get_api_source_config(APISource.ALPHA_VANTAGE)
        super().__init__(
            source_config=source_config,
            base_url="https://www.alphavantage.co/query",
            headers={"User-Agent": "StockTracker/1.0"},
        )
        self.api_key = api_key

    async def _collect_data(self, symbols: list[str], **kwargs: Any) -> list[StockData]:
        """Thu thập dữ liệu từ Alpha Vantage API"""
        data = []
        data_type = kwargs.get("data_type", DataType.COMPANIES)

        for symbol in symbols:
            try:
                if data_type == DataType.COMPANIES:
                    endpoint = ""
                    params = {"function": "OVERVIEW", "symbol": symbol, "apikey": self.api_key}
                else:
                    endpoint = ""
                    params = {"function": "TIME_SERIES_DAILY", "symbol": symbol, "apikey": self.api_key}

                response_data = await self._make_request(endpoint, params)

                stock_data = StockData(
                    symbol=symbol,
                    data_type=data_type,
                    source=self.data_source,
                    timestamp=datetime.now(),
                    data=response_data,
                    metadata={"source_name": self.source_config.name, "api_function": params.get("function")},
                )
                data.append(stock_data)

            except Exception as e:
                logger.error(f"Failed to collect Alpha Vantage data for {symbol}: {e}")
                continue

        return data


class MultiAPICollector(BaseCollector):
    """
    Collector tổng hợp cho nhiều API sources
    Có thể thu thập từ nhiều API cùng lúc
    """

    def __init__(self, collectors: list[HTTPAPICollector]):
        # Sử dụng config của collector đầu tiên làm default
        super().__init__(collectors[0].source_config if collectors else DataSourceConfig(name="multi_api"))
        self.collectors = collectors
        self._collector_semaphore = asyncio.Semaphore(len(collectors))

    @property
    def data_source(self) -> DataSource:
        return DataSource.API

    async def _collect_data(self, symbols: list[str], **kwargs: Any) -> list[StockData]:
        """Thu thập dữ liệu từ tất cả collectors song song"""
        all_data = []

        # Tạo tasks cho từng collector
        tasks = []
        for collector in self.collectors:
            if collector.source_config.enabled:
                task = asyncio.create_task(
                    collector._collect_data(symbols, **kwargs), name=f"collect_{collector.source_config.name}"
                )
                tasks.append(task)

        # Chờ tất cả tasks hoàn thành
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Multi-API collection error: {result}")
                elif isinstance(result, list):
                    all_data.extend(result)

        return all_data

    async def start(self) -> None:
        """Khởi động tất cả sub-collectors"""
        await super().start()
        for collector in self.collectors:
            await collector.start()

    async def stop(self) -> None:
        """Dừng tất cả sub-collectors"""
        await super().stop()
        for collector in self.collectors:
            await collector.stop()


class WebhookAPICollector(BaseCollector):
    """
    Collector cho việc nhận dữ liệu từ webhooks
    Chạy HTTP server để nhận push notifications
    """

    def __init__(self, port: int = 8080, webhook_path: str = "/webhook"):
        # Tạo config mới với rate limit tự động cho webhook
        source_config = config.get_or_create_data_source_config("webhook_api", DataSource.API)
        super().__init__(source_config)
        self.port = port
        self.webhook_path = webhook_path
        self._app: aiohttp.web.Application | None = None
        self._runner: aiohttp.web.AppRunner | None = None
        self._received_data: list[StockData] = []
        self._data_lock = asyncio.Lock()

    @property
    def data_source(self) -> DataSource:
        return DataSource.API

    async def _webhook_handler(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """Xử lý webhook requests"""
        try:
            data = await request.json()

            # Parse incoming data thành StockData
            stock_data = StockData(
                symbol=data.get("symbol", "UNKNOWN"),
                data_type=DataType(data.get("data_type", "companies")),
                source=self.data_source,
                timestamp=datetime.now(),
                data=data.get("data", {}),
                metadata={
                    "source_name": self.source_config.name,
                    "webhook_path": self.webhook_path,
                    "remote_addr": request.remote,
                },
            )

            async with self._data_lock:
                self._received_data.append(stock_data)

            logger.info(f"Received webhook data for symbol: {stock_data.symbol}")
            return aiohttp.web.json_response({"status": "success"})

        except Exception as e:
            logger.error(f"Webhook handling error: {e}")
            return aiohttp.web.json_response({"status": "error", "message": str(e)}, status=400)

    async def _collect_data(self, symbols: list[str], **kwargs: Any) -> list[StockData]:
        """Trả về dữ liệu đã nhận từ webhooks"""
        async with self._data_lock:
            # Filter theo symbols nếu được cung cấp
            if symbols:
                filtered_data = [data for data in self._received_data if data.symbol in symbols]
            else:
                filtered_data = self._received_data.copy()

            # Clear received data sau khi collect
            if kwargs.get("clear_after_collect", True):
                self._received_data.clear()

            return filtered_data

    async def start(self) -> None:
        """Khởi động webhook server"""
        await super().start()

        self._app = aiohttp.web.Application()
        self._app.router.add_post(self.webhook_path, self._webhook_handler)

        self._runner = aiohttp.web.AppRunner(self._app)
        await self._runner.setup()

        site = aiohttp.web.TCPSite(self._runner, "0.0.0.0", self.port)
        await site.start()

        logger.info(f"Webhook server started on port {self.port}, path: {self.webhook_path}")

    async def stop(self) -> None:
        """Dừng webhook server"""
        await super().stop()

        if self._runner:
            await self._runner.cleanup()
            self._runner = None

        logger.info("Webhook server stopped")

"""
Library Collectors cho việc thu thập dữ liệu từ Python libraries
"""

import asyncio
from collections.abc import Callable
from datetime import datetime
import importlib
from typing import Any

from loguru import logger

from ..core.base import BaseCollector
from ..core.config import DataSourceConfig, config
from ..core.models import DataSource, DataType, LibrarySource, StockData


class PythonLibraryCollector(BaseCollector):
    """Base collector cho Python libraries"""

    def __init__(self, source_config: DataSourceConfig, library_name: str):
        super().__init__(source_config)
        self.library_name = library_name
        self._library_module = None
        self._executor = None

    @property
    def data_source(self) -> DataSource:
        return DataSource.LIBRARY

    async def _import_library(self) -> Any:
        """Lazy import library để tránh blocking"""
        if self._library_module is None:
            try:
                # Sử dụng thread executor cho blocking imports
                loop = asyncio.get_event_loop()
                self._library_module = await loop.run_in_executor(
                    self._executor, importlib.import_module, self.library_name
                )
                logger.info(f"Successfully imported library: {self.library_name}")
            except ImportError as e:
                logger.error(f"Failed to import library {self.library_name}: {e}")
                raise
        return self._library_module

    async def _run_in_executor(self, func: Callable, *args: Any) -> Any:
        """Chạy blocking function trong executor"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, func, *args)

    async def start(self) -> None:
        """Khởi tạo executor và import library"""
        await super().start()
        # Tạo dedicated thread pool cho library operations
        import concurrent.futures

        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=config.concurrency.max_workers,
            thread_name_prefix=f"{config.concurrency.prefix_thread_name}{self.library_name}_",
        )
        await self._import_library()

    async def stop(self) -> None:
        """Cleanup executor"""
        await super().stop()
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None


class YFinanceCollector(PythonLibraryCollector):
    """Collector cho yfinance library"""

    def __init__(self):
        # Sử dụng cơ chế tự động chọn rate limit config
        source_config = config.get_library_source_config(LibrarySource.YFINANCE)
        super().__init__(source_config, "yfinance")

    async def _collect_data(self, symbols: list[str], **kwargs: Any) -> list[StockData]:
        """Thu thập dữ liệu từ yfinance"""
        yf = await self._import_library()
        data = []
        data_type = kwargs.get("data_type", DataType.COMPANIES)

        for symbol in symbols:
            try:
                # Tạo ticker object
                ticker = await self._run_in_executor(yf.Ticker, symbol)

                # Thu thập data based on data_type
                if data_type == DataType.COMPANIES:
                    # Lấy thông tin công ty
                    info_data = await self._run_in_executor(lambda: ticker.info)

                elif data_type == DataType.NEWS:
                    # Lấy tin tức
                    info_data = await self._run_in_executor(lambda: ticker.news)

                elif data_type == DataType.OFFICERS:
                    # Lấy thông tin leadership
                    major_holders = await self._run_in_executor(lambda: ticker.major_holders)
                    institutional_holders = await self._run_in_executor(lambda: ticker.institutional_holders)
                    info_data = {
                        "major_holders": major_holders.to_dict()
                        if hasattr(major_holders, "to_dict")
                        else str(major_holders),
                        "institutional_holders": institutional_holders.to_dict()
                        if hasattr(institutional_holders, "to_dict")
                        else str(institutional_holders),
                    }

                else:
                    # Default: historical data
                    period = kwargs.get("period", "1y")
                    hist_data = await self._run_in_executor(lambda: ticker.history(period=period))
                    info_data = hist_data.to_dict() if hasattr(hist_data, "to_dict") else str(hist_data)

                stock_data = StockData(
                    symbol=symbol,
                    data_type=data_type,
                    source=self.data_source,
                    timestamp=datetime.now(),
                    data=info_data,
                    metadata={
                        "source_name": self.source_config.name,
                        "library": self.library_name,
                        "data_type_requested": data_type.value,
                    },
                )
                data.append(stock_data)

            except Exception as e:
                logger.error(f"Failed to collect yfinance data for {symbol}: {e}")
                continue

        return data


class VNStockLibraryCollector(PythonLibraryCollector):
    """Collector cho vnstock library"""

    def __init__(self):
        # Sử dụng cơ chế tự động chọn rate limit config
        source_config = config.get_library_source_config(LibrarySource.VNSTOCK_LIB)
        super().__init__(source_config, "vnstock")

    async def _collect_data(self, symbols: list[str], **kwargs: Any) -> list[StockData]:
        """Thu thập dữ liệu từ vnstock library"""
        vnstock = await self._import_library()
        data = []
        data_type = kwargs.get("data_type", DataType.COMPANIES)

        for symbol in symbols:
            try:
                if data_type == DataType.COMPANIES:
                    # Lấy thông tin công ty
                    company_data = await self._run_in_executor(vnstock.stock_overview, symbol)

                elif data_type == DataType.SHAREHOLDERS:
                    # Lấy thông tin cổ đông
                    company_data = await self._run_in_executor(vnstock.stock_ownership, symbol)

                elif data_type == DataType.OFFICERS:
                    # Lấy thông tin lãnh đạo
                    company_data = await self._run_in_executor(vnstock.stock_leadership, symbol)

                elif data_type == DataType.SUBSIDIARIES:
                    # Lấy thông tin công ty con
                    company_data = await self._run_in_executor(vnstock.stock_subsidiaries, symbol)

                elif data_type == DataType.EVENTS:
                    # Lấy sự kiện công ty
                    company_data = await self._run_in_executor(vnstock.stock_events, symbol)

                else:
                    # Default: historical price data
                    start_date = kwargs.get("start_date", "2024-01-01")
                    end_date = kwargs.get("end_date", "2024-12-31")
                    company_data = await self._run_in_executor(
                        vnstock.stock_historical_data, symbol, start_date, end_date
                    )

                # Convert pandas DataFrame to dict if needed
                if hasattr(company_data, "to_dict"):
                    data_dict = company_data.to_dict()
                else:
                    data_dict = company_data

                stock_data = StockData(
                    symbol=symbol,
                    data_type=data_type,
                    source=self.data_source,
                    timestamp=datetime.now(),
                    data=data_dict,
                    metadata={
                        "source_name": self.source_config.name,
                        "library": self.library_name,
                        "data_type_requested": data_type.value,
                    },
                )
                data.append(stock_data)

            except Exception as e:
                logger.error(f"Failed to collect vnstock data for {symbol}: {e}")
                continue

        return data


class PandasDataReaderCollector(PythonLibraryCollector):
    """Collector cho pandas-datareader library"""

    def __init__(self, data_source_name: str = "yahoo"):
        # Sử dụng cơ chế tự động chọn rate limit config
        source_config = config.get_library_source_config(LibrarySource.PANDAS_DATAREADER)
        super().__init__(source_config, "pandas_datareader")
        self.data_source_name = data_source_name

    async def _collect_data(self, symbols: list[str], **kwargs: Any) -> list[StockData]:
        """Thu thập dữ liệu từ pandas-datareader"""
        pdr = await self._import_library()
        data = []
        data_type = kwargs.get("data_type", DataType.COMPANIES)

        start_date = kwargs.get("start_date", "2024-01-01")
        end_date = kwargs.get("end_date", "2024-12-31")

        for symbol in symbols:
            try:
                # Thu thập historical data
                historical_data = await self._run_in_executor(pdr.get_data_yahoo, symbol, start_date, end_date)

                # Convert to dict
                data_dict = historical_data.to_dict() if hasattr(historical_data, "to_dict") else {}

                stock_data = StockData(
                    symbol=symbol,
                    data_type=data_type,
                    source=self.data_source,
                    timestamp=datetime.now(),
                    data=data_dict,
                    metadata={
                        "source_name": self.source_config.name,
                        "library": self.library_name,
                        "data_source": self.data_source_name,
                        "start_date": start_date,
                        "end_date": end_date,
                    },
                )
                data.append(stock_data)

            except Exception as e:
                logger.error(f"Failed to collect pandas-datareader data for {symbol}: {e}")
                continue

        return data


class QuandlCollector(PythonLibraryCollector):
    """Collector cho Quandl library"""

    def __init__(self, api_key: str):
        source_config = config.get_library_source_config(LibrarySource.QUANDL)
        super().__init__(source_config, "quandl")
        self.api_key = api_key

    async def _collect_data(self, symbols: list[str], **kwargs: Any) -> list[StockData]:
        """Thu thập dữ liệu từ Quandl"""
        quandl = await self._import_library()
        data = []
        data_type = kwargs.get("data_type", DataType.COMPANIES)

        # Set API key
        quandl.ApiConfig.api_key = self.api_key

        for symbol in symbols:
            try:
                # Thu thập data từ Quandl
                dataset_code = f"WIKI/{symbol}" if kwargs.get("use_wiki", True) else symbol
                start_date = kwargs.get("start_date", "2024-01-01")
                end_date = kwargs.get("end_date", "2024-12-31")

                quandl_data = await self._run_in_executor(
                    quandl.get, dataset_code, start_date=start_date, end_date=end_date
                )

                # Convert to dict
                data_dict = quandl_data.to_dict() if hasattr(quandl_data, "to_dict") else {}

                stock_data = StockData(
                    symbol=symbol,
                    data_type=data_type,
                    source=self.data_source,
                    timestamp=datetime.now(),
                    data=data_dict,
                    metadata={
                        "source_name": self.source_config.name,
                        "library": self.library_name,
                        "dataset_code": dataset_code,
                        "start_date": start_date,
                        "end_date": end_date,
                    },
                )
                data.append(stock_data)

            except Exception as e:
                logger.error(f"Failed to collect Quandl data for {symbol}: {e}")
                continue

        return data


class MultiLibraryCollector(BaseCollector):
    """
    Collector tổng hợp cho nhiều library sources
    Có thể thu thập từ nhiều libraries cùng lúc
    """

    def __init__(self, collectors: list[PythonLibraryCollector]):
        # Sử dụng config của collector đầu tiên làm default
        super().__init__(collectors[0].source_config if collectors else DataSourceConfig(name="multi_library"))
        self.collectors = collectors
        self._collector_semaphore = asyncio.Semaphore(len(collectors))

    @property
    def data_source(self) -> DataSource:
        return DataSource.LIBRARY

    async def _collect_data(self, symbols: list[str], **kwargs: Any) -> list[StockData]:
        """Thu thập dữ liệu từ tất cả library collectors song song"""
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
                    logger.error(f"Multi-library collection error: {result}")
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


class CustomLibraryCollector(PythonLibraryCollector):
    """
    Generic collector cho bất kỳ Python library nào
    Cho phép custom function calls
    """

    def __init__(
        self, source_config: DataSourceConfig, library_name: str, collection_functions: dict[DataType, Callable]
    ):
        super().__init__(source_config, library_name)
        self.collection_functions = collection_functions

    async def _collect_data(self, symbols: list[str], **kwargs: Any) -> list[StockData]:
        """Thu thập dữ liệu sử dụng custom functions"""
        library = await self._import_library()
        data = []
        data_type = kwargs.get("data_type", DataType.COMPANIES)

        # Lấy function cho data_type
        collection_func = self.collection_functions.get(data_type)
        if not collection_func:
            logger.warning(f"No collection function defined for {data_type}")
            return data

        for symbol in symbols:
            try:
                # Chạy custom function
                result = await self._run_in_executor(collection_func, library, symbol, **kwargs)

                stock_data = StockData(
                    symbol=symbol,
                    data_type=data_type,
                    source=self.data_source,
                    timestamp=datetime.now(),
                    data=result,
                    metadata={
                        "source_name": self.source_config.name,
                        "library": self.library_name,
                        "custom_function": collection_func.__name__,
                    },
                )
                data.append(stock_data)

            except Exception as e:
                logger.error(f"Failed to collect custom library data for {symbol}: {e}")
                continue

        return data

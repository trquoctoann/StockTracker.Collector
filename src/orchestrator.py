"""
Orchestrator chính cho việc điều phối thu thập và gửi dữ liệu
"""

import asyncio
from datetime import datetime
import signal
from typing import Any

from loguru import logger

from .collectors.api_collector import AlphaVantageAPICollector, FinhubAPICollector, VNStockAPICollector
from .collectors.library_collector import VNStockLibraryCollector, YFinanceCollector
from .core.base import BaseCollector, BaseSender
from .core.models import CollectorResult, DataType, SenderResult
from .sender.message_sender import KafkaSender, RabbitMQSender, RedisSender


class DataPipeline:
    """
    Pipeline cho việc thu thập và xử lý dữ liệu
    Thread-safe và async-ready
    """

    def __init__(
        self,
        name: str,
        collectors: list[BaseCollector],
        senders: list[BaseSender],
        symbols: list[str],
        data_types: list[DataType],
    ):
        self.name = name
        self.collectors = collectors
        self.senders = senders
        self.symbols = symbols
        self.data_types = data_types
        self._is_running = False
        self._pipeline_lock = asyncio.Lock()
        self._stats: dict[str, Any] = {
            "total_runs": 0,
            "successful_runs": 0,
            "total_collected": 0,
            "total_sent": 0,
            "last_run": None,
            "errors": [],
        }

    async def run_once(self, **kwargs: Any) -> dict[str, Any]:
        """Chạy pipeline một lần"""
        async with self._pipeline_lock:
            run_id = f"{self.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            logger.info(f"Starting pipeline run: {run_id}")

            try:
                self._stats["total_runs"] += 1
                self._stats["last_run"] = datetime.now()

                all_collected_data = []
                collection_results = {}

                # Thu thập dữ liệu từ tất cả collectors
                for data_type in self.data_types:
                    logger.info(f"Collecting {data_type.value} data for {len(self.symbols)} symbols")

                    # Tạo tasks cho tất cả collectors
                    collection_tasks = []
                    for collector in self.collectors:
                        if collector.source_config.enabled:
                            task = asyncio.create_task(
                                collector.collect(self.symbols, data_type=data_type, **kwargs),
                                name=f"collect_{collector.source_config.name}_{data_type.value}",
                            )
                            collection_tasks.append((collector.source_config.name, task))

                    # Chờ tất cả collection tasks
                    if collection_tasks:
                        completed_tasks = await asyncio.gather(
                            *[task for _, task in collection_tasks], return_exceptions=True
                        )

                        for (collector_name, _), result in zip(collection_tasks, completed_tasks, strict=False):
                            result_key = f"{collector_name}_{data_type.value}"

                            if isinstance(result, Exception):
                                logger.error(f"Collection failed for {result_key}: {result}")
                                collection_results[result_key] = CollectorResult(success=False, error=str(result))
                            else:
                                collection_results[result_key] = result
                                if result.success and result.data:
                                    all_collected_data.extend(result.data)
                                    self._stats["total_collected"] += len(result.data)

                logger.info(f"Collected {len(all_collected_data)} total data items")

                # Gửi dữ liệu qua tất cả senders
                send_results = {}
                if all_collected_data and self.senders:
                    # Tạo tasks cho tất cả senders
                    send_tasks = []
                    for sender in self.senders:
                        task = asyncio.create_task(sender.send(all_collected_data), name=f"send_{sender.name}")
                        send_tasks.append((sender.name, task))

                    # Chờ tất cả send tasks
                    if send_tasks:
                        completed_send_tasks = await asyncio.gather(
                            *[task for _, task in send_tasks], return_exceptions=True
                        )

                        for (sender_name, _), result in zip(send_tasks, completed_send_tasks, strict=False):
                            if isinstance(result, Exception):
                                logger.error(f"Send failed for {sender_name}: {result}")
                                send_results[sender_name] = SenderResult(success=False, error=str(result))
                            else:
                                send_results[sender_name] = result
                                if result.success:
                                    self._stats["total_sent"] += result.sent_count

                # Cập nhật stats
                success = len(all_collected_data) > 0
                if success:
                    self._stats["successful_runs"] += 1

                run_result = {
                    "run_id": run_id,
                    "success": success,
                    "collected_count": len(all_collected_data),
                    "collection_results": collection_results,
                    "send_results": send_results,
                    "duration": (datetime.now() - self._stats["last_run"]).total_seconds(),
                    "timestamp": datetime.now().isoformat(),
                }

                logger.info(f"Pipeline run {run_id} completed. Success: {success}")
                return run_result

            except Exception as e:
                error_msg = f"Pipeline run {run_id} failed: {e}"
                logger.error(error_msg)
                self._stats["errors"].append(
                    {"timestamp": datetime.now().isoformat(), "error": str(e), "run_id": run_id}
                )

                return {"run_id": run_id, "success": False, "error": str(e), "timestamp": datetime.now().isoformat()}

    async def start_collectors(self) -> None:
        """Khởi động tất cả collectors"""
        for collector in self.collectors:
            try:
                await collector.start()
            except Exception as e:
                logger.error(f"Failed to start collector {collector.source_config.name}: {e}")

    async def start_senders(self) -> None:
        """Khởi động tất cả senders"""
        for sender in self.senders:
            try:
                await sender.start()
            except Exception as e:
                logger.error(f"Failed to start sender {sender.name}: {e}")

    async def stop_all(self) -> None:
        """Dừng tất cả collectors và senders"""
        # Stop collectors
        for collector in self.collectors:
            try:
                await collector.stop()
            except Exception as e:
                logger.error(f"Failed to stop collector {collector.source_config.name}: {e}")

        # Stop senders
        for sender in self.senders:
            try:
                await sender.stop()
            except Exception as e:
                logger.error(f"Failed to stop sender {sender.name}: {e}")

    def get_stats(self) -> dict[str, Any]:
        """Lấy thống kê pipeline"""
        return {
            "name": self.name,
            "stats": self._stats.copy(),
            "collectors": [c.source_config.name for c in self.collectors],
            "senders": [s.name for s in self.senders],
            "symbols_count": len(self.symbols),
            "data_types": [dt.value for dt in self.data_types],
        }


class StockDataOrchestrator:
    """
    Orchestrator chính cho việc điều phối thu thập dữ liệu stock
    """

    def __init__(self):
        self.pipelines: dict[str, DataPipeline] = {}
        self._scheduler_task: asyncio.Task | None = None
        self._is_running = False
        self._shutdown_event = asyncio.Event()
        self._health_check_interval = 60  # seconds
        self._stats_lock = asyncio.Lock()

        # Global stats
        self._global_stats: dict[str, Any] = {
            "start_time": None,
            "total_pipelines": 0,
            "active_pipelines": 0,
            "total_runs": 0,
            "uptime_seconds": 0,
        }

    def add_pipeline(self, pipeline: DataPipeline) -> None:
        """Thêm pipeline vào orchestrator"""
        self.pipelines[pipeline.name] = pipeline
        self._global_stats["total_pipelines"] = len(self.pipelines)
        logger.info(f"Added pipeline: {pipeline.name}")

    def remove_pipeline(self, pipeline_name: str) -> bool:
        """Xóa pipeline khỏi orchestrator"""
        if pipeline_name in self.pipelines:
            del self.pipelines[pipeline_name]
            self._global_stats["total_pipelines"] = len(self.pipelines)
            logger.info(f"Removed pipeline: {pipeline_name}")
            return True
        return False

    async def run_pipeline(self, pipeline_name: str, **kwargs: Any) -> dict[str, Any] | None:
        """Chạy một pipeline cụ thể"""
        if pipeline_name not in self.pipelines:
            logger.error(f"Pipeline {pipeline_name} not found")
            return None

        pipeline = self.pipelines[pipeline_name]
        result = await pipeline.run_once(**kwargs)

        async with self._stats_lock:
            self._global_stats["total_runs"] += 1

        return result

    async def run_all_pipelines(self, **kwargs: Any) -> dict[str, dict[str, Any]]:
        """Chạy tất cả pipelines song song"""
        if not self.pipelines:
            logger.warning("No pipelines to run")
            return {}

        logger.info(f"Running {len(self.pipelines)} pipelines")

        # Tạo tasks cho tất cả pipelines
        tasks = []
        for name, pipeline in self.pipelines.items():
            task = asyncio.create_task(pipeline.run_once(**kwargs), name=f"pipeline_{name}")
            tasks.append((name, task))

        # Chờ tất cả pipelines hoàn thành
        results = {}
        if tasks:
            completed_tasks = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)

            for (name, _), result in zip(tasks, completed_tasks, strict=False):
                if isinstance(result, Exception):
                    logger.error(f"Pipeline {name} failed: {result}")
                    results[name] = {"success": False, "error": str(result), "timestamp": datetime.now().isoformat()}
                else:
                    results[name] = result

        async with self._stats_lock:
            self._global_stats["total_runs"] += len(results)

        return results

    async def schedule_pipelines(self, interval_minutes: int = 30, pipeline_names: list[str] | None = None) -> None:
        """
        Lên lịch chạy pipelines định kỳ
        """
        if pipeline_names is None:
            pipeline_names = list(self.pipelines.keys())

        logger.info(f"Starting scheduled execution every {interval_minutes} minutes")

        while not self._shutdown_event.is_set():
            try:
                # Chạy pipelines đã chọn
                if pipeline_names:
                    tasks = []
                    for name in pipeline_names:
                        if name in self.pipelines:
                            task = asyncio.create_task(self.run_pipeline(name), name=f"scheduled_{name}")
                            tasks.append(task)

                    if tasks:
                        await asyncio.gather(*tasks, return_exceptions=True)

                # Chờ interval tiếp theo hoặc shutdown signal
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=interval_minutes * 60)

            except TimeoutError:
                # Timeout bình thường, tiếp tục loop
                continue
            except Exception as e:
                logger.error(f"Error in scheduler: {e}")
                await asyncio.sleep(60)  # Wait before retry

    async def health_check(self) -> dict[str, Any]:
        """Kiểm tra trạng thái tổng thể của orchestrator"""
        async with self._stats_lock:
            uptime = (
                (datetime.now() - self._global_stats["start_time"]).total_seconds()
                if self._global_stats["start_time"]
                else 0
            )
            self._global_stats["uptime_seconds"] = uptime

            # Collect pipeline health
            pipeline_health = {}
            active_pipelines = 0

            for name, pipeline in self.pipelines.items():
                # Check collector health
                collector_health = {}
                for collector in pipeline.collectors:
                    try:
                        collector_health[collector.source_config.name] = await collector.health_check()
                    except Exception as e:
                        collector_health[collector.source_config.name] = {"error": str(e)}

                # Check sender health
                sender_health = {}
                for sender in pipeline.senders:
                    try:
                        sender_health[sender.name] = await sender.health_check()
                    except Exception as e:
                        sender_health[sender.name] = {"error": str(e)}

                pipeline_health[name] = {
                    "collectors": collector_health,
                    "senders": sender_health,
                    "stats": pipeline.get_stats(),
                }

                # Count active pipelines (simplified check)
                if any(c.source_config.enabled for c in pipeline.collectors):
                    active_pipelines += 1

            self._global_stats["active_pipelines"] = active_pipelines

            return {
                "orchestrator": {
                    "is_running": self._is_running,
                    "global_stats": self._global_stats.copy(),
                    "health_status": "healthy" if self._is_running else "stopped",
                },
                "pipelines": pipeline_health,
            }

    async def start(self) -> None:
        """Khởi động orchestrator"""
        logger.info("Starting Stock Data Orchestrator")
        self._is_running = True
        self._global_stats["start_time"] = datetime.now()

        # Start all pipelines
        for pipeline in self.pipelines.values():
            try:
                await pipeline.start_collectors()
                await pipeline.start_senders()
            except Exception as e:
                logger.error(f"Failed to start pipeline {pipeline.name}: {e}")

        logger.info("Stock Data Orchestrator started successfully")

    async def stop(self) -> None:
        """Dừng orchestrator"""
        logger.info("Stopping Stock Data Orchestrator")
        self._is_running = False
        self._shutdown_event.set()

        # Stop scheduler if running
        if self._scheduler_task and not self._scheduler_task.done():
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

        # Stop all pipelines
        for pipeline in self.pipelines.values():
            try:
                await pipeline.stop_all()
            except Exception as e:
                logger.error(f"Failed to stop pipeline {pipeline.name}: {e}")

        logger.info("Stock Data Orchestrator stopped")

    async def run_with_scheduler(self, interval_minutes: int = 30, pipeline_names: list[str] | None = None) -> None:
        """
        Chạy orchestrator với scheduler
        """
        await self.start()

        # Setup signal handlers for graceful shutdown
        def signal_handler():
            logger.info("Received shutdown signal")
            self._shutdown_event.set()

        # Register signal handlers
        for sig in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_event_loop().add_signal_handler(sig, signal_handler)

        try:
            # Start scheduler
            self._scheduler_task = asyncio.create_task(self.schedule_pipelines(interval_minutes, pipeline_names))

            # Wait for shutdown signal
            await self._shutdown_event.wait()

        finally:
            await self.stop()


def create_default_orchestrator(
    symbols: list[str], finhub_api_key: str | None = None, alpha_vantage_api_key: str | None = None
) -> StockDataOrchestrator:
    """
    Tạo orchestrator mặc định với các collectors và senders phổ biến
    """
    orchestrator = StockDataOrchestrator()

    # Tạo collectors
    collectors = []

    # API Collectors
    try:
        vnstock_api = VNStockAPICollector()
        collectors.append(vnstock_api)
        logger.info("✓ VNStock API collector created")
    except Exception as e:
        logger.warning(f"Failed to create VNStock API collector: {e}")

    if finhub_api_key:
        try:
            finhub_api = FinhubAPICollector(finhub_api_key)
            collectors.append(finhub_api)
            logger.info("✓ Finhub API collector created")
        except Exception as e:
            logger.warning(f"Failed to create Finhub API collector: {e}")

    if alpha_vantage_api_key:
        try:
            alpha_vantage_api = AlphaVantageAPICollector(alpha_vantage_api_key)
            collectors.append(alpha_vantage_api)
            logger.info("✓ Alpha Vantage API collector created")
        except Exception as e:
            logger.warning(f"Failed to create Alpha Vantage API collector: {e}")

    # Library Collectors
    try:
        yfinance_lib = YFinanceCollector()
        collectors.append(yfinance_lib)
        logger.info("✓ YFinance collector created")
    except Exception as e:
        logger.warning(f"Failed to create YFinance collector: {e}")

    try:
        vnstock_lib = VNStockLibraryCollector()
        collectors.append(vnstock_lib)
        logger.info("✓ VNStock library collector created")
    except Exception as e:
        logger.warning(f"Failed to create VNStock library collector: {e}")

    # Tạo senders
    senders = []

    # Message broker sender (Kafka hoặc RabbitMQ)
    from .core.config import config

    if config.message_broker.is_kafka:
        try:
            kafka_sender = KafkaSender()
            senders.append(kafka_sender)
            logger.info("✓ Kafka sender created")
        except Exception as e:
            logger.warning(f"Failed to create Kafka sender: {e}")
    elif config.message_broker.is_rabbitmq:
        try:
            rabbitmq_sender = RabbitMQSender()
            senders.append(rabbitmq_sender)
            logger.info("✓ RabbitMQ sender created")
        except Exception as e:
            logger.warning(f"Failed to create RabbitMQ sender: {e}")
    else:
        logger.warning(f"Unknown message broker type: {config.message_broker.type}")

    # Redis sender
    try:
        redis_sender = RedisSender()
        senders.append(redis_sender)
        logger.info("✓ Redis sender created")
    except Exception as e:
        logger.warning(f"Failed to create Redis sender: {e}")

    # File sender as fallback
    try:
        from .sender.message_sender import FileSender

        file_sender = FileSender("output/stock_data.jsonl", "json")
        senders.append(file_sender)
        logger.info("✓ File sender created")
    except Exception as e:
        logger.warning(f"Failed to create File sender: {e}")

    # Tạo pipeline chính
    if collectors:
        main_pipeline = DataPipeline(
            name="main_stock_pipeline",
            collectors=collectors,
            senders=senders,
            symbols=symbols,
            data_types=[DataType.COMPANIES, DataType.NEWS, DataType.EVENTS],
        )

        orchestrator.add_pipeline(main_pipeline)
        logger.info(f"✓ Main pipeline created with {len(collectors)} collectors and {len(senders)} senders")
    else:
        logger.error("No collectors available - cannot create pipeline")

    return orchestrator

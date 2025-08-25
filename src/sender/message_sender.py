"""
Message Senders cho việc gửi dữ liệu đến message brokers và 3rd party APIs
"""

import asyncio
from datetime import datetime
import json

import aiohttp
from loguru import logger

from ..core.base import BaseSender
from ..core.config import config
from ..core.models import SenderResult, StockData


class KafkaSender(BaseSender):
    """Sender cho Apache Kafka"""

    def __init__(self, bootstrap_servers: str | None = None, topic: str | None = None):
        super().__init__("kafka_sender")
        self.bootstrap_servers = bootstrap_servers or config.message_broker.kafka_hosts
        self.topic = topic or config.message_broker.kafka_topic
        self.batch_size = config.message_broker.batch_size
        self._producer = None

    async def _get_producer(self):
        """Lazy initialization của Kafka producer"""
        if self._producer is None:
            try:
                from aiokafka import AIOKafkaProducer

                self._producer = AIOKafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                    **config.message_broker.kafka_config,
                )
                await self._producer.start()
                logger.info(f"Kafka producer started, servers: {self.bootstrap_servers}")
            except ImportError:
                logger.error("aiokafka not installed. Install with: pip install aiokafka")
                raise
            except Exception as e:
                logger.error(f"Failed to start Kafka producer: {e}")
                raise
        return self._producer

    async def _send_data(self, data: list[StockData]) -> SenderResult:
        """Gửi dữ liệu đến Kafka"""
        try:
            producer = await self._get_producer()
            sent_count = 0

            # Send data in batches
            for i in range(0, len(data), self.batch_size):
                batch = data[i : i + self.batch_size]

                # Create tasks for parallel sending
                tasks = []
                for stock_data in batch:
                    # Tạo message với partition key based on symbol
                    message_value = stock_data.to_dict()
                    task = producer.send(self.topic, value=message_value, key=stock_data.symbol.encode("utf-8"))
                    tasks.append(task)

                # Wait for all messages in batch to be sent
                await asyncio.gather(*tasks)
                sent_count += len(batch)

                logger.debug(f"Sent batch of {len(batch)} messages to Kafka")

            # Ensure all messages are sent
            await producer.flush()

            return SenderResult(
                success=True,
                sent_count=sent_count,
                metadata={
                    "topic": self.topic,
                    "bootstrap_servers": self.bootstrap_servers,
                    "batch_size": self.batch_size,
                },
            )

        except Exception as e:
            logger.error(f"Failed to send data to Kafka: {e}")
            return SenderResult(success=False, error=str(e), metadata={"topic": self.topic})

    async def start(self) -> None:
        """Khởi tạo Kafka producer"""
        await super().start()
        await self._get_producer()

    async def stop(self) -> None:
        """Dừng Kafka producer"""
        await super().stop()
        if self._producer:
            await self._producer.stop()
            self._producer = None


class RabbitMQSender(BaseSender):
    """Sender cho RabbitMQ"""

    def __init__(
        self,
        connection_url: str | None = None,
        exchange: str | None = None,
        exchange_type: str | None = None,
        routing_key: str | None = None,
        queue: str | None = None,
        durable: bool | None = None,
    ):
        super().__init__("rabbitmq_sender")
        self.connection_url = connection_url or config.message_broker.rabbitmq_url
        self.exchange = exchange or config.message_broker.rabbitmq_exchange
        self.exchange_type = exchange_type or config.message_broker.rabbitmq_exchange_type
        self.routing_key = routing_key or config.message_broker.rabbitmq_routing_key
        self.queue = queue or config.message_broker.rabbitmq_queue
        self.durable = durable if durable is not None else config.message_broker.rabbitmq_durable
        self.batch_size = config.message_broker.batch_size
        self._connection = None
        self._channel = None
        self._exchange_obj = None

    async def _get_connection(self):
        """Lazy initialization của RabbitMQ connection"""
        if self._connection is None or self._connection.is_closed:
            try:
                import aio_pika

                self._connection = await aio_pika.connect_robust(
                    self.connection_url, **config.message_broker.rabbitmq_config
                )
                logger.info(f"RabbitMQ connection established: {self.connection_url}")
            except ImportError:
                logger.error("aio-pika not installed. Install with: pip install aio-pika")
                raise
            except Exception as e:
                logger.error(f"Failed to connect to RabbitMQ: {e}")
                raise
        return self._connection

    async def _get_channel(self):
        """Lazy initialization của RabbitMQ channel"""
        if self._channel is None or self._channel.is_closed:
            connection = await self._get_connection()
            self._channel = await connection.channel()
            await self._channel.set_qos(prefetch_count=self.batch_size)
        return self._channel

    async def _get_exchange(self):
        """Lazy initialization của RabbitMQ exchange"""
        if self._exchange_obj is None:
            import aio_pika

            channel = await self._get_channel()
            self._exchange_obj = await channel.declare_exchange(
                self.exchange, type=aio_pika.ExchangeType(self.exchange_type), durable=self.durable
            )

            # Declare queue and bind to exchange
            queue = await channel.declare_queue(self.queue, durable=self.durable)
            await queue.bind(self._exchange_obj, routing_key=self.routing_key)
            logger.info(f"RabbitMQ exchange '{self.exchange}' and queue '{self.queue}' ready")

        return self._exchange_obj

    async def _send_data(self, data: list[StockData]) -> SenderResult:
        """Gửi dữ liệu đến RabbitMQ"""
        try:
            import aio_pika

            exchange = await self._get_exchange()
            sent_count = 0

            # Send data in batches
            for i in range(0, len(data), self.batch_size):
                batch = data[i : i + self.batch_size]

                # Create tasks for parallel sending
                tasks = []
                for stock_data in batch:
                    # Tạo message với routing key based on symbol
                    message_body = stock_data.to_json().encode("utf-8")
                    routing_key = f"{self.routing_key}.{stock_data.symbol}"

                    message = aio_pika.Message(
                        message_body,
                        headers={
                            "symbol": stock_data.symbol,
                            "data_type": stock_data.data_type.value,
                            "source": stock_data.source.value,
                            "timestamp": stock_data.timestamp.isoformat(),
                        },
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                        if self.durable
                        else aio_pika.DeliveryMode.NOT_PERSISTENT,
                    )

                    task = exchange.publish(message, routing_key=routing_key)
                    tasks.append(task)

                # Wait for all messages in batch to be sent
                await asyncio.gather(*tasks)
                sent_count += len(batch)

                logger.debug(f"Sent batch of {len(batch)} messages to RabbitMQ")

            return SenderResult(
                success=True,
                sent_count=sent_count,
                metadata={
                    "connection_url": self.connection_url.split("@")[-1]
                    if "@" in self.connection_url
                    else self.connection_url,  # Hide credentials
                    "exchange": self.exchange,
                    "routing_key": self.routing_key,
                    "queue": self.queue,
                    "batch_size": self.batch_size,
                },
            )

        except Exception as e:
            logger.error(f"Failed to send data to RabbitMQ: {e}")
            return SenderResult(
                success=False,
                error=str(e),
                metadata={
                    "exchange": self.exchange,
                    "queue": self.queue,
                },
            )

    async def start(self) -> None:
        """Khởi tạo RabbitMQ connection"""
        await super().start()
        await self._get_exchange()

    async def stop(self) -> None:
        """Dừng RabbitMQ connection"""
        await super().stop()
        if self._channel and not self._channel.is_closed:
            await self._channel.close()
            self._channel = None
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
            self._connection = None
        self._exchange_obj = None


class RedisSender(BaseSender):
    """Sender cho Redis"""

    def __init__(self, redis_url: str | None = None, key_prefix: str = "stock_data"):
        super().__init__("redis_sender")
        self.redis_url = redis_url or "redis://localhost:6379"
        self.key_prefix = key_prefix
        self._redis = None

    async def _get_redis(self):
        """Lazy initialization của Redis connection"""
        if self._redis is None:
            try:
                import aioredis

                self._redis = await aioredis.from_url(self.redis_url)
                logger.info(f"Redis connection established: {self.redis_url}")
            except ImportError:
                logger.error("aioredis not installed. Install with: pip install aioredis")
                raise
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {e}")
                raise
        return self._redis

    async def _send_data(self, data: list[StockData]) -> SenderResult:
        """Gửi dữ liệu đến Redis"""
        try:
            redis = await self._get_redis()
            sent_count = 0

            # Use pipeline for better performance
            pipe = redis.pipeline()

            for stock_data in data:
                # Tạo key dựa trên symbol và timestamp
                timestamp = stock_data.timestamp.strftime("%Y%m%d_%H%M%S")
                key = f"{self.key_prefix}:{stock_data.symbol}:{timestamp}"

                # Store as JSON
                value = stock_data.to_json()
                pipe.set(key, value)

                # Set expiration (1 week)
                pipe.expire(key, 7 * 24 * 3600)

                # Add to symbol-based list for easy retrieval
                list_key = f"{self.key_prefix}:symbols:{stock_data.symbol}"
                pipe.lpush(list_key, key)
                pipe.ltrim(list_key, 0, 999)  # Keep only last 1000 entries

                sent_count += 1

            # Execute all commands
            await pipe.execute()

            return SenderResult(
                success=True,
                sent_count=sent_count,
                metadata={"redis_url": self.redis_url, "key_prefix": self.key_prefix},
            )

        except Exception as e:
            logger.error(f"Failed to send data to Redis: {e}")
            return SenderResult(success=False, error=str(e), metadata={"redis_url": self.redis_url})

    async def stop(self) -> None:
        """Đóng Redis connection"""
        await super().stop()
        if self._redis:
            await self._redis.close()
            self._redis = None


class HTTPAPISender(BaseSender):
    """Sender cho 3rd party HTTP APIs"""

    def __init__(self, api_url: str, headers: dict[str, str] | None = None, auth_token: str | None = None):
        super().__init__("http_api_sender")
        self.api_url = api_url
        self.headers = headers or {}
        if auth_token:
            self.headers["Authorization"] = f"Bearer {auth_token}"
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Lazy initialization của HTTP session"""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(timeout=timeout, headers=self.headers)
        return self._session

    async def _send_data(self, data: list[StockData]) -> SenderResult:
        """Gửi dữ liệu đến HTTP API"""
        try:
            session = await self._get_session()
            sent_count = 0

            # Prepare payload
            payload = {
                "data": [stock_data.to_dict() for stock_data in data],
                "timestamp": datetime.now().isoformat(),
                "source": "stock_tracker_collector",
            }

            async with session.post(self.api_url, json=payload) as response:
                response.raise_for_status()

                # Try to parse response
                try:
                    response_data = await response.json()
                except:
                    response_data = {"status": "sent"}

                sent_count = len(data)

                return SenderResult(
                    success=True,
                    sent_count=sent_count,
                    metadata={
                        "api_url": self.api_url,
                        "response_status": response.status,
                        "response_data": response_data,
                    },
                )

        except aiohttp.ClientError as e:
            logger.error(f"HTTP API request failed: {e}")
            return SenderResult(success=False, error=f"HTTP error: {e!s}", metadata={"api_url": self.api_url})
        except Exception as e:
            logger.error(f"Failed to send data to HTTP API: {e}")
            return SenderResult(success=False, error=str(e), metadata={"api_url": self.api_url})

    async def stop(self) -> None:
        """Cleanup HTTP session"""
        await super().stop()
        if self._session and not self._session.closed:
            await self._session.close()


class DatabaseSender(BaseSender):
    """Sender cho PostgreSQL database"""

    def __init__(self, database_url: str | None = None, table_name: str = "stock_data"):
        super().__init__("database_sender")
        self.database_url = database_url or "postgresql://localhost/stocktracker"
        self.table_name = table_name
        self._pool = None

    async def _get_pool(self):
        """Lazy initialization của connection pool"""
        if self._pool is None:
            try:
                import asyncpg

                self._pool = await asyncpg.create_pool(self.database_url, min_size=1, max_size=10)
                logger.info("Database connection pool created")
            except ImportError:
                logger.error("asyncpg not installed. Install with: pip install asyncpg")
                raise
            except Exception as e:
                logger.error(f"Failed to create database pool: {e}")
                raise
        return self._pool

    async def _send_data(self, data: list[StockData]) -> SenderResult:
        """Gửi dữ liệu đến database"""
        try:
            pool = await self._get_pool()
            sent_count = 0

            async with pool.acquire() as conn:
                # Prepare batch insert
                insert_query = f"""
                    INSERT INTO {self.table_name}
                    (symbol, data_type, source, timestamp, data, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6)
                """

                # Prepare data for batch insert
                records = []
                for stock_data in data:
                    records.append(
                        (
                            stock_data.symbol,
                            stock_data.data_type.value,
                            stock_data.source.value,
                            stock_data.timestamp,
                            json.dumps(stock_data.data, ensure_ascii=False),
                            json.dumps(stock_data.metadata, ensure_ascii=False),
                        )
                    )

                # Execute batch insert
                await conn.executemany(insert_query, records)
                sent_count = len(records)

                return SenderResult(
                    success=True,
                    sent_count=sent_count,
                    metadata={"database_url": self.database_url, "table_name": self.table_name},
                )

        except Exception as e:
            logger.error(f"Failed to send data to database: {e}")
            return SenderResult(success=False, error=str(e), metadata={"database_url": self.database_url})

    async def stop(self) -> None:
        """Đóng connection pool"""
        await super().stop()
        if self._pool:
            await self._pool.close()
            self._pool = None


class FileSender(BaseSender):
    """Sender để lưu dữ liệu vào file (JSON, CSV)"""

    def __init__(self, file_path: str, file_format: str = "json"):
        super().__init__("file_sender")
        self.file_path = file_path
        self.file_format = file_format.lower()
        self._write_lock = asyncio.Lock()

    async def _send_data(self, data: list[StockData]) -> SenderResult:
        """Ghi dữ liệu vào file"""
        try:
            async with self._write_lock:
                if self.file_format == "json":
                    await self._write_json(data)
                elif self.file_format == "csv":
                    await self._write_csv(data)
                else:
                    raise ValueError(f"Unsupported file format: {self.file_format}")

                return SenderResult(
                    success=True,
                    sent_count=len(data),
                    metadata={"file_path": self.file_path, "file_format": self.file_format},
                )

        except Exception as e:
            logger.error(f"Failed to write data to file: {e}")
            return SenderResult(success=False, error=str(e), metadata={"file_path": self.file_path})

    async def _write_json(self, data: list[StockData]) -> None:
        """Ghi data dưới dạng JSON"""
        import aiofiles

        json_data = [stock_data.to_dict() for stock_data in data]

        async with aiofiles.open(self.file_path, mode="a", encoding="utf-8") as f:
            for item in json_data:
                await f.write(json.dumps(item, ensure_ascii=False) + "\n")

    async def _write_csv(self, data: list[StockData]) -> None:
        """Ghi data dưới dạng CSV"""
        import csv
        import io

        import aiofiles

        # Create CSV in memory first
        output = io.StringIO()
        writer = csv.writer(output)

        # Write header if file is new
        import os

        write_header = not os.path.exists(self.file_path)

        if write_header:
            writer.writerow(["symbol", "data_type", "source", "timestamp", "data", "metadata"])

        # Write data rows
        for stock_data in data:
            writer.writerow(
                [
                    stock_data.symbol,
                    stock_data.data_type.value,
                    stock_data.source.value,
                    stock_data.timestamp.isoformat(),
                    json.dumps(stock_data.data, ensure_ascii=False),
                    json.dumps(stock_data.metadata, ensure_ascii=False),
                ]
            )

        # Write to file
        async with aiofiles.open(self.file_path, mode="a", encoding="utf-8") as f:
            await f.write(output.getvalue())


class MultiSender(BaseSender):
    """
    Sender tổng hợp - gửi dữ liệu đến nhiều destinations cùng lúc
    """

    def __init__(self, senders: list[BaseSender], fail_fast: bool = False):
        super().__init__("multi_sender")
        self.senders = senders
        self.fail_fast = fail_fast  # Nếu True, dừng ngay khi có lỗi

    async def _send_data(self, data: list[StockData]) -> SenderResult:
        """Gửi dữ liệu đến tất cả senders"""
        try:
            # Tạo tasks cho tất cả senders
            tasks = []
            for sender in self.senders:
                task = asyncio.create_task(sender.send(data), name=f"send_{sender.name}")
                tasks.append((sender.name, task))

            # Chờ tất cả tasks hoàn thành
            results = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)

            total_sent = 0
            errors = []
            successful_senders = []

            for (sender_name, _), result in zip(tasks, results, strict=False):
                if isinstance(result, Exception):
                    error_msg = f"{sender_name}: {result!s}"
                    errors.append(error_msg)
                    logger.error(f"Sender {sender_name} failed: {result}")

                    if self.fail_fast:
                        return SenderResult(
                            success=False,
                            error=f"Fail-fast enabled. First error: {error_msg}",
                            metadata={"failed_sender": sender_name},
                        )
                elif result.success:
                    total_sent += result.sent_count
                    successful_senders.append(sender_name)
                else:
                    errors.append(f"{sender_name}: {result.error}")

            # Determine overall success
            success = len(successful_senders) > 0

            return SenderResult(
                success=success,
                sent_count=total_sent,
                error="; ".join(errors) if errors else None,
                metadata={
                    "successful_senders": successful_senders,
                    "failed_senders": [name for name, _ in tasks if name not in successful_senders],
                    "total_senders": len(self.senders),
                },
            )

        except Exception as e:
            logger.error(f"Unexpected error in MultiSender: {e}")
            return SenderResult(success=False, error=str(e), metadata={"sender_count": len(self.senders)})

    async def start(self) -> None:
        """Khởi động tất cả senders"""
        await super().start()
        for sender in self.senders:
            try:
                await sender.start()
            except Exception as e:
                logger.error(f"Failed to start sender {sender.name}: {e}")

    async def stop(self) -> None:
        """Dừng tất cả senders"""
        await super().stop()
        for sender in self.senders:
            try:
                await sender.stop()
            except Exception as e:
                logger.error(f"Failed to stop sender {sender.name}: {e}")

"""
Utility functions cho Stock Tracker Collector
"""

import asyncio
from datetime import datetime
import json
from typing import Any

from loguru import logger


def batch_list(items: list[Any], batch_size: int) -> list[list[Any]]:
    """Chia list thành các batch nhỏ hơn"""
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


def safe_json_loads(json_str: str, default: Any = None) -> Any:
    """Safe JSON parsing với default value"""
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return default


def safe_json_dumps(obj: Any, default: str = "{}") -> str:
    """Safe JSON serialization với default value"""
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return default


def flatten_dict(d: dict[str, Any], parent_key: str = "", sep: str = ".") -> dict[str, Any]:
    """Flatten nested dictionary"""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def normalize_symbol(symbol: str) -> str:
    """Normalize stock symbol format"""
    return symbol.upper().strip()


def is_valid_symbol(symbol: str) -> bool:
    """Validate stock symbol format"""
    normalized = normalize_symbol(symbol)
    return len(normalized) >= 2 and normalized.isalnum()


def format_timestamp(timestamp: datetime | None = None) -> str:
    """Format timestamp for logging"""
    if timestamp is None:
        timestamp = datetime.now()
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")


def calculate_rate_limit_delay(current_count: int, limit: int, period: int) -> float:
    """Tính toán delay cần thiết để tuân thủ rate limit"""
    if current_count >= limit:
        return period  # Wait full period if limit exceeded

    remaining = limit - current_count
    if remaining <= 0:
        return period

    # Calculate optimal delay to spread requests evenly
    return period / remaining if remaining > 0 else 0


async def retry_async(
    func: Any, max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0, exceptions: tuple = (Exception,)
) -> Any:
    """
    Retry async function với exponential backoff
    """
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            if asyncio.iscoroutinefunction(func):
                return await func()
            return func()
        except exceptions as e:
            last_exception = e
            if attempt < max_retries:
                wait_time = delay * (backoff**attempt)
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time:.2f}s")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"All {max_retries + 1} attempts failed. Last error: {e}")

    if last_exception:
        raise last_exception


class AsyncRateLimiter:
    """
    Simple async rate limiter
    """

    def __init__(self, rate: int, per: float):
        self.rate = rate
        self.per = per
        self._tokens = rate
        self._updated_at = asyncio.get_event_loop().time()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> None:
        """Acquire tokens from the rate limiter"""
        async with self._lock:
            now = asyncio.get_event_loop().time()

            # Add tokens based on elapsed time
            elapsed = now - self._updated_at
            self._tokens = min(self.rate, self._tokens + elapsed * (self.rate / self.per))
            self._updated_at = now

            # Wait if not enough tokens
            if self._tokens < tokens:
                sleep_time = (tokens - self._tokens) / (self.rate / self.per)
                await asyncio.sleep(sleep_time)
                self._tokens = 0
                self._updated_at = asyncio.get_event_loop().time()
            else:
                self._tokens -= tokens


class CircuitBreaker:
    """
    Circuit breaker pattern cho error handling
    """

    def __init__(self, failure_threshold: int = 5, timeout: float = 60.0, recovery_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.recovery_timeout = recovery_timeout

        self._failure_count = 0
        self._last_failure_time = None
        self._state = "closed"  # closed, open, half_open
        self._lock = asyncio.Lock()

    async def call(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Execute function with circuit breaker protection"""
        async with self._lock:
            if self._state == "open":
                if (asyncio.get_event_loop().time() - self._last_failure_time) > self.recovery_timeout:
                    self._state = "half_open"
                    logger.info("Circuit breaker half-open")
                else:
                    raise Exception("Circuit breaker is open")

            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)

                # Success - reset if in half_open state
                if self._state == "half_open":
                    self._state = "closed"
                    self._failure_count = 0
                    logger.info("Circuit breaker closed")

                return result

            except Exception as e:
                self._failure_count += 1
                self._last_failure_time = asyncio.get_event_loop().time()

                if self._failure_count >= self.failure_threshold:
                    self._state = "open"
                    logger.warning(f"Circuit breaker opened after {self._failure_count} failures")

                raise e


class MemoryCache:
    """
    Simple in-memory cache với TTL
    """

    def __init__(self, default_ttl: float = 300.0):  # 5 minutes default
        self.default_ttl = default_ttl
        self._cache: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any | None:
        """Get value from cache"""
        async with self._lock:
            if key not in self._cache:
                return None

            entry = self._cache[key]
            if asyncio.get_event_loop().time() > entry["expires_at"]:
                del self._cache[key]
                return None

            return entry["value"]

    async def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        """Set value in cache"""
        if ttl is None:
            ttl = self.default_ttl

        async with self._lock:
            self._cache[key] = {"value": value, "expires_at": asyncio.get_event_loop().time() + ttl}

    async def clear(self) -> None:
        """Clear all cache entries"""
        async with self._lock:
            self._cache.clear()

    async def cleanup_expired(self) -> int:
        """Remove expired entries and return count removed"""
        async with self._lock:
            current_time = asyncio.get_event_loop().time()
            expired_keys = [key for key, entry in self._cache.items() if current_time > entry["expires_at"]]

            for key in expired_keys:
                del self._cache[key]

            return len(expired_keys)


def validate_config(config_dict: dict[str, Any], required_keys: list[str]) -> bool:
    """Validate configuration dictionary"""
    missing_keys = [key for key in required_keys if key not in config_dict]
    if missing_keys:
        logger.error(f"Missing required config keys: {missing_keys}")
        return False
    return True


def sanitize_filename(filename: str) -> str:
    """Sanitize filename for safe file operations"""
    import re

    # Remove invalid characters
    sanitized = re.sub(r'[<>:"/\\|?*]', "_", filename)
    # Remove multiple underscores
    sanitized = re.sub(r"_+", "_", sanitized)
    return sanitized.strip("_")


async def measure_performance(func: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
    """Measure performance of async function"""
    start_time = asyncio.get_event_loop().time()
    start_memory = None

    try:
        import psutil

        process = psutil.Process()
        start_memory = process.memory_info().rss
    except ImportError:
        pass

    try:
        if asyncio.iscoroutinefunction(func):
            result = await func(*args, **kwargs)
        else:
            result = func(*args, **kwargs)

        end_time = asyncio.get_event_loop().time()
        duration = end_time - start_time

        perf_data = {"success": True, "duration_seconds": duration, "result": result}

        if start_memory:
            try:
                end_memory = process.memory_info().rss
                perf_data["memory_delta_mb"] = (end_memory - start_memory) / 1024 / 1024
            except:
                pass

        return perf_data

    except Exception as e:
        end_time = asyncio.get_event_loop().time()
        duration = end_time - start_time

        return {"success": False, "duration_seconds": duration, "error": str(e)}

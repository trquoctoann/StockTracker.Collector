"""
Main entry point cho Stock Tracker Collector
"""

import asyncio
import os

from loguru import logger

try:
    import uvloop

    UVLOOP_AVAILABLE = True
except ImportError:
    UVLOOP_AVAILABLE = False

from src.orchestrator import create_default_orchestrator


async def main() -> None:
    """Main function để chạy stock data collector"""

    # Setup logging
    logger.remove()
    logger.add(
        "logs/stock_collector_{time:YYYY-MM-DD}.log",
        rotation="1 day",
        retention="30 days",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
    )
    logger.add(
        lambda msg: print(msg, end=""),
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | <level>{message}</level>",
    )

    logger.info("Starting Stock Tracker Collector")

    # Load symbols from environment hoặc default
    symbols_env = os.getenv("STOCK_SYMBOLS", "VIC,VCB,HPG,MSN,TCB")
    symbols = [s.strip() for s in symbols_env.split(",") if s.strip()]

    # API keys
    finhub_api_key = os.getenv("FINHUB_API_KEY")
    alpha_vantage_api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

    logger.info(f"Configured symbols: {symbols}")
    logger.info(f"Finhub API key: {'Available' if finhub_api_key else 'Not provided'}")
    logger.info(f"Alpha Vantage API key: {'Available' if alpha_vantage_api_key else 'Not provided'}")

    # Tạo orchestrator
    orchestrator = create_default_orchestrator(
        symbols=symbols, finhub_api_key=finhub_api_key, alpha_vantage_api_key=alpha_vantage_api_key
    )

    # Lấy interval từ environment
    interval_minutes = int(os.getenv("COLLECTION_INTERVAL_MINUTES", "30"))

    logger.info(f"Collection interval: {interval_minutes} minutes")

    try:
        # Chạy orchestrator với scheduler
        await orchestrator.run_with_scheduler(interval_minutes=interval_minutes)

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise
    finally:
        logger.info("Stock Tracker Collector stopped")


async def run_once() -> None:
    """Chạy collection một lần và thoát"""

    logger.info("Running single collection cycle")

    # Load symbols
    symbols_env = os.getenv("STOCK_SYMBOLS", "VIC,VCB,HPG")
    symbols = [s.strip() for s in symbols_env.split(",") if s.strip()]

    finhub_api_key = os.getenv("FINHUB_API_KEY")
    alpha_vantage_api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

    # Tạo orchestrator
    orchestrator = create_default_orchestrator(
        symbols=symbols, finhub_api_key=finhub_api_key, alpha_vantage_api_key=alpha_vantage_api_key
    )

    try:
        # Start orchestrator
        await orchestrator.start()

        # Run all pipelines once
        results = await orchestrator.run_all_pipelines()

        # Log results
        for pipeline_name, result in results.items():
            if result.get("success"):
                logger.info(f"Pipeline {pipeline_name} completed successfully")
                logger.info(f"  Collected: {result.get('collected_count', 0)} items")
                logger.info(f"  Duration: {result.get('duration', 0):.2f} seconds")
            else:
                logger.error(f"Pipeline {pipeline_name} failed: {result.get('error', 'Unknown error')}")

        # Health check
        health = await orchestrator.health_check()
        logger.info(f"Health check: {health['orchestrator']['health_status']}")

    finally:
        await orchestrator.stop()


async def health_check() -> None:
    """Kiểm tra trạng thái hệ thống"""

    logger.info("Running health check")

    # Minimal orchestrator for health check
    symbols = ["VIC"]
    orchestrator = create_default_orchestrator(symbols=symbols)

    try:
        await orchestrator.start()
        health = await orchestrator.health_check()

        print("\n=== HEALTH CHECK REPORT ===")
        print(f"Orchestrator Status: {health['orchestrator']['health_status']}")
        print(f"Total Pipelines: {health['orchestrator']['global_stats']['total_pipelines']}")
        print(f"Active Pipelines: {health['orchestrator']['global_stats']['active_pipelines']}")

        for pipeline_name, pipeline_health in health["pipelines"].items():
            print(f"\nPipeline: {pipeline_name}")
            print("  Collectors:")
            for collector_name, collector_health in pipeline_health["collectors"].items():
                status = "healthy" if collector_health.get("enabled", False) else "disabled"
                data_source_type = collector_health.get("data_source_type", "unknown")
                print(f"    - {collector_name}: {status} ({data_source_type})")

            print("  Senders:")
            for sender_name, sender_health in pipeline_health["senders"].items():
                status = "running" if sender_health.get("is_running", False) else "stopped"
                print(f"    - {sender_name}: {status}")

        print("\n=== END HEALTH CHECK ===\n")

    finally:
        await orchestrator.stop()


def setup_uvloop() -> None:
    """Setup uvloop for better performance"""
    try:
        if os.name != "nt" and UVLOOP_AVAILABLE:  # Not Windows and uvloop available
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            logger.info("Using uvloop for enhanced async performance")
        else:
            logger.info("uvloop not available or not supported, using default asyncio")
    except Exception as e:
        logger.warning(f"Failed to setup uvloop: {e}")


if __name__ == "__main__":
    # Setup uvloop for better performance
    setup_uvloop()

    # Parse command line arguments
    import sys

    if len(sys.argv) > 1:
        command = sys.argv[1].lower()

        if command == "once":
            asyncio.run(run_once())
        elif command == "health":
            asyncio.run(health_check())
        elif command == "help":
            print("Stock Tracker Collector - Usage:")
            print("  python main.py          - Run with scheduler (default)")
            print("  python main.py once     - Run once and exit")
            print("  python main.py health   - Run health check")
            print("  python main.py help     - Show this help")
        else:
            print(f"Unknown command: {command}")
            print("Use 'python main.py help' for usage information")
    else:
        # Default: run with scheduler
        asyncio.run(main())

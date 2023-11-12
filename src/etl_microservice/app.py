import asyncio
import logging
import os
from typing import AsyncGenerator

from aiohttp.web import Application

from .database import InfluxUploader
from .etl import ETL

logger = logging.getLogger(__name__)


async def make_app() -> Application:
    app = Application()
    app.cleanup_ctx.append(database_context)
    app.cleanup_ctx.append(etl_context)

    url = os.getenv("INFLUX_URL", "http://localhost:8086")
    token = os.getenv("INFLUX_TOKEN")
    org = os.getenv("INFLUX_ORG", "team")
    bucket = os.getenv("INFLUX_BUCKET", "candles")
    logger.error(token)
    if not token:
        raise ValueError("Influx Token can't be missing")
    app["InfluxUploader"] = await InfluxUploader.get_uploader(url, token, org, bucket)

    api_token = os.getenv("FINNHUB_TOKEN")
    if not api_token:
        raise ValueError("Finnhub Token can't be missing")
    app["ETLWorker"] = await ETL.start_etl(api_token, app["InfluxUploader"])

    return app


async def database_context(app: Application) -> AsyncGenerator[None, None]:
    yield
    await app["InfluxUploader"].close()


async def etl_context(app: Application) -> AsyncGenerator[None, None]:
    yield

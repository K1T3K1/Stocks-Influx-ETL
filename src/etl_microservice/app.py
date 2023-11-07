from aiohttp.web import Application
from typing import AsyncIterator
from .database import InfluxUploader
from .etl import ETL
import os
import asyncio


def make_app() -> None:
    app = Application()
    app.cleanup_ctx.append(database_context)
    app.cleanup_ctx.append(etl_context)


async def database_context(app: Application) -> AsyncIterator[None, None]:
    url = os.getenv("INFLUX_URL")
    token = os.getenv("INFLUX_TOKEN")
    org = os.getenv("INFLUX_ORG")
    bucket = os.getenv("INFLUX_BUCKET")
    app["InfluxUploader"] = InfluxUploader(url, token, org, bucket)
    app["InfluxWriteTask"] = asyncio.create_task(app["InfluxUploader"]._write_task())
    yield
    await app["InfluxUploader"].close()


async def etl_context(app: Application) -> AsyncIterator[None, None]:
    api_token = os.getenv("FINNHUB_TOKEN")
    app["ETLWorker"] = ETL.start_etl(api_token)
    yield

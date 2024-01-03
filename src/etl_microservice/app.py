import asyncio
import logging
import os
from typing import AsyncGenerator

from aiohttp.web import Application

from .database import InfluxUploader
from .etl import ETL, YFinanceClient

logger = logging.getLogger(__name__)


async def make_app() -> Application:
    app = Application()
    app.cleanup_ctx.append(database_context)
    app.cleanup_ctx.append(etl_context)

    url = os.getenv("INFLUX_URL", "http://localhost:8086")
    token = os.getenv("INFLUX_TOKEN")
    org = os.getenv("INFLUX_ORG", "team")
    bucket = os.getenv("INFLUX_BUCKET", "candles")
    internal_token = os.getenv("INTERNAL_TOKEN")

    if not token:
        raise ValueError("Influx Token can't be missing")
    if not internal_token:
        raise ValueError("Internal Token can't be missing")
    app["InfluxUploader"] = await InfluxUploader.get_uploader(url, token, org, bucket)

    api_secrets = get_api_provider()
    stocks_client = YFinanceClient
    app["ETLWorker"] = await ETL.start_etl(api_secrets, app["InfluxUploader"], stocks_client, internal_token)

    return app


def get_api_provider() -> dict[str, str]:
    dict_builder = lambda key, secret, endpoint: {"key": key, "secret": secret, "endpoint": endpoint}
    provider = os.getenv("API_PROVIDER", "YAHOO")
    match provider:
        case "ALPACA":
            key = os.getenv("ALPACA_KEY")
            secret = os.getenv("ALPACA_SECRET")
            endpoint = os.getenv("ALPACA_ENDPOINT")
            api_dict = dict_builder(key, secret, endpoint)
            for k, v in api_dict.items():
                if v is None:
                    raise ValueError(f"Missing value for ENV: {k}")
            return api_dict
        case "FINNHUB":
            key = os.getenv("FINNHUB_TOKEN")
            if key is None:
                raise ValueError(f"Missing value for ENV: FINNHUB_TOKEN")
            return dict_builder(key, None, None)
        case "YAHOO":
            return dict_builder(None, None, None)
        case _:
            logger.error(f"Provider not recognized. Got: {provider}")
            raise ValueError("Provider must be correct in order to proceed")


async def database_context(app: Application) -> AsyncGenerator[None, None]:
    yield
    await app["InfluxUploader"].close()


async def etl_context(app: Application) -> AsyncGenerator[None, None]:
    yield

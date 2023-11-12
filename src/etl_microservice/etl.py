from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import partial
from typing import ClassVar, Optional

import finnhub
from influxdb_client import Point, WritePrecision

from .database import InfluxUploader

logger = logging.getLogger(__name__)


class ETL:
    client: finnhub.Client
    executor: ThreadPoolExecutor
    is_historical_filled: bool = False
    _etl_task: asyncio.Task
    target_market: str
    etl_delay: float = 60
    uploader: InfluxUploader

    # this attribute requires ETL to be a singleton
    companies: ClassVar[dict[str, str]] = {}

    @classmethod
    async def start_etl(cls, api_key: str, influx: InfluxUploader, target_market: str = "US") -> ETL:
        self = cls()
        logger.info("Starting ETL...")
        self.uploader = influx
        self.target_market = target_market
        self.client = finnhub.Client(api_key)
        self.executor = ThreadPoolExecutor(1)
        await self.get_available_companies()
        self.is_historical_filled = await self._is_historical_filled()
        if self.is_historical_filled is not True:
            logger.info("Need to fill historical. Gathering...")
            await self.gather_historical()
        self._etl_task = asyncio.create_task(self._run_etl())
        return self

    async def get_candles(self, days_delta: int) -> None:
        start = int((datetime.utcnow() - timedelta(days=days_delta)).timestamp())
        end = int(datetime.utcnow().timestamp())
        loop = asyncio.get_running_loop()
        logger.info(f"Getting last {days_delta} days")
        for symbol, _ in self.companies.items():
            func = partial(self.client.stock_candles, symbol=symbol, resolution="5", _from=start, to=end)
            p = await loop.run_in_executor(self.executor, func)
            prepared_p = self.prepare_point(p, symbol)
            #logger.info(f"Succesfully got candle for: {symbol}")

            if prepared_p:
                await self.uploader.write_point(prepared_p)

    async def _is_historical_filled(self) -> bool:
        logger.info("Checking if historical data exists")
        df = await self.uploader.query_data("93d")
        if not df.empty:
            oldest_date = df._time.min()
            reference_date = datetime.utcnow() - timedelta(days=89)
            if oldest_date < reference_date:
                return True
        return False

    async def gather_historical(self) -> None:
        await self.get_candles(92)

    async def _run_etl(self) -> None:
        while True:
            try:
                logger.info("Getting candles for single day")
                await self.get_candles(1.5)
                await asyncio.sleep(self.etl_delay)
            except Exception as e:
                logger.error(f"Failed to get single day Candles. Reason: {e}")
                await asyncio.sleep(self.etl_delay)

    async def _get_available_companies(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            func = partial(self.client.stock_symbols, exchange=self.target_market)
            symbols = await loop.run_in_executor(self.executor, func)
            for company in symbols:
                if company["type"] == "Common Stock" and company["mic"] == "XNAS":
                    self.companies[company["displaySymbol"]] = company["description"]
        except Exception as e:
            logger.critical(f"Failed to retrieve stock symbols. Reason: {e}")
        logger.info(f"Retrieved company symbols. Count: {len(self.companies)}")

    async def get_available_companies(self) -> None:
        self.companies = {
            "AAPL": "Apple Inc",
            "MSFT": "Microsoft Corp",
            "NVDA": "Nvidia Corp",
            "META": "Meta Platforms, Inc. Class A",
            "AMZN": "Amazon.Com Inc",
            "GOOGL": "Alphabet Inc Class A",
            "GOOG": "Alphabet Inc Class C",
            "AVGO": "Broadcom Inc",
            "TSLA": "Tesla Inc",
            "ADBE": "Adobe Inc",
            "COST": "Costco Wholesale Corp",
            "PEP": "PepsiCo Inc",
            "CSCO": "Cisco System Inc",
            "NFLX": "Netflix Inc",
            "AMD": "Advanced Micro Devices Inc",
            "TMUS": "T-Mobile US Inc",
            "CMCSA": "Comcast Corp",
            "INTC": "Intel Corp",
            "INTU": "Intuit Inc",
            "AMGN": "Amgen Inc",
            "QCOM": "QUALCOMM Inc",
        }

    def prepare_point(self, candle_data: dict, symbol: str) -> Optional[list[Point]]:
        result = []
        logger.info(candle_data)
        if candle_data["s"] == "ok":
            for i in range(len(candle_data["c"])):
                result.append(
                    Point("CandleData")
                    .tag("Symbol", symbol)
                    .field("ClosePrice", float(candle_data["c"][i]))
                    .field("HighPrice", float(candle_data["h"][i]))
                    .field("LowPrice", float(candle_data["l"][i]))
                    .field("OpenPrice", float(candle_data["o"][i]))
                    .field("Volume", candle_data["v"][i])
                    .time(candle_data["t"][i], write_precision='s')
                )
            return result
        return None

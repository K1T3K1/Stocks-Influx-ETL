from __future__ import annotations
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import partial
from typing import ClassVar, Optional
import pandas as pd
from influxdb_client import Point
from yfinance import Ticker
from .database import InfluxUploader
from abc import ABC, abstractmethod
import finnhub
logger = logging.getLogger(__name__)

class ETL:
    executor: ThreadPoolExecutor
    is_historical_filled: bool = False
    _etl_task: asyncio.Task
    target_market: str
    etl_delay: float = 60
    uploader: InfluxUploader
    client: StocksClient

    @classmethod
    async def start_etl(cls, api_secrets: dict[str, str], influx: InfluxUploader, client: YFinanceClient, target_market: str = "US") -> ETL:
        self = cls()
        logger.info("Starting ETL...")
        self.uploader = influx
        self.target_market = target_market
        self.executor = ThreadPoolExecutor(1)
        self.client = client(self.executor, self.uploader)
        await self.client.get_available_companies()
        self.is_historical_filled = await self._is_historical_filled()
        if self.is_historical_filled is not True:
            logger.info("Need to fill historical. Gathering...")
            await self.gather_historical()
        self._etl_task = asyncio.create_task(self._run_etl())
        return self


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
        await self.client.get_candles("3mo", "1h")

    async def _run_etl(self) -> None:
        while True:
            try:
                logger.info("Getting candles for single day")
                await self.client.get_candles("1d", "5m")
                await asyncio.sleep(self.etl_delay)
            except Exception as e:
                logger.error(f"Failed to get single day Candles. Reason: {e}")
                await asyncio.sleep(self.etl_delay)

class StocksClient:
    @abstractmethod
    def prepare_point(self, candle_data, symbol: str) -> Optional[list[Point]]:
        pass

    @abstractmethod
    async def get_candles(self, days_delta: str | int, resolution: str) -> None:
        pass
class YFinanceClient(StocksClient):
    def __init__(self, executor: ThreadPoolExecutor, uploader: InfluxUploader, api_key: str = None):
        self.executor = executor
        self.uploader = uploader
        self.companies = {}

    def prepare_point(self, candle_data: pd.Dataframe, symbol: str) -> Optional[list[Point]]:
        result = []
        if len(candle_data):
            for idx, row in candle_data.iterrows():
                result.append(
                    Point("CandleData")
                    .tag("Symbol", symbol)
                    .field("ClosePrice", row["Close"])
                    .field("HighPrice", row["High"])
                    .field("LowPrice", row["Low"])
                    .field("OpenPrice", row["Open"])
                    .field("Volume", row["Volume"])
                    .time(idx, write_precision="s")
                )
            return result
        return None
    
    async def get_candles(self, delta: int | str, resolution: str) -> None:
        loop = asyncio.get_running_loop()
        if isinstance(delta, int):
            period = f"{delta}d"
            logger.info(f"Getting last {delta} days")
        else:
            period = delta
            logger.info(f"Getting last {delta}")
        for symbol, _ in self.companies.items():
            ticker = Ticker(symbol)
            query = partial(ticker.history, period=period, interval=resolution)
            df = await loop.run_in_executor(self.executor, query)
            prepared_p = self.prepare_point(df, symbol)
            if prepared_p:
                await self.uploader.write_point(prepared_p)

    async def get_available_companies(self) -> dict[str, str]:
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

class FinnhubClient:
    client: finnhub.Client

    def __init__(self, executor: ThreadPoolExecutor, uploader: InfluxUploader, api_key: str = None) -> None:
        if api_key == None:
            raise ValueError("Api key must be provided for finnhub client")
        self.client = finnhub.Client(api_key)
        self.executor = executor
        self.uploader = uploader
        self.companies = {}

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
    
    async def get_candles(self, delta: int, resolution: str) -> None:
        start = int((datetime.utcnow() - timedelta(days=delta)).timestamp())
        end = int(datetime.utcnow().timestamp())
        loop = asyncio.get_running_loop()
        logger.info(f"Getting last {delta} days")
        for symbol, _ in self.companies.items():
            func = partial(self.client.stock_candles, symbol=symbol, resolution=resolution, _from=start, to=end)
            p = await loop.run_in_executor(self.executor, func)
            prepared_p = self.prepare_point(p, symbol)

            if prepared_p:
                await self.uploader.write_point(prepared_p)

    async def get_available_companies(self) -> None:
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
import finnhub
from concurrent.futures import ThreadPoolExecutor
import asyncio
from typing import Optional, ClassVar
import logging
from influxdb_client import Point, WritePrecision
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class ETL:
    client: finnhub.Client
    executor: ThreadPoolExecutor
    is_historical_filled: bool = False
    _etl_task: asyncio.Task
    target_market: str
    etl_delay: float = 30

    # this attribute requires ETL to be a singleton
    companies: ClassVar[dict[str, str]] = {}

    @classmethod
    async def start_etl(cls, api_key: str, target_market: Optional[str] = "US") -> None:
        self = cls()
        self.target_market = target_market
        self.client = finnhub.Client(api_key)
        self.executor = ThreadPoolExecutor(1)
        await self.get_available_companies()
        self.is_historical_filled = await self._is_historical_filled()
        if self.is_historical_filled is not True:
            await self.gather_historical()
        self._etl_task = asyncio.create_task(self._run_etl())

    async def get_candles(self, days_delta: int) -> None:
        start = (datetime.utcnow() - timedelta(days=days_delta)).timestamp()
        end = datetime.utcnow().timestamp()
        loop = asyncio.get_running_loop()
        for symbol, _ in self.companies.items():
            p = loop.run_in_executor(
                self.executor,
                self.client.stock_candles,
                symbol=symbol,
                resolution="D",
                _from=start,
                to=end,
            )
            prepared_p = self.prepare_point(p, symbol)

            if prepared_p:
                self.influx_uploader.write_point(prepared_p)

    async def _is_historical_filled(self) -> bool:
        pass

    # query influx for latest data,
    # if latest data older than 90 days:
    #   return True
    # return False

    async def gather_historical(self) -> None:
        await self.get_candles(92)

    async def _run_etl(self) -> None:
        await self.get_candles(1)
        asyncio.sleep(self.etl_delay)

    async def get_available_companies(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            symbols = await loop.run_in_executor(
                self.executor, self.client.stock_symbols, exchange=self.target_market
            )
            for company in symbols:
                if company["type"] == "Common Stock" and company["mic"] == "XNAS":
                    self.companies[company["displaySymbol"]] = company["description"]
        except Exception as e:
            logger.critical(f"Failed to retrieve stock symbols. Reason: {e}")
        logger.info(f"Retrieved company symbols. Count: {len(self.companies)}")

    def prepare_point(candle_data: dict, symbol: str) -> Optional[Point]:
        if candle_data["s"] == "ok":
            return (
                Point("CandleData")
                .tag("Symbol", symbol)
                .field("ClosePrice", candle_data["c"])
                .field("HighPrice", candle_data["h"])
                .field("LowPrice", candle_data["l"])
                .field("OpenPrice", candle_data["o"])
                .field("Volume", candle_data["v"])
                .time(candle_data["t"], write_precision=WritePrecision.S)
            )
        return None

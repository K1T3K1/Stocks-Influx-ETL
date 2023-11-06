import finnhub
from concurrent.futures import ThreadPoolExecutor
import asyncio

class ETL:
    
    @classmethod
    async def start_etl(cls):
        self = cls()
        self.client = finnhub.Client("")
        self.executor = ThreadPoolExecutor(1)
        self.is_historical_filled = await self._is_historical_filled()
        if self.is_historical_filled is not True:
            await self.gather_historical()
        loop = asyncio.get_running_loop()
        loop.create_task(self.run_etl())

    async def gather_historical(self):
        pass

    async def _is_historical_filled(self):
        pass
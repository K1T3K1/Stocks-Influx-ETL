from __future__ import annotations

import asyncio
import logging
from collections import deque
from typing import Optional

from influxdb_client import Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.client.write_api_async import WriteApiAsync
from pandas import DataFrame

logger = logging.getLogger(__name__)


class InfluxUploader:
    _client: InfluxDBClientAsync
    _write_api: WriteApiAsync
    _batching_queue: asyncio.Queue[Point]
    _batching_time: float
    _bucket: str

    @classmethod
    async def get_uploader(cls, url: str, token: str, org: str, bucket: str) -> InfluxUploader:
        self = cls()
        self._client = InfluxDBClientAsync(url, token, org, enable_gzip=True)
        self._write_api = self._client.write_api()
        self._batching_queue = asyncio.Queue()
        self._batching_time = 65
        self._bucket = bucket
        self._write_task = asyncio.create_task(self._write_task())
        self._print_task = asyncio.create_task(self.queue_size())
        return self

    async def write_point(self, data_points: list[Point]) -> None:
        # logger.info("Adding point to queue")
        for point in data_points:
            await self._batching_queue.put(point)

    async def queue_size(self) -> None:
        while True:
            logger.info(f"Size: {self._batching_queue.qsize()}")
            await asyncio.sleep(5)

    async def _write_task(
        self,
    ) -> None:
        while True:
            logger.info(f"Batch size: {self._batching_queue.qsize()}")
            if self._batching_queue.qsize() > 0:
                try:
                    logger.info("Starting write_task")
                    write_task = asyncio.create_task(self._upload_task())
                    await write_task
                except Exception as e:
                    logger.error(f"Failed to write to influx. Reason", exc_info=1)
                    await asyncio.sleep(5)
            await asyncio.sleep(self._batching_time)

    async def _upload_task(
        self,
    ) -> None:
        logger.info(f"Uploading batch. Batch size: {self._batching_queue.qsize()}")
        batch: deque = deque()
        for _ in range(self._batching_queue.qsize()):
            data_point = await self._batching_queue.get()
            batch.append(data_point)
            self._batching_queue.task_done()
        await self._write_api.write(self._bucket, record=batch, write_precision="s")

    async def query_data(
        self, start: str, end: Optional[str] = None, symbol: Optional[str] = None
    ) -> Optional[DataFrame]:
        query = self._get_query(start, end, symbol)
        api = self._client.query_api()
        try:
            df: DataFrame = await api.query_data_frame(query)
        except Exception as e:
            logger.info(f"Failed to query data. Reason: {e}")
            return DataFrame()
        return df

    def _get_query(self, start: str, end: Optional[str] = None, symbol: Optional[str] = None) -> str:
        if end and not symbol:
            return f"""from (bucket: "{self._bucket}")
                    |> range(start: -{start}, end: -{end})
                    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                    """
        if not end and symbol:
            return f"""from (bucket: "{self._bucket}")
                    |> range(start: -{start})
                    |> filter(fn: (r) => r._measurement == "CandleData" and r.Symbol == "{symbol}")
                    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                    """
        if end and symbol:
            return f"""from (bucket: "{self._bucket}")
                    |> range(start: -{start}, end: -{end})
                    |> filter(fn: (r) => r._measurement == "CandleData" and r.Symbol == "{symbol}")
                    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                    """
        return f"""from (bucket: "{self._bucket}")
                |> range(start: -{start})
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                """

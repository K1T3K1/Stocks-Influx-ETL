from influxdb_client import Point, WritePrecision
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
import asyncio
from collections import deque


class InfluxUploader:
    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        self._client = InfluxDBClientAsync(url, token, org, enable_gzip=True)
        self._write_api = self._client.write_api()
        self._batching_queue: asyncio.Queue[Point] = asyncio.Queue()
        self._batching_time = 60
        self._bucket = bucket

    async def write_point(self, data_point: Point) -> None:
        await self._batching_queue.put(data_point)

    async def _write_task(
        self,
    ) -> None:
        while True:
            write_task = asyncio.create_task(self._upload_task())
            await write_task
            asyncio.sleep(self._batching_time)

    async def _upload_task(
        self,
    ) -> None:
        batch = deque()
        for _ in range(self._batching_queue.qsize()):
            data_point = await self._batching_queue.get()
            batch.append(data_point)
            self._batching_queue.task_done()
        self._write_api.write(self._bucket, record=batch)

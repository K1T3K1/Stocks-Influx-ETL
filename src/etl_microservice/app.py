from aiohttp.web import Application
from typing import AsyncGenerator

def make_app() -> None:
    app = Application()

async def database_context(app: Application) -> AsyncGenerator[None, None]:
    yield

async def etl_context(app: Application) -> AsyncGenerator[None, None]:
    yield
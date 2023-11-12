from aiohttp.web import run_app
import logging
import queue
from logging.handlers import QueueHandler, QueueListener
from .app import make_app

que: queue.Queue = queue.Queue(-1)  # no limit on size
queue_handler = QueueHandler(que)
log_format = "%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s"
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter(log_format))
listener = QueueListener(que, handler)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)
listener.start()

def main() -> None:
    app = make_app()
    run_app(app)


if __name__ == "__main__":
    main()

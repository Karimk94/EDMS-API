import asyncio
import logging
from dotenv import load_dotenv
from services.processing_queue import processing_worker_loop

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def main():
    stop_event = asyncio.Event()
    logging.info("Starting EDMS processing queue worker.")
    try:
        await processing_worker_loop(stop_event)
    except asyncio.CancelledError:
        logging.info("Processing queue worker cancelled.")
    except Exception:
        logging.exception("Processing queue worker terminated unexpectedly.")
        raise
    finally:
        logging.info("EDMS processing queue worker stopped.")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Processing queue worker shutdown requested by KeyboardInterrupt.")

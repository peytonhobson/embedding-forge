import os
import asyncio
from dotenv import load_dotenv
from src import poll_sqs_queue, process_file_event

load_dotenv()

QUEUE_URL = os.getenv("QUEUE_URL")

if __name__ == "__main__":
    asyncio.run(poll_sqs_queue(QUEUE_URL, process_file_event))

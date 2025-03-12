import asyncio
import boto3
from dotenv import load_dotenv
import argparse

from utils import chunk_documents
from embedding_manager import generate_and_upsert_embeddings
from file_processor import download_and_process_file

load_dotenv()

s3_client = boto3.client(
    "s3",
)


async def download_file_from_s3(bucket_name, object_key, local_path):
    """Download a file from S3 to a local path."""
    try:
        s3_client.download_file(bucket_name, object_key, local_path)
        return True
    except Exception as e:
        print(f"Error downloading {object_key}: {e}")
        return False


async def process_s3_object(bucket_name, object_key):
    """Process a single S3 object - download, extract text, create embeddings, and add to index."""

    try:
        success = await download_file_from_s3(bucket_name, object_key)
        if not success:
            return False

        file_info = {"bucket_name": bucket_name, "object_key": object_key}

        docs, success = await download_and_process_file(file_info)

        if success and docs:
            # Chunk the documents
            chunked_docs = chunk_documents(docs)

            # Generate and upsert embeddings
            return await generate_and_upsert_embeddings(chunked_docs, file_info)

        print(f"Successfully processed: {object_key}")
        return True

    except Exception as e:
        print(f"Error processing {object_key}: {e}")
        return False


async def process_s3_bucket(bucket_name, prefix=""):
    """Process all objects in an S3 bucket and add embeddings to the specified index."""

    print(f"Processing bucket: {bucket_name} with prefix: {prefix}")

    # List all objects in the bucket with the given prefix
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    tasks = []
    for page in page_iterator:
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            object_key = obj["Key"]
            # Skip folders (objects that end with '/')
            if object_key.endswith("/"):
                continue

            task = process_s3_object(s3_client, bucket_name, object_key)
            tasks.append(task)

    # Process files in batches to avoid overwhelming the system
    batch_size = 10
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i : i + batch_size]
        results = await asyncio.gather(*batch)
        print(
            f"Processed batch {i//batch_size + 1}/{(len(tasks) + batch_size - 1)//batch_size}: {sum(results)}/{len(batch)} successful"
        )

    print(f"Finished processing bucket: {bucket_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate embeddings for all files in an S3 bucket"
    )
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument(
        "--prefix", default="", help="Optional prefix to filter objects in the bucket"
    )

    args = parser.parse_args()

    asyncio.run(process_s3_bucket(args.bucket, args.prefix))

from typing import Dict, Any
from .file_processor import download_and_process_file
from .embedding_manager import (
    generate_and_upsert_embeddings,
    delete_document_embeddings,
)
from .utils import chunk_documents


async def process_file_event(file_info: Dict[str, Any], event_type: str) -> bool:
    """Process a file based on the event type (create, update, delete)."""
    object_key = file_info["object_key"]

    # Handle different event types
    if "ObjectRemoved" in event_type or event_type.startswith("s3:ObjectRemoved"):
        # DELETE event - remove embeddings from Pinecone
        print(f"Processing DELETE event for {object_key}")
        return await delete_document_embeddings(file_info)

    elif "ObjectCreated" in event_type or event_type.startswith("s3:ObjectCreated"):
        # CREATE or UPDATE event
        print(f"Processing CREATE/UPDATE event for {object_key}")

        # For PUT/POST events that update existing files, first delete old embeddings
        if "Put" in event_type or "CompleteMultipartUpload" in event_type:
            print(
                f"File may be an update, deleting existing embeddings for {object_key}"
            )
            await delete_document_embeddings(file_info)

        # Process the file normally
        docs, success = await download_and_process_file(file_info)

        if success and docs:
            # Chunk the documents
            chunked_docs = chunk_documents(docs)

            # Generate and upsert embeddings
            return await generate_and_upsert_embeddings(chunked_docs, file_info)
        elif success:
            return True
        else:
            print(f"Failed to process file {object_key}")
            return False

    else:
        # Unknown event type
        print(f"Unsupported event type: {event_type} for {object_key}")
        return False

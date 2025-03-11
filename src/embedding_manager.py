import os
from typing import List, Dict, Any
from langchain.schema import Document
from .utils import (
    generate_document_embeddings,
    upsert_embeddings,
    delete_embeddings,
)


async def generate_and_upsert_embeddings(
    chunked_docs: List[Document], file_info: Dict[str, Any]
) -> bool:
    """Generate embeddings for document chunks and upsert them to the database."""
    bucket_name = file_info["bucket_name"]
    object_key = file_info["object_key"]

    print(f"Generating embeddings for {len(chunked_docs)} chunks from {object_key}")

    # Extract content from chunks
    chunk_contents = [doc.page_content for doc in chunked_docs]

    # Generate embeddings
    embeddings = await generate_document_embeddings(chunk_contents)

    if not embeddings:
        print(f"Failed to generate embeddings for {object_key}")
        return False

    # Upsert embeddings for each chunk
    try:
        results = []
        for idx, vector in enumerate(embeddings):
            result = upsert_embeddings(
                id=f"{object_key}-chunk-{idx}",
                vector=vector,
                metadata={
                    "bucketName": bucket_name,
                    "objectKey": object_key,
                    "chunkIndex": idx,
                    "chunk_text": chunked_docs[idx].page_content,  # Store full text
                    "source": f"s3://{bucket_name}/{object_key}",
                    "file_type": chunked_docs[idx].metadata.get("file_type", "unknown"),
                    "title": os.path.basename(object_key),
                },
            )
            results.append(result)

        success_count = results.count(True)
        print(
            f"Upserted {success_count}/{len(chunked_docs)} embeddings for {object_key}"
        )

        # Consider the operation successful if at least one chunk was upserted
        return success_count > 0
    except Exception as e:
        print(f"Error upserting embeddings: {e}")
        import traceback

        traceback.print_exc()
        return False


async def delete_document_embeddings(file_info: Dict[str, Any]) -> bool:
    """Delete all embeddings associated with a specific file from Pinecone."""
    object_key = file_info["object_key"]

    print(f"Deleting embeddings for {object_key}")

    try:
        # The ID prefix used when upserting embeddings
        id_prefix = f"{object_key}-chunk-"

        # Call the delete_embeddings function from src
        result = delete_embeddings(id_prefix=id_prefix)

        return result
    except Exception as e:
        print(f"Error deleting embeddings for {object_key}: {e}")
        import traceback

        traceback.print_exc()
        return False

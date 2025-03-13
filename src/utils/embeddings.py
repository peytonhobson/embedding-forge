from typing import List
from dotenv import load_dotenv
from src.clients.openai_embeddings import openai_embeddings_client
from src.clients.pinecone_client import pinecone_index

load_dotenv()


async def generate_document_embeddings(texts: List[str]) -> List[List[float]]:
    """Generate embeddings using OpenAI API."""
    try:

        response = openai_embeddings_client.embed_documents(texts)

        print(f"Successfully generated {len(response)} embeddings")
        return response
    except Exception as e:
        print(f"Error generating embeddings: {e}")
        import traceback

        traceback.print_exc()
        return []


def upsert_embeddings(id: str, vector: list, metadata: dict):
    """
    Upsert a vector embedding into Pinecone index.

    Args:
        id: Unique identifier for the vector
        vector: The embedding vector
        metadata: Additional metadata to store with the vector

    Returns:
        None
    """
    try:
        # Upsert the vector to Pinecone
        pinecone_index.upsert(
            vectors=[{"id": id, "values": vector, "metadata": metadata}]
        )

        return True
    except Exception as e:
        print(f"Error upserting embedding {id}: {e}")

        return False


def delete_embeddings(id_prefix: str = None, filter: dict = None):
    """
    Delete embeddings from Pinecone index by ID prefix or metadata filter.

    Args:
        id_prefix: Prefix of IDs to delete (e.g., "document123-chunk-")
        filter: Metadata filter to select vectors to delete

    Returns:
        bool: True if deletion was successful, False otherwise
    """
    try:
        if id_prefix:
            # Delete by ID prefix
            print(f"Deleting embeddings with ID prefix: {id_prefix}")
            for ids in pinecone_index.list(prefix=id_prefix, namespace=""):
                pinecone_index.delete(
                    ids=ids,
                    namespace="",
                )
            return True
        elif filter:
            # Delete by metadata filter
            print(f"Deleting embeddings with filter: {filter}")
            pinecone_index.delete(
                delete_all=False, ids=None, namespace="", filter=filter
            )
            return True
        else:
            print("Error: Either id_prefix or filter must be provided")
            return False
    except Exception as e:
        print(f"Error deleting embeddings: {e}")
        import traceback

        traceback.print_exc()
        return False

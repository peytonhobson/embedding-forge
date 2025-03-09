import os
from dotenv import load_dotenv
from pinecone import Pinecone

load_dotenv()

# Create an instance of the Pinecone client
pinecone_client = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))

pinecone_index = pinecone_client.Index(os.getenv("PINECONE_INDEX"))

from langchain_openai import OpenAIEmbeddings
import os
from dotenv import load_dotenv

load_dotenv()

openai_embeddings_client = OpenAIEmbeddings(
    api_key=os.getenv("OPENAI_API_KEY"), model="text-embedding-ada-002"
)

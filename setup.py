from setuptools import setup, find_packages

setup(
    name="embedding-forge",
    version="0.1.0",
    install_requires=[
        "langchain-openai>=0.3.0",
        "python-dotenv>=1.0.0",
        "pinecone>=6.0.0",
        "boto3>=1.34.0",
        "langchain-core>=0.3.0",
        "spacy>=3.7.0",
        "pandas>=2.2.0",
        "langchain-community>=0.3.0",
        "pymupdf>=1.25.0",
    ],
    python_requires=">=3.8",
    packages=find_packages(),
)

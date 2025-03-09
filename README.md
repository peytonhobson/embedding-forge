# Business Documents Project

A document processing and querying system using LangChain, OpenAI, and Pinecone.

## Setup

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   make install
   ```
4. Copy `.env.example` to `.env` and fill in your credentials:
   - OPENAI_API_KEY
   - PINECONE_API_KEY
   - AWS credentials (if using AWS services)

## Development

- Format code: `make format`
- Run linting: `make lint`
- Run tests: `make test`
- Clean up: `make clean`

## Environment Variables

Required environment variables:
- OPENAI_API_KEY: Your OpenAI API key
- PINECONE_API_KEY: Your Pinecone API key
- AWS_PROFILE: AWS profile for authentication
- AWS_REGION: AWS region (default: us-east-1)
- QUEUE_URL: SQS queue URL for processing
- PINECONE_INDEX: Pinecone index name

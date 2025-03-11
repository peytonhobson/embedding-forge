import os
import tempfile
import pandas as pd
import boto3
from typing import List, Tuple, Dict, Any
from langchain.schema import Document
from langchain_community.document_loaders import (
    PyMuPDFLoader,
    TextLoader,
    CSVLoader,
)
import botocore

# Initialize AWS clients
s3 = boto3.client("s3")


async def download_and_process_file(
    file_info: Dict[str, Any],
) -> Tuple[List[Document], bool]:
    """Download a file from S3 and process it based on its type using Langchain loaders."""
    bucket_name = file_info["bucket_name"]
    object_key = file_info["object_key"]

    print(f"Processing file: {bucket_name}/{object_key}")

    try:
        # Download file from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response["Body"].read()

        # Determine file type from object key
        file_extension = os.path.splitext(object_key)[1].lower()

        # Create a temporary file to use with Langchain loaders
        with tempfile.NamedTemporaryFile(
            suffix=file_extension, delete=False
        ) as temp_file:
            temp_file.write(file_content)
            temp_path = temp_file.name

        docs = []
        try:
            # Process file based on its type using Langchain loaders
            if file_extension == ".txt":
                loader = TextLoader(temp_path)
                docs = loader.load()

            elif file_extension == ".pdf":
                loader = PyMuPDFLoader(temp_path)
                docs = loader.load()

            elif file_extension == ".csv":
                loader = CSVLoader(temp_path)
                docs = loader.load()

            elif file_extension in [".xlsx", ".xls"]:
                sheets = pd.read_excel(temp_path, sheet_name=None)
                for sheet_name, df in sheets.items():
                    text = f"Sheet: {sheet_name}\n" + df.to_csv(index=False)
                    docs.append(
                        Document(
                            page_content=text,
                            metadata={"sheet_name": sheet_name},
                        )
                    )

            else:
                # Unsupported file type
                print(f"Unsupported file type: {file_extension} for {object_key}")
                return [], False

            # Add metadata to all documents
            for doc in docs:
                doc.metadata.update(
                    {
                        "bucketName": bucket_name,
                        "objectKey": object_key,
                        "source": f"s3://{bucket_name}/{object_key}",
                        "file_type": file_extension[1:],  # Remove the dot
                    }
                )

            print(f"Extracted {len(docs)} documents from {object_key}")
            return docs, True

        finally:
            # Clean up the temporary file
            os.unlink(temp_path)

    except botocore.exceptions.ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code == "NoSuchKey":
            # File doesn't exist in S3, but we got a message about it
            # This is likely because the file was deleted directly from S3
            print(
                f"File {object_key} no longer exists in S3 bucket {bucket_name}. Skipping processing."
            )
            # Return True to indicate the message should be deleted from the queue
            return [], True
        else:
            print(f"Error processing file {object_key}: {e}")
            return [], False
    except Exception as e:
        print(f"Error processing file {object_key}: {e}")
        return [], False

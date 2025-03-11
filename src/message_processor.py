import json
import boto3
from typing import List, Dict, Any

# Initialize AWS clients
sqs = boto3.client("sqs", region_name="us-east-1")


def parse_message(message: Dict[str, Any]) -> Dict[str, Any]:
    """Parse a single SQS message and extract file information and event type."""
    message_id = message.get("MessageId", "unknown")
    try:
        message_body = message.get("Body")
        if not message_body:
            print(f"Message {message_id} has no body")
            return None

        body = json.loads(message_body)

        file_info = {
            "bucket_name": body.get("bucket", ""),
            "object_key": body.get("key", ""),
            "event_type": body.get("eventType", ""),
            "message_id": message_id,
        }

        return file_info
    except Exception as e:
        print(f"Error parsing message {message_id}: {e}")
        return None


async def process_messages(
    messages: List[Dict[str, Any]], process_file_callback
) -> List[bool]:
    """Process multiple messages from SQS based on their event types."""
    results = []

    # Parse all messages
    file_infos = []
    for message in messages:
        file_info = parse_message(message)
        if file_info:
            file_infos.append(file_info)
            results.append(True)  # Placeholder, will update based on processing result
        else:
            results.append(False)

    print(f"Parsed {len(file_infos)} of {len(messages)} messages")

    # Process each file based on event type
    for i, file_info in enumerate(file_infos):
        event_type = file_info.get("event_type", "")
        object_key = file_info["object_key"]

        try:
            # Call the appropriate callback to process the file
            success = await process_file_callback(file_info, event_type)
            results[i] = success

        except Exception as e:
            print(f"Error processing {event_type} event for {object_key}: {str(e)}")
            import traceback

            traceback.print_exc()
            results[i] = False

    success_count = results.count(True)
    print(f"Processed messages: {success_count}/{len(messages)} successful")
    return results


async def poll_sqs_queue(queue_url: str, process_file_callback) -> None:
    """Poll and process messages from SQS queue."""
    print("Starting SQS message polling...")
    try:
        has_more_messages = True
        message_count = 0
        success_count = 0

        while has_more_messages:
            # Receive messages from SQS
            response = sqs.receive_message(
                QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=10
            )

            messages = response.get("Messages", [])

            if not messages:
                print("No messages received, ending polling")
                has_more_messages = False
                continue

            print(f"Received {len(messages)} messages from SQS")
            message_count += len(messages)

            # Process all messages in batch
            successfully_processed = await process_messages(
                messages, process_file_callback
            )

            # Delete successfully processed messages
            success_count = 0  # Reset success_count for this batch
            for message, success in zip(messages, successfully_processed):
                receipt_handle = message.get("ReceiptHandle")
                if receipt_handle and success:
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
                    success_count += 1

            print(
                f"Successfully processed and deleted {success_count} of {len(messages)} messages"
            )

        print(f"Finished processing a total of {message_count} messages")
    except Exception as error:
        print(f"Error polling messages: {error}")

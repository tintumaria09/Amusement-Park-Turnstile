import boto3, json
from botocore.exceptions import ClientError 
import time
import uuid
import threading
import random

sqs = boto3.resource('sqs')

# Initialize the high-level DynamoDB resource
dynamodb = boto3.resource('dynamodb', region_name = "ap-south-1")

queue = sqs.get_queue_by_name(QueueName="amusement-park-turnstile")

# Reference your specific table
table = dynamodb.Table("processed-tickets")

worker_id = str(uuid.uuid4()) # Generate a random, cryptographically secure Unique Identifier and convert UUID to string for DynamoDB

print(f"Worker started. ID: {worker_id}")

print("Worker started. Listening for turnstile events....")

def run_heartbeat(ticket_id, worker_id, stop_event):

  ''' In Python, is_set() is a built-in method belonging to the Event class within both the threading and multiprocessing modules. It is used to check the status of an internal boolean flag managed by an event object.'''

  while not stop_event.is_set():
    # wait(5) returns True if the stop event was triggered during the 5s nap, breaking the loop instantly.
    # It returns False if the 5s timer ran out normally, letting the loop continue to update DynamoDB.
    if stop_event.wait(5):
      break
    try:
      current_time = int(time.time())
      table.update_item(
        Key={"ticket_id":ticket_id},
        # :val act as placeholders for value
        UpdateExpression="SET last_heartbeat = :val",
        ExpressionAttributeValues={
          ":val":current_time,
          ":w_id": worker_id,
          ":status": "processing"
        },
        # Conditional check: only update if we still own it and it's processing
        ConditionExpression="worker_id = :w_id AND #st = :status",
        ExpressionAttributeNames={"#st": "status"} # Status is a reserved word in DynamoDB, so using #st as a placeholder alias prevents syntax errors.

      )
      print(f"[Heartbeat] Refreshed lease for ticket {ticket_id}")
    except ClientError as e:
      # If we lost the lease or the task finished, the background thread stops updating
      break
    except Exception as e:
      print(f"[Heartbeat Error] {e}")
      break




while True:
  messages = queue.receive_messages(
    MaxNumberOfMessages=1,
    WaitTimeSeconds=20
  )

  if not messages:
    print("No messages found. Waiting..")
    continue

  for message in messages:
    print(f"\nReceived Message ID: {message.message_id}")

    # Parse the JSON payload (Poison pill check)

    # A poison pill in SQS is an invalid message that a worker receives but fails to process repeatedly

    try:
      # json.loads() is a function from Python's built-in json module that deserializes a JSON-formatted string into a corresponding Python object (such as a dictionary or a list)

      payload = json.loads(message.body)
      ticket_id = payload.get("ticket_id")
      gate_id = payload.get("gate_id")
      event = payload.get("event")


     

      # Step B: Idempotency Check using DynamoDB Conditional Write

      expires_at = int(time.time()) + (7 * 24 * 60 * 60) # Set expiration to 7 days from now -- it automatically deletes old, completed, or expired records after 7 days so your table doesn't grow infinitely in storage.
      current_time = int(time.time())
      
      try:

        table.put_item(
          Item={
            "ticket_id" : ticket_id,
            "gate_id" : gate_id,
            "event" : event,
            "expires_at" : expires_at,
            "status" : "processing",
            "worker_id": worker_id, # Claim ownership
            "last_heartbeat" : current_time # Initial liveness timestamp

          }, ConditionExpression="attribute_not_exists(ticket_id)",
        )



   
        print(f"Processing turnstile scan -> Gate: {payload.get('gate_id')}, Ticket: {payload.get('ticket_id')}")


      except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
          print(f"[Worker] Collision detected! Inspecting existing record...")
          response = table.get_item(
            Key={"ticket_id":ticket_id}
          )
          existing_item = response.get("Item")
          
          # table.get_item() returns a full dictionary wrapper. The actual ticket data lives inside response.get("Item", {})

          if existing_item:
            status = existing_item.get("status")
            last_heartbeat = existing_item.get("last_heartbeat", 0)
            current_time = int(time.time())

            if status == "processing" and (current_time - last_heartbeat) > 15:
              print(f"[Worker] Found a dead worker's lock! Clearing the record...")
              table.delete_item(Key={"ticket_id":ticket_id})
              table.put_item(
                    Item={
                        "ticket_id": ticket_id,
                        "gate_id": gate_id,
                        "event": event,
                        "expires_at": expires_at,
                        "status": "processing",
                        "worker_id": worker_id,
                        "last_heartbeat": current_time
                    },
                    ConditionExpression="attribute_not_exists(ticket_id)"
                )
              print(f"[Worker] Successfully claimed the zombie task!")
              

            elif status == "completed":
                print(f"[Worker] Task was already successfully completed. Safe to delete message.")
                message.delete()
                continue
        else: 
          raise e           # If it's any other AWS error, crash/raise it


      # Start Heartbeat thread
      stop_heartbeat = threading.Event()
      heartbeat_thread = threading.Thread(
        target=run_heartbeat,
        args=(ticket_id, worker_id, stop_heartbeat),
        daemon=True 
      )
      '''daemon=True makes this a background safety net thread. If the main worker crashes or exits, this thread dies automatically instead of lingering.'''

      heartbeat_thread.start()

      try:
        # Simulate a dynamic workload with a random duration between 8 and 25 seconds
        processing_time = random.randint(8, 25)
        print(f"Processing turnstile scan for ticket {ticket_id}...")
        time.sleep(processing_time)
        
        # Delete the item from DynamoDB after processing finishes successfully:
        table.delete_item(Key={"ticket_id": ticket_id})
        # Delete the message so it's removed from the queue permanently
        message.delete()
        print("Message processed, heartbeat stopped and deleted from queue successfully.")

        
        
      finally:
        # Cleanly stop the background thread before finishing up
        stop_heartbeat.set()
        # Wait for the background thread to safely wrap up and shut down
        heartbeat_thread.join()

        # Update DynamoDB record status to "completed"

     








    except json.JSONDecodeError:
      print(f"Error parsing message body: {message.body}")
      # Immediately delete corrupted payloads so they don't loop forever
      message.delete()
      print("Poison pill deleted from queue.")







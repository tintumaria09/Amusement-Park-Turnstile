import boto3
import random

# Initialize the SQS resource
sqs = boto3.resource('sqs', region_name="ap-south-1")

# Get the queue. This returns an SQS.Queue instance
queue = sqs.get_queue_by_name(QueueName="amusement-park-turnstile")



for i in range(10):

  gates = ["gate-1", "gate-2", "gate-3", "gate-4", "gate-5"]
  events = ["entry", "entry", "entry", "entry", "exit"] 

  selected_gate = random.choice(gates)
  selected_event = random.choice(events)

  ticket_id = f'TICKET-{random.randint(10000,99999)}'

  # 20% chance to simulate a broken, malformed message body (not valid JSON)

  # random.random() always generates a random decimal number (a float) between 0.0 and 1.0

  if random.random() < 0.2:
    message_body = f"BROKEN-PAYLOAD-{random.randint(100, 999)}-CORRUPT-SCAN"

    print(f"Sending a MALFORMED message...")

  else:
    # Send valid JSON payload
        message_body = f'{{"gate_id": "{selected_gate}", "event": "{selected_event}", "ticket_id": "{ticket_id}"}}'
        print(f"Sending valid message for {selected_gate}...")

  response = queue.send_message(MessageBody=message_body)
  print(f"   Message ID: {response.get('MessageId')}\n")
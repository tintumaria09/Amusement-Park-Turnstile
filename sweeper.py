import boto3
import time

dynamodb = boto3.resource("dynamodb", region_name="ap-south-1")

table = dynamodb.Table("processed-tickets")

sweep_interval = 10  # How often the sweeper checks for dead tasks
stale_threshold_seconds = 15 # If no heartbeat for 15+ seconds, the worker is dead

def run_sweeper():
  while True:
    try:
      current_time = int(time.time())
      stale_threshold = current_time - stale_threshold_seconds

      # Scan DynamoDB for items stuck in "processing" with an outdated heartbeat
      response = table.scan(
        FilterExpression="#st = :processing_status AND last_heartbeat < :threshold",
        ExpressionAttributeNames={"#st" : "status"},
        ExpressionAttributeValues={
          ":processing_status": "processing",
          ":threshold": stale_threshold
        }
      )
      # Safely extract the list of stuck items from the scan response
      # defaults to an empty list if no dead tasks were found.
      stale_items = response.get("Items",[])

      if stale_items:
        print(f'\n[Sweeper] Found {len(stale_items)} abandoned tasks.')

        for item in stale_items:
          ticket_id = item["ticket_id"]
          print(f"[Sweeper] Cleaning up stale task -> Ticket:{ticket_id}")
          # Delete the stale record from DynamoDB
          table.delete_item(Key={"ticket_id":ticket_id})
          print(f"[Sweeper] successfully deleted stale record from DynamoDB")

      else:
        print("All tasks are alive in this iteration.", end="", flush=True)
    except Exception as e:
      print(f"\n[Sweeper Error] {e}")

    time.sleep(sweep_interval)


if __name__=="__main__":
  run_sweeper()









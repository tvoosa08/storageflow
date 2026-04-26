#!/bin/bash

# Script to poll a Google Cloud Dataproc Operation until completion or failure.
#
# Usage:
#   ./poll_dataproc_op.sh <OPERATION_ID> <REGION> <PROJECT_ID>
#
# Example:
#   ./poll_dataproc_op.sh b1fc5c66-c0c3-424e-b66b-e0fc520694ae us-east5 search-apolloprod-p

# --- Input Parameters ---
OPERATION_ID="$1"
REGION="$2"
PROJECT_ID="$3"

# --- Input Validation ---
if [ -z "$OPERATION_ID" ] || [ -z "$REGION" ] || [ -z "$PROJECT_ID" ]; then
  echo "Usage: $0 <OPERATION_ID> <REGION> <PROJECT_ID>"
  echo "Example: $0 b1fc5c66-c0c3-424e-b66b-e0fc520694ae us-east5 search-apolloprod-p"
  exit 1
fi

echo "--- Starting Dataproc Operation Poller ---"
echo "Operation ID: $OPERATION_ID"
echo "Region:       $REGION"
echo "Project ID:   $PROJECT_ID"
echo "-----------------------------------------"

while true; do
  # Get the 'done' status of the operation
  STATUS=$(gcloud dataproc operations describe "$OPERATION_ID" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --format="value(done)" 2>/dev/null) # Suppress stderr for cleaner output

  # Check if the command failed (e.g., operation not found, permission denied)
  if [ $? -ne 0 ]; then
    echo "ERROR: Failed to retrieve operation status. Check ID, region, project, and permissions."
    exit 1
  fi

  if [[ "$STATUS" == "True" ]]; then
    echo "Operation is done."

    # Get the full details of the completed operation to check for errors
    OPERATION_DETAILS=$(gcloud dataproc operations describe "$OPERATION_ID" \
      --region="$REGION" \
      --project="$PROJECT_ID" \
      --format="json") # Use json for easier parsing

    # Check for errors in the operation details
    ERROR_MESSAGE=$(echo "$OPERATION_DETAILS" | jq -r '.error.message // empty') # Use jq to safely extract error message

    if [[ -n "$ERROR_MESSAGE" ]]; then
      echo "Operation failed with error:"
      echo "$ERROR_MESSAGE"
      exit 1
    else
      echo "Operation completed successfully."
      # Optionally, print the full response if successful
      echo "Full Operation Response:"
      echo "$OPERATION_DETAILS" | jq .response # Print only the 'response' section
      exit 0
    fi
  else
    echo "Operation still in progress. Waiting 10 seconds..."
    sleep 10 # Wait for 10 seconds before polling again
  fi
done

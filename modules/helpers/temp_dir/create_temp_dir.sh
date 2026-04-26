#!/bin/bash

# Create a temporary directory using mktemp
TEMP_DIR=$(mktemp -d)

# Check if mktemp succeeded
if [[ ! -d "$TEMP_DIR" ]]; then
  echo "Failed to create temporary directory"
  exit 1
fi

read -r -d '' OUTPUT <<EOF
{
  "temp_dir": "$TEMP_DIR"
}
EOF

echo "$OUTPUT"

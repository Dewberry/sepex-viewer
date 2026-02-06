#!/bin/bash
set -euo pipefail


# Accept JSON as argument or from stdin and save payload
if [ $# -gt 0 ]; then
  # If argument provided, pass it to payload-writer
  PAYLOAD="$1"
  echo "$PAYLOAD" | ./payload-writer
else
  # Otherwise read from stdin
  PAYLOAD=$(cat)
  echo "$PAYLOAD" | ./payload-writer
fi

# # Run fragility-curves
# /app/fragility-curves

# rm /mnt/payload # TODO: remove hardcoding / verify path management

# # On success, output plugin results with output links from payload
# if command -v jq &> /dev/null; then
#   # Extract outputs from payload and format as hrefs
#   LINKS=$(echo "$PAYLOAD" | jq -c '[.outputs[]? | {href: .id}]' 2>/dev/null)
#   if [ -n "$LINKS" ] && [ "$LINKS" != "[]" ]; then
#     echo "{\"plugin_results\":{\"links\":$LINKS}}"
#   else
#     echo '{"plugin_results":"No outputs identified"}'
#   fi
# else
#   # Fallback if jq is not available
#   echo '{"plugin_results":"No outputs identified"}'
# fi
#!/usr/bin/env bash
set -euo pipefail

# ANSI color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

# Interactive script to add a receiver via the public /addreceiver endpoint.

# Required base URL (no default)
BASE_URL=""
AUTO_YES_IP=false
LAT_LONG=""
CALLSIGN=""
DESCRIPTION=""
URL=""
URL_SPECIFIED=false

print_usage() {
  cat <<EOF
Usage: $(basename "$0") -u base_url [-y] [-l lat,long] [-c callsign] [-d description] [-U url]
Options:
  -u base_url      Base URL of server (required)
  -y               Automatically use detected IP without prompt
  -l lat,long      Specify latitude and longitude (comma-separated)
  -c callsign      Specify callsign or name (max 15 characters)
  -d description   Specify description (max 30 characters)
  -U url           Specify optional URL (must be a valid http(s) URL)
  -h               Show this help message
EOF
}

# Parse optional flags
while getopts "u:hyl:c:d:U:" opt; do
  case $opt in
    u) BASE_URL="$OPTARG" ;;
    h) print_usage; exit 0 ;;
    y) AUTO_YES_IP=true ;;
    l) LAT_LONG="$OPTARG" ;;
    c) CALLSIGN="$OPTARG" ;;
    d) DESCRIPTION="$OPTARG" ;;
    U) URL="$OPTARG"; URL_SPECIFIED=true ;;
    *) print_usage; exit 1 ;;
  esac
done
shift $((OPTIND -1))

# Ensure base URL is provided
if [[ -z "$BASE_URL" ]]; then
  echo "Error: base_url is required."
  print_usage
  exit 1
fi

# Ensure dependencies
MISSING_CMDS=()
for cmd in curl jq bc crontab mktemp; do
  if ! command -v "$cmd" >/dev/null; then
    MISSING_CMDS+=("$cmd")
  fi
done
if [ ${#MISSING_CMDS[@]} -ne 0 ]; then
  echo "Missing required commands: ${MISSING_CMDS[*]}"
  read -p "Install missing with sudo apt-get install ${MISSING_CMDS[*]}? [Y/n]: " install_resp
  if [[ "$install_resp" =~ ^[Nn]$ ]]; then
    echo "Aborting due to missing dependencies."
    exit 1
  fi
  sudo apt-get update
  sudo apt-get install -y "${MISSING_CMDS[@]}"
fi

# Detect IP from /myip
echo "Detecting public IP from $BASE_URL/myip..."
DETECTED_IP=$(curl -s "$BASE_URL/myip" | jq -r .ip)
if [[ -z "$DETECTED_IP" || "$DETECTED_IP" == "null" ]]; then
  echo "Failed to detect IP."
  exit 1
fi
echo "Detected IP address: $DETECTED_IP"

if [[ "$AUTO_YES_IP" == true ]]; then
  IPADDRESS="$DETECTED_IP"
else
  # Confirm or override IP
  read -p "Use detected IP? [Y/n]: " ip_resp
  if [[ "$ip_resp" =~ ^[Nn]$ ]]; then
    while true; do
      read -p "Enter IP address: " IPADDRESS
      if [[ $IPADDRESS =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then
        break
      else
        echo "Invalid IP address format."
      fi
    done
  else
    IPADDRESS="$DETECTED_IP"
  fi
fi

if [[ -n "$CALLSIGN" ]]; then
  NAME="${CALLSIGN^^}"
  if (( ${#NAME} > 15 )); then
    echo "Callsign or Name must be ≤15 characters."
    exit 1
  fi
else
  # Prompt for Callsign or Name (≤15 chars)
  while true; do
    read -p "Callsign or Name (≤15 chars): " NAME
    if [[ -z "$NAME" ]]; then
      echo "Callsign or Name is required."
    elif (( ${#NAME} > 15 )); then
      echo "Callsign or Name must be ≤15 characters."
    else
      NAME="${NAME^^}"
      break
    fi
  done
fi

if [[ -n "$DESCRIPTION" ]]; then
  if (( ${#DESCRIPTION} > 30 )); then
    echo "Description must be ≤30 characters."
    exit 1
  fi
else
  # Prompt for Description (≤30 chars)
  while true; do
    read -p "Description (≤30 chars): " DESCRIPTION
    if [[ -z "$DESCRIPTION" ]]; then
      echo "Description is required."
    elif (( ${#DESCRIPTION} > 30 )); then
      echo "Description must be ≤30 characters."
    else
      break
    fi
  done
fi

if [[ -n "$LAT_LONG" ]]; then
  if ! [[ "$LAT_LONG" =~ ^-?[0-9]+(\.[0-9]+)?,\-?[0-9]+(\.[0-9]+)?$ ]]; then
    echo "Invalid latitude,longitude format."
    exit 1
  fi
  LATITUDE="${LAT_LONG%%,*}"
  LONGITUDE="${LAT_LONG##*,}"
else
  # Prompt for Latitude (-90 to 90)
  while true; do
    read -p "Latitude (-90 to 90): " LATITUDE
    if ! [[ $LATITUDE =~ ^-?[0-9]+(\.[0-9]+)?$ ]]; then
      echo "Invalid number."
      continue
    fi
    if (( $(echo "$LATITUDE < -90" | bc -l) )) || (( $(echo "$LATITUDE > 90" | bc -l) )); then
      echo "Latitude must be between -90 and 90."
    else
      break
    fi
  done

  # Prompt for Longitude (-180 to 180)
  while true; do
    read -p "Longitude (-180 to 180): " LONGITUDE
    if ! [[ $LONGITUDE =~ ^-?[0-9]+(\.[0-9]+)?$ ]]; then
      echo "Invalid number."
      continue
    fi
    if (( $(echo "$LONGITUDE < -180" | bc -l) )) || (( $(echo "$LONGITUDE > 180" | bc -l) )); then
      echo "Longitude must be between -180 and 180."
    else
      break
    fi
  done
fi

# Handle URL input based on -U flag
if [[ "$URL_SPECIFIED" == true ]]; then
  # User specified -U; if non-empty, validate; if empty, skip prompting
  if [[ -n "$URL" ]]; then
    if ! [[ "$URL" =~ ^https?://([A-Za-z0-9][-A-Za-z0-9]*\.)+[A-Za-z]{2,}(:[0-9]+)?(/.*)?$ ]]; then
      echo "Invalid URL format; must be a valid http(s) URL."
      exit 1
    fi
  fi
else
  # Interactive prompt for optional URL
  read -p "URL (optional): " URL
  if [[ -n "$URL" ]]; then
    if ! [[ "$URL" =~ ^https?://([A-Za-z0-9][-A-Za-z0-9]*\.)+[A-Za-z]{2,}(:[0-9]+)?(/.*)?$ ]]; then
      echo "Invalid URL format; must be a valid http(s) URL."
      exit 1
    fi
  fi
fi

# Display entered values for confirmation
echo
echo "Please confirm the following values:"
echo "Base URL: $BASE_URL"
echo "Name: $NAME"
echo "Description: $DESCRIPTION"
echo "Latitude: $LATITUDE"
echo "Longitude: $LONGITUDE"
echo "IP Address: $IPADDRESS"
echo "URL: ${URL:-<none>}"
read -p "Proceed? [y/N]: " confirm
if ! [[ "$confirm" =~ ^[Yy]$ ]]; then
  echo "Aborting."
  exit 1
fi

# Build JSON payload using jq
if [[ -n "${URL-}" ]]; then
  PAYLOAD=$(jq -n \
    --arg name "$NAME" \
    --arg description "$DESCRIPTION" \
    --argjson latitude "$LATITUDE" \
    --argjson longitude "$LONGITUDE" \
    --arg ipaddress "$IPADDRESS" \
    --arg url "$URL" \
    '{name:$name,description:$description,latitude:$latitude,longitude:$longitude,ipaddress:$ipaddress,url:$url}')
else
  PAYLOAD=$(jq -n \
    --arg name "$NAME" \
    --arg description "$DESCRIPTION" \
    --argjson latitude "$LATITUDE" \
    --argjson longitude "$LONGITUDE" \
    --arg ipaddress "$IPADDRESS" \
    '{name:$name,description:$description,latitude:$latitude,longitude:$longitude,ipaddress:$ipaddress}')
fi

# Send POST request
HTTP_RESPONSE=$(curl -s -w "HTTP_CODE:%{http_code}" -X POST "$BASE_URL/addreceiver" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD")

HTTP_BODY=$(echo "$HTTP_RESPONSE" | sed -e 's/HTTP_CODE:.*//')
HTTP_CODE=$(echo "$HTTP_RESPONSE" | tr -d '\n' | sed -e 's/.*HTTP_CODE://')

# Display result
if [[ "$HTTP_CODE" -eq 201 ]]; then
  echo -e "${GREEN}Receiver created successfully:${NC}"
  printf "%s\n" "$HTTP_BODY" | jq .
  # Extract id and password for cron setup
  # Extract id and password for cron setup
  ID=$(echo "$HTTP_BODY" | jq -r .id)
  PASSWORD=$(echo "$HTTP_BODY" | jq -r .password)
  # Ask to add cron job
  if [[ "$AUTO_YES_IP" == true ]]; then
    cron_resp="y"
  else
    read -p "Add a cron job to update IP every 10 minutes? [y/N]: " cron_resp
  fi
  if [[ "$cron_resp" =~ ^[Yy]$ ]]; then
    OFFSET=$(( RANDOM % 10 ))
    CRON_MIN="${OFFSET}-59/10"
    JSON_PAYLOAD=$(jq -nc --arg id "$ID" --arg password "$PASSWORD" '{id:( $id | tonumber), password:$password}')
    CRON_CMD="curl -s -X POST \"$BASE_URL/receiverip\" -H 'Content-Type: application/json' -d '$JSON_PAYLOAD' > /dev/null"
    TMP_CRON=$(mktemp)
    crontab -l 2>/dev/null > "$TMP_CRON" || true
    if grep -F "$BASE_URL/receiverip" "$TMP_CRON"; then
      echo -e "${GREEN}Cron job already exists for $BASE_URL${NC}"
    else
      echo "$CRON_MIN * * * * $CRON_CMD" >> "$TMP_CRON"
      crontab "$TMP_CRON"
      echo -e "${GREEN}Cron job installed: $CRON_MIN * * * * $CRON_CMD${NC}"
    fi
    rm "$TMP_CRON"
    echo -e "${GREEN}All operations completed successfully.${NC}"
  fi
  exit 0
else
  echo -e "${RED}Error creating receiver (HTTP $HTTP_CODE):${NC}"
  echo -e "${RED}$HTTP_BODY${NC}"
  exit 1
fi
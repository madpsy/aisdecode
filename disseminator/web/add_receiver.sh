#!/usr/bin/env bash
set -euo pipefail

# ANSI color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

# Interactive script to add a receiver via the public /addreceiver endpoint.

# Required base URL (no default)
BASE_URL=""
LAT_LONG=""
CALLSIGN=""
DESCRIPTION=""
URL=""
IP_ADDRESS=""
URL_SPECIFIED=false
AUTO_YES=false

print_usage() {
  cat <<EOF
Usage: $(basename "$0") -u base_url [-y] [-l lat,long] [-c callsign] [-d description] [-U url] [-i ip_address]
Options:
  -u base_url      Base URL of server (required)
  -y               Automatically proceed without confirmation prompt
  -l lat,long      Specify latitude and longitude (comma-separated)
  -c callsign      Specify callsign or name (max 15 characters)
  -d description   Specify description (max 30 characters)
  -U url           Specify optional URL (must be a valid http(s) URL)
  -i ip_address    Specify optional IP address
  -h               Show this help message
EOF
}

# Parse optional flags
while getopts "u:hyl:c:d:U:i:" opt; do
  case $opt in
    u) BASE_URL="$OPTARG" ;;
    h) print_usage; exit 0 ;;
    y) AUTO_YES=true ;;
    l) LAT_LONG="$OPTARG" ;;
    c) CALLSIGN="$OPTARG" ;;
    d) DESCRIPTION="$OPTARG" ;;
    U) URL="$OPTARG"; URL_SPECIFIED=true ;;
    i) IP_ADDRESS="$OPTARG" ;;
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
for cmd in curl jq bc; do
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

# Validate IP address if provided
if [[ -n "$IP_ADDRESS" ]]; then
  # Simple IPv4 validation
  if ! [[ "$IP_ADDRESS" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then
    echo "Invalid IP address format."
    exit 1
  fi
  
  # Validate each octet is between 0-255
  IFS='.' read -r -a octets <<< "$IP_ADDRESS"
  for octet in "${octets[@]}"; do
    if (( octet < 0 || octet > 255 )); then
      echo "Invalid IP address: each octet must be between 0-255."
      exit 1
    fi
  done
fi

# Display entered values for confirmation
echo
echo "Please confirm the following values:"
echo "Name: $NAME"
echo "Description: $DESCRIPTION"
echo "Latitude: $LATITUDE"
echo "Longitude: $LONGITUDE"
echo "URL: ${URL:-<none>}"
echo "IP Address: ${IP_ADDRESS:-<none>}"
if [[ "$AUTO_YES" == true ]]; then
  # Skip confirmation if -y flag is set
  echo "Proceeding automatically..."
else
  read -p "Proceed? [y/N]: " confirm
  if ! [[ "$confirm" =~ ^[Yy]$ ]]; then
    echo "Aborting."
    exit 1
  fi
fi

# Build JSON payload using jq
PAYLOAD_ARGS=(
  --arg name "$NAME"
  --arg description "$DESCRIPTION"
  --argjson latitude "$LATITUDE"
  --argjson longitude "$LONGITUDE"
)

# Add optional fields if provided
if [[ -n "${URL-}" ]]; then
  PAYLOAD_ARGS+=(--arg url "$URL")
fi

if [[ -n "${IP_ADDRESS-}" ]]; then
  PAYLOAD_ARGS+=(--arg ip_address "$IP_ADDRESS")
fi

# Construct the JSON object with conditional fields
PAYLOAD_EXPR='{name:$name,description:$description,latitude:$latitude,longitude:$longitude}'

if [[ -n "${URL-}" ]]; then
  PAYLOAD_EXPR=$(echo "$PAYLOAD_EXPR" | sed 's/}$/,url:$url}/')
fi

if [[ -n "${IP_ADDRESS-}" ]]; then
  PAYLOAD_EXPR=$(echo "$PAYLOAD_EXPR" | sed 's/}$/,ip_address:$ip_address}/')
fi

# Generate the final payload
PAYLOAD=$(jq -n "${PAYLOAD_ARGS[@]}" "$PAYLOAD_EXPR")

# Send POST request
HTTP_RESPONSE=$(curl -s -w "HTTP_CODE:%{http_code}" -X POST "$BASE_URL/addreceiver" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD")

HTTP_BODY=$(echo "$HTTP_RESPONSE" | sed -e 's/HTTP_CODE:.*//')
HTTP_CODE=$(echo "$HTTP_RESPONSE" | tr -d '\n' | sed -e 's/.*HTTP_CODE://')

# Display result
if [[ "$HTTP_CODE" -eq 201 ]]; then
  echo -e "${GREEN}Receiver created successfully:${NC}"
  printf "\n"
  
  # Extract UDP port and password from response
  UDP_PORT=$(echo "$HTTP_BODY" | jq -r .udp_port)
  ID=$(echo "$HTTP_BODY" | jq -r .id)
  PASSWORD=$(echo "$HTTP_BODY" | jq -r .password)
  
  echo -e "${GREEN}UDP port: ${UDP_PORT}${NC}"
  echo -e "${GREEN}Password: ${PASSWORD}${NC}"
  echo
  echo -e "${GREEN}You can see your receiver at the following URL:${NC}"
  echo "${BASE_URL}/metrics/receiver.html?receiver=${ID}"
  exit 0
else
  echo -e "${RED}Error creating receiver (HTTP $HTTP_CODE):${NC}"
  echo -e "${RED}$HTTP_BODY${NC}"
  exit 1
fi
#!/usr/bin/env python3
import argparse
import random
import socket
import time
import math

def compute_nmea_checksum(sentence):
    """
    Compute the NMEA checksum as an XOR of all characters in the sentence.
    The sentence should not include the starting '!' or the '*' character.
    """
    checksum = 0
    for char in sentence:
        checksum ^= ord(char)
    return format(checksum, '02X')

def encode_signed_int(val, bits):
    """
    Encode a signed integer into a two's complement binary string of a given width.
    """
    if val < 0:
        val = (1 << bits) + val
    return format(val, '0{}b'.format(bits))

def ais_sixbit_encode(bit_str):
    """
    Convert a binary string into AIS 6-bit encoded characters.
    Also returns the number of fill bits (0–5) that were added.
    """
    # Calculate required fill bits so that length is a multiple of 6.
    fill_bits = (6 - len(bit_str) % 6) % 6
    bit_str += "0" * fill_bits
    encoded = ""
    for i in range(0, len(bit_str), 6):
        chunk = bit_str[i:i+6]
        val = int(chunk, 2)
        # AIS 6-bit conversion: values 0-63 convert to ASCII as follows:
        # if val < 40 then add 48, otherwise add 56.
        if val < 40:
            encoded += chr(val + 48)
        else:
            encoded += chr(val + 56)
    return encoded, fill_bits

def ais_text_to_binary(text, length):
    """
    Convert an ASCII text string to a binary string using the AIS 6-bit encoding table.
    The text will be padded or truncated to exactly `length` characters.
    """
    # AIS 6-bit alphabet as defined in the standard.
    ais_table = "@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_ !\"#$%&'()*+,-./0123456789:;<=>?"
    text = text.upper()
    if len(text) < length:
        text = text + " " * (length - len(text))
    else:
        text = text[:length]
    bin_str = ""
    for ch in text:
        try:
            idx = ais_table.index(ch)
        except ValueError:
            # If character not found, treat as a space.
            idx = ais_table.index(" ")
        bin_str += format(idx, '06b')
    return bin_str

def build_type1_payload(mmsi, lat, lon, cog, heading):
    """
    Build AIS Message Type 1 (Class A Position Report) payload (168 bits).
    """
    type_field = format(1, '06b')
    repeat_field = "00"
    mmsi_field = format(mmsi, '030b')
    nav_status = "0000"
    rot = "00000000"       # 8 bits
    sog = "0000000000"     # 10 bits
    pos_acc = "0"
    # Convert degrees to AIS integer format (degrees * 600000, rounded)
    lon_int = int(round(lon * 600000))
    lon_field = encode_signed_int(lon_int, 28)
    lat_int = int(round(lat * 600000))
    lat_field = encode_signed_int(lat_int, 27)
    # Course over ground: stored as value in tenths of degrees.
    cog_val = int(round(cog * 10))
    cog_field = format(cog_val, '012b')
    # True heading: 9 bits, degrees. We use modulo 360.
    heading_field = format(int(round(heading)) % 360, '09b')
    timestamp = "000000"   # 6 bits
    maneuver = "00"        # 2 bits
    spare = "000"          # 3 bits
    raim = "0"             # 1 bit
    radio_status = "0" * 19  # 19 bits

    bit_string = (
        type_field + repeat_field + mmsi_field + nav_status +
        rot + sog + pos_acc + lon_field + lat_field + cog_field +
        heading_field + timestamp + maneuver + spare + raim + radio_status
    )
    return bit_string

def build_type18_payload(mmsi, lat, lon, cog, heading):
    """
    Build AIS Message Type 18 (Class B Position Report) payload (168 bits).
    """
    type_field = format(18, '06b')
    repeat_field = "00"
    mmsi_field = format(mmsi, '030b')
    sog = "0000000000"    # 10 bits
    pos_acc = "0"         # 1 bit
    lon_int = int(round(lon * 600000))
    lon_field = encode_signed_int(lon_int, 28)
    lat_int = int(round(lat * 600000))
    lat_field = encode_signed_int(lat_int, 27)
    cog_val = int(round(cog * 10))
    cog_field = format(cog_val, '012b')
    heading_field = format(int(round(heading)) % 360, '09b')
    timestamp = "000000"  # 6 bits
    reserved = "0" * 38   # 38 bits to fill to 168 bits

    bit_string = (
        type_field + repeat_field + mmsi_field + sog + pos_acc +
        lon_field + lat_field + cog_field + heading_field +
        timestamp + reserved
    )
    return bit_string

def build_type5_static_payload(mmsi, vessel_name):
    """
    Build AIS Message Type 5 (Class A Static and Voyage Related Data) payload (424 bits).
    Most fields are set to zero or spaces except the MMSI and Vessel Name.
    """
    # Field breakdown:
    #  1) Message ID: 6 bits (5)
    #  2) Repeat Indicator: 2 bits (0)
    #  3) MMSI: 30 bits
    #  4) AIS Version: 2 bits (0)
    #  5) IMO Number: 30 bits (0)
    #  6) Call Sign: 42 bits (7 characters, spaces)
    #  7) Vessel Name: 120 bits (20 characters)
    #  8) Ship Type: 8 bits (0)
    #  9) Dimension to Bow: 9 bits (0)
    # 10) Dimension to Stern: 9 bits (0)
    # 11) Dimension to Port: 6 bits (0)
    # 12) Dimension to Starboard: 6 bits (0)
    # 13) Position Fix Type: 4 bits (0)
    # 14) ETA: 20 bits (0)
    # 15) Draught: 8 bits (0)
    # 16) Destination: 120 bits (20 characters, spaces)
    # 17) DTE: 1 bit (0)
    # 18) Spare: 1 bit (0)
    
    msg_id = format(5, '06b')
    repeat = "00"
    mmsi_field = format(mmsi, '030b')
    ais_version = "00"
    imo_number = "0" * 30
    call_sign = ais_text_to_binary(" " * 7, 7)
    vessel_name_field = ais_text_to_binary(vessel_name, 20)
    ship_type = "00000000"
    dim_bow = "0" * 9
    dim_stern = "0" * 9
    dim_port = "0" * 6
    dim_starboard = "0" * 6
    pos_fix_type = "0000"
    eta = "0" * 20
    draught = "00000000"
    destination = ais_text_to_binary(" " * 20, 20)
    dte = "0"
    spare = "0"
    
    bit_string = (
        msg_id + repeat + mmsi_field + ais_version + imo_number +
        call_sign + vessel_name_field + ship_type +
        dim_bow + dim_stern + dim_port + dim_starboard +
        pos_fix_type + eta + draught + destination + dte + spare
    )
    return bit_string

def build_type24_static_payload(mmsi, vessel_name):
    """
    Build AIS Message Type 24 Part A (Class B Static Data) payload (168 bits).
    Fields:
      - Message ID: 6 bits (24)
      - Repeat Indicator: 2 bits (0)
      - MMSI: 30 bits
      - Part Number: 2 bits (0 for Part A)
      - Vessel Name: 120 bits (20 characters)
      - Spare: 8 bits (0)
    """
    msg_id = format(24, '06b')
    repeat = "00"
    mmsi_field = format(mmsi, '030b')
    part_no = "00"  # Part A indicator
    vessel_name_field = ais_text_to_binary(vessel_name, 20)
    spare = "0" * 8
    bit_string = msg_id + repeat + mmsi_field + part_no + vessel_name_field + spare
    return bit_string

def construct_nmea_sentence(channel, encoded_payload, fill_bits):
    """
    Construct the full NMEA AIVDM sentence using the provided channel, encoded payload, and fill bits.
    Sentence format:
      !AIVDM,1,1,,<channel>,<payload>,<fill>*<checksum>
    """
    body = f"AIVDM,1,1,,{channel},{encoded_payload},{fill_bits}"
    checksum = compute_nmea_checksum(body)
    sentence = f"!{body}*{checksum}"
    return sentence

def update_position(lat, lon, max_distance):
    """
    Given a current latitude and longitude (in degrees), compute a new position that is at most
    max_distance meters away from the original, in a random direction.
    Uses a small-angle approximation.
    Returns new_lat, new_lon and the chosen bearing (in degrees).
    """
    R = 6371000  # Earth radius in meters
    # Random distance from 0 to max_distance.
    distance = random.uniform(0, max_distance)
    # Random bearing in radians.
    bearing = random.uniform(0, 2 * math.pi)
    # Convert current latitude to radians.
    lat_rad = math.radians(lat)
    # Approximate new latitude.
    new_lat = lat + math.degrees(distance / R * math.cos(bearing))
    # Approximate new longitude.
    if abs(math.cos(lat_rad)) < 1e-6:
        delta_lon = 0
    else:
        delta_lon = math.degrees(distance / R * math.sin(bearing) / math.cos(lat_rad))
    new_lon = lon + delta_lon

    new_lat = max(min(new_lat, 90), -90)
    new_lon = ((new_lon + 180) % 360) - 180

    bearing_deg = math.degrees(bearing)
    return new_lat, new_lon, bearing_deg

def main():
    parser = argparse.ArgumentParser(
        description="Dummy AIS NMEA sentence generator with valid payloads and limited movement."
    )
    parser.add_argument(
        '--rate',
        type=float,
        default=1.0,
        help="Messages per second (default: 1)"
    )
    parser.add_argument(
        '--host',
        type=str,
        default="127.0.0.1",
        help="UDP host to send sentences to (default: 127.0.0.1)"
    )
    parser.add_argument(
        '--port',
        type=int,
        default=8101,
        help="UDP port to send sentences to (default: 8101)"
    )
    parser.add_argument(
        '--mmsi_count',
        type=int,
        default=10,
        help="Number of different MMSI numbers to generate (default: 10)"
    )
    parser.add_argument(
        '--max_distance',
        type=float,
        default=100.0,
        help="Maximum movement in meters per message update (default: 100)"
    )
    args = parser.parse_args()

    # For each MMSI, generate an initial random position and assign a vessel class.
    # Vessel class: 'A' or 'B' (remains constant for the run).
    mmsi_data = {}
    while len(mmsi_data) < args.mmsi_count:
        mmsi = random.randint(100000000, 999999999)
        if mmsi not in mmsi_data:
            lat = random.uniform(-90, 90)
            lon = random.uniform(-180, 180)
            vessel_class = random.choice(['A', 'B'])
            mmsi_data[mmsi] = {"lat": lat, "lon": lon, "class": vessel_class}
    mmsi_list = list(mmsi_data.keys())

    # Create a UDP socket.
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    interval = 1.0 / args.rate if args.rate > 0 else 1.0

    print(f"Sending dummy AIS sentences to {args.host}:{args.port} at {args.rate} message(s) per second.")
    print("MMSI data (MMSI: {lat, lon, class}):")
    for m, data in mmsi_data.items():
        print(f"  {m}: {data}")

    # --- Send static data messages for every MMSI ---
    for mmsi, data in mmsi_data.items():
        vessel_class = data["class"]
        if vessel_class == 'A':
            # For class A vessels use type 5 static message.
            payload_bin = build_type5_static_payload(mmsi, "SYNTH_DATA_GEN")
            channel = "A"
        else:
            # For class B vessels use type 24 static message (Part A).
            payload_bin = build_type24_static_payload(mmsi, "SYNTH_DATA_GEN")
            channel = "B"
        encoded_payload, fill = ais_sixbit_encode(payload_bin)
        sentence = construct_nmea_sentence(channel, encoded_payload, fill)
        sock.sendto(sentence.encode('ascii'), (args.host, args.port))
        print("Static message:", sentence)
    
    # --- Now enter the loop sending dynamic position messages ---
    try:
        while True:
            # Pick a random MMSI.
            mmsi = random.choice(mmsi_list)
            data = mmsi_data[mmsi]
            old_lat = data["lat"]
            old_lon = data["lon"]
            new_lat, new_lon, bearing = update_position(old_lat, old_lon, args.max_distance)
            cog = bearing
            heading = bearing

            # Update stored position.
            mmsi_data[mmsi]["lat"] = new_lat
            mmsi_data[mmsi]["lon"] = new_lon

            # Build binary payload.
            if data["class"] == 'A':
                bit_str = build_type1_payload(mmsi, new_lat, new_lon, cog, heading)
                channel = "A"
            else:
                bit_str = build_type18_payload(mmsi, new_lat, new_lon, cog, heading)
                channel = "B"

            encoded_payload, fill = ais_sixbit_encode(bit_str)
            sentence = construct_nmea_sentence(channel, encoded_payload, fill)
            sock.sendto(sentence.encode('ascii'), (args.host, args.port))
            print(sentence)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nTerminating AIS sentence generator.")

if __name__ == "__main__":
    main()

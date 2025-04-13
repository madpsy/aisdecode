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
    fill_bits = (6 - len(bit_str) % 6) % 6
    bit_str += "0" * fill_bits
    encoded = ""
    for i in range(0, len(bit_str), 6):
        chunk = bit_str[i:i+6]
        val = int(chunk, 2)
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
    lon_int = int(round(lon * 600000))
    lon_field = encode_signed_int(lon_int, 28)
    lat_int = int(round(lat * 600000))
    lat_field = encode_signed_int(lat_int, 27)
    cog_val = int(round(cog * 10))
    cog_field = format(cog_val, '012b')
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
    reserved = "0" * 38   # 38 bits

    bit_string = (
        type_field + repeat_field + mmsi_field + sog + pos_acc +
        lon_field + lat_field + cog_field + heading_field +
        timestamp + reserved
    )
    return bit_string

def build_type5_static_payload(mmsi, vessel_name):
    """
    Build AIS Message Type 5 (Class A Static and Voyage Related Data) payload (424 bits).
    Most fields are set to zero or spaces except for the MMSI and Vessel Name.
    """
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
    """
    body = f"AIVDM,1,1,,{channel},{encoded_payload},{fill_bits}"
    checksum = compute_nmea_checksum(body)
    sentence = f"!{body}*{checksum}"
    return sentence

def update_position(lat, lon, max_distance, prev_bearing, max_turn=30):
    """
    Update the position using a geodesic calculation that applies a small turn relative
    to the vessel's previous bearing. This ensures progress in the same general direction.
    
    Parameters:
      lat, lon: current position in degrees
      max_distance: maximum distance to move in meters
      prev_bearing: the previous bearing in degrees
      max_turn: maximum change in bearing in degrees (default 30°)
    
    Returns:
      new_lat, new_lon: updated position in degrees
      new_bearing: new bearing in degrees
    """
    R = 6371000.0  # Earth radius in meters
    # Choose a small deviation from the previous bearing.
    delta_bearing = random.uniform(-max_turn, max_turn)
    new_bearing = (prev_bearing + delta_bearing) % 360
    new_bearing_rad = math.radians(new_bearing)
    distance = random.uniform(0, max_distance)

    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)
    delta = distance / R

    new_lat_rad = math.asin(math.sin(lat_rad) * math.cos(delta) +
                            math.cos(lat_rad) * math.sin(delta) * math.cos(new_bearing_rad))
    new_lon_rad = lon_rad + math.atan2(math.sin(new_bearing_rad) * math.sin(delta) * math.cos(lat_rad),
                                       math.cos(delta) - math.sin(lat_rad) * math.sin(new_lat_rad))
    new_lat = math.degrees(new_lat_rad)
    new_lon = math.degrees(new_lon_rad)
    new_lon = ((new_lon + 180) % 360) - 180

    return new_lat, new_lon, new_bearing

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

    # Generate initial positions with a vessel class and a random initial bearing.
    mmsi_data = {}
    while len(mmsi_data) < args.mmsi_count:
        mmsi = random.randint(100000000, 999999999)
        if mmsi not in mmsi_data:
            lat = random.uniform(-90, 90)
            lon = random.uniform(-180, 180)
            vessel_class = random.choice(['A', 'B'])
            init_bearing = random.uniform(0, 360)
            mmsi_data[mmsi] = {"lat": lat, "lon": lon, "class": vessel_class, "bearing": init_bearing}
    mmsi_list = list(mmsi_data.keys())

    # Create a UDP socket.
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    interval = 1.0 / args.rate if args.rate > 0 else 1.0

    print(f"Sending dummy AIS sentences to {args.host}:{args.port} at {args.rate} message(s) per second.")
    print("MMSI data (MMSI: {lat, lon, class, bearing}):")
    for m, data in mmsi_data.items():
        print(f"  {m}: {data}")

    # --- Send static data messages and the initial dynamic position message for every MMSI ---
    for mmsi, data in mmsi_data.items():
        vessel_class = data["class"]
        if vessel_class == 'A':
            payload_bin = build_type5_static_payload(mmsi, "SYNTH_DATA_GEN")
            channel = "A"
        else:
            payload_bin = build_type24_static_payload(mmsi, "SYNTH_DATA_GEN")
            channel = "B"
        encoded_payload, fill = ais_sixbit_encode(payload_bin)
        sentence = construct_nmea_sentence(channel, encoded_payload, fill)
        sock.sendto(sentence.encode('ascii'), (args.host, args.port))
        print("Static message:", sentence)
        
        # --- Send initial dynamic (position) message for this vessel ---
        lat = data["lat"]
        lon = data["lon"]
        cog = data["bearing"]
        heading = data["bearing"]

        if vessel_class == 'A':
            bit_str = build_type1_payload(mmsi, lat, lon, cog, heading)
            channel = "A"
        else:
            bit_str = build_type18_payload(mmsi, lat, lon, cog, heading)
            channel = "B"
        encoded_payload, fill = ais_sixbit_encode(bit_str)
        sentence = construct_nmea_sentence(channel, encoded_payload, fill)
        sock.sendto(sentence.encode('ascii'), (args.host, args.port))
        print("Initial dynamic message:", sentence)

    # --- Main loop: Send dynamic position messages ---
    try:
        while True:
            mmsi = random.choice(mmsi_list)
            data = mmsi_data[mmsi]
            old_lat = data["lat"]
            old_lon = data["lon"]
            old_bearing = data["bearing"]

            new_lat, new_lon, new_bearing = update_position(old_lat, old_lon, args.max_distance, old_bearing)
            cog = new_bearing
            heading = new_bearing

            # Update the vessel's state.
            mmsi_data[mmsi]["lat"] = new_lat
            mmsi_data[mmsi]["lon"] = new_lon
            mmsi_data[mmsi]["bearing"] = new_bearing

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

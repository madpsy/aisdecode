package decoders

import (
    "encoding/base64"
    "fmt"
    "strings"
)

// ----------------------------------------------------------------------------
// Register this decoder for MessageID=8, DAC=200, FI=10
// ----------------------------------------------------------------------------
func init() {
    RegisterDecoder(8, 200, 10, decode_8_200_10)
}

// https://www.e-navigation.nl/content/inland-ship-static-and-voyage-related-data

// shipTypeMap maps the 14-bit code to its human-readable description.
var shipTypeMap = map[uint]string{
    0:    "default",
    8000: "Vessel, type unknown",
    8010: "Motor freighter",
    8020: "Motor tanker",
    8021: "Motor tanker, liquid cargo, type N",
    8022: "Motor tanker, liquid cargo, type C",
    8023: "Motor tanker, dry cargo as if liquid (e.g. cement)",
    8030: "Container vessel",
    8040: "Gas tanker",
    8050: "Motor freighter, tug",
    8060: "Motor tanker, tug",
    8070: "Motor freighter with one or more ships alongside",
    8080: "Motor freighter with tanker",
    8090: "Motor freighter pushing one or more freighters",
    8100: "Motor freighter pushing at least one tank-ship",
    8110: "Tug, freighter",
    8120: "Tug, tanker",
    8130: "Tug freighter, coupled",
    8140: "Tug, freighter/tanker, coupled",
    8150: "Freightbarge",
    8160: "Tankbarge",
    8161: "Tankbarge, liquid cargo, type N",
    8162: "Tankbarge, liquid cargo, type C",
    8163: "Tankbarge, dry cargo as if liquid (e.g. cement)",
    8170: "Freightbarge with containers",
    8180: "Tankbarge, gas",
    8210: "Pushtow, one cargo barge",
    8220: "Pushtow, two cargo barges",
    8230: "Pushtow, three cargo barges",
    8240: "Pushtow, four cargo barges",
    8250: "Pushtow, five cargo barges",
    8260: "Pushtow, six cargo barges",
    8270: "Pushtow, seven cargo barges",
    8280: "Pushtow, eigth cargo barges",
    8290: "Pushtow, nine or more barges",
    8310: "Pushtow, one tank/gas barge",
    8320: "Pushtow, two barges at least one tanker or gas barge",
    8330: "Pushtow, three barges at least one tanker or gas barge",
    8340: "Pushtow, four barges at least one tanker or gas barge",
    8350: "Pushtow, five barges at least one tanker or gas barge",
    8360: "Pushtow, six barges at least one tanker or gas barge",
    8370: "Pushtow, seven barges at least one tanker or gas barge",
    8380: "Pushtow, eight barges at least one tanker or gas barge",
    8390: "Pushtow, nine or more barges at least one tanker or gas barge",
    8400: "Tug, single",
    8410: "Tug, one or more tows",
    8420: "Tug, assisting a vessel or linked combination",
    8430: "Pushboat, single",
    8440: "Passenger ship, ferry, cruise ship, red cross ship",
    8441: "Ferry",
    8442: "Red cross ship",
    8443: "Cruise ship",
    8444: "Passenger ship without accomodation",
    8450: "Service vessel, police patrol, port service",
    8460: "Vessel, work maintainance craft, floating derrick, cable-ship, buoy-ship, dredge",
    8470: "Object, towed, not otherwise specified",
    8480: "Fishing boat",
    8490: "Bunkership",
    8500: "Barge, tanker, chemical",
    8510: "Object, not otherwise specified",
    1500: "General cargo Vessel maritime",
    1510: "Unit carrier maritime",
    1520: "Bulk carrier maritime",
    1530: "Tanker",
    1540: "Liquified gas tanker",
    1850: "Pleasure craft, longer than 20 metres",
    1900: "Fast ship",
    1910: "Hydrofoil",
    1920: "Catamaran fast",
}

// ----------------------------------------------------------------------------
// decode_8_200_10 (EU Vessel ID + dimensions + load status)
// ----------------------------------------------------------------------------
func decode_8_200_10(packet map[string]interface{}) (map[string]interface{}, error) {
    // 1) Base64 → bits (assumes the first 56-bit header has been stripped)
    rawB64, ok := packet["BinaryData"].(string)
    if !ok {
        return nil, fmt.Errorf("decode_8_200_10: missing BinaryData")
    }
    raw, err := base64.StdEncoding.DecodeString(rawB64)
    if err != nil {
        return nil, fmt.Errorf("decode_8_200_10: base64 decode: %v", err)
    }
    bits := raw

    // 2) Field offsets (all offsets are after that 56-bit header)
    O := map[string][2]int{
        "uevin":        {0, 48},  // 8×6-bit ASCII
        "length":       {48, 13}, // 0=default, 1–8000 decimetres
        "beam":         {61, 10}, // 0=default, 1–1000 decimetres
        "ship_type":    {71, 14}, // 0=default
        "hazard":       {85, 3},  // 0–3 valid, 4=B-Flag, 5=default
        "draught":      {88, 11}, // 0=default, 1–2000 (in 1/100 m)
        "load_stat":    {99, 2},  // 0=not available/default, 1=loaded, 2=unloaded, 3=reserved
        "qual_speed":   {101, 1},
        "qual_course":  {102, 1},
        "qual_heading": {103, 1},
        // spare @ 104–111
    }

    out := make(map[string]interface{})

    // — Unique European Vessel Identification Number (8 chars) —
    var runes []rune
    for i := 0; i < 8; i++ {
        bitPos := O["uevin"][0] + i*6
        code, err := SafeGetUint(bits, bitPos, 6)
        if err != nil {
            return nil, fmt.Errorf("decode_8_200_10: uevin[%d]: %v", i, err)
        }
        var ch rune
        if code < 32 {
            ch = rune(code + 64)
        } else {
            ch = rune(code)
        }
        runes = append(runes, ch)
    }
    uevin := strings.TrimRight(string(runes), "@")
    if uevin != "" {
        out["unique_eu_vessel_identification_number"] = uevin
    }

    // — Length of ship —
    length, err := SafeGetUint(bits, O["length"][0], O["length"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_200_10: length: %v", err)
    }
    switch {
    case length == 0:
        out["length_status"] = "default"
    case length >= 1 && length <= 8000:
        out["length_tenths_metres"] = length
    default:
        out["length_status"] = "invalid"
    }

    // — Beam of ship —
    beam, err := SafeGetUint(bits, O["beam"][0], O["beam"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_200_10: beam: %v", err)
    }
    switch {
    case beam == 0:
        out["beam_status"] = "default"
    case beam >= 1 && beam <= 1000:
        out["beam_tenths_metres"] = beam
    default:
        out["beam_status"] = "invalid"
    }

    // — Ship or combination type —
    stypeInt, err := SafeGetUint(bits, O["ship_type"][0], O["ship_type"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_200_10: ship_type: %v", err)
    }
    // cast to uint for the map lookup
    stype := uint(stypeInt)

    if desc, ok := shipTypeMap[stype]; ok {
        out["ship_or_combination_type"] = desc
    } else {
        // fallback if code not in the map
        out["ship_or_combination_type"] = fmt.Sprintf("unknown(%d)", stypeInt)
    }

    // — Hazardous cargo —
    haz, err := SafeGetUint(bits, O["hazard"][0], O["hazard"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_200_10: hazard: %v", err)
    }
    switch haz {
    case 0, 1, 2, 3:
        out["hazardous_cargo"] = haz
    case 4:
        out["hazardous_cargo"] = "b-flag"
    case 5:
        out["hazardous_cargo"] = "unknown"
    default:
        out["hazardous_cargo"] = "invalid"
    }

    // — Draught —
    draught, err := SafeGetUint(bits, O["draught"][0], O["draught"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_200_10: draught: %v", err)
    }
    switch {
    case draught == 0:
        out["draught_status"] = "default"
    case draught >= 1 && draught <= 2000:
        out["draught_cm"] = draught
    default:
        out["draught_status"] = "invalid"
    }

    // — Loaded/unloaded status —
    load, err := SafeGetUint(bits, O["load_stat"][0], O["load_stat"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_200_10: load_status: %v", err)
    }
    switch load {
    case 0:
        out["loaded_unloaded"] = "not available"
    case 1:
        out["loaded_unloaded"] = "loaded"
    case 2:
        out["loaded_unloaded"] = "unloaded"
    case 3:
        out["loaded_unloaded"] = "reserved"
    }

    // — Quality of speed, course, heading —
    qs, err := SafeGetUint(bits, O["qual_speed"][0], 1)
    if err != nil {
        return nil, fmt.Errorf("decode_8_200_10: qual_speed: %v", err)
    }
    out["quality_speed"] = (qs == 1)

    qc, err := SafeGetUint(bits, O["qual_course"][0], 1)
    if err != nil {
        return nil, fmt.Errorf("decode_8_200_10: qual_course: %v", err)
    }
    out["quality_course"] = (qc == 1)

    qh, err := SafeGetUint(bits, O["qual_heading"][0], 1)
    if err != nil {
        return nil, fmt.Errorf("decode_8_200_10: qual_heading: %v", err)
    }
    out["quality_heading"] = (qh == 1)

    return out, nil
}

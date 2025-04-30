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
    stype, err := SafeGetUint(bits, O["ship_type"][0], O["ship_type"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_200_10: ship_type: %v", err)
    }
    if stype == 0 {
        out["ship_or_combination_type"] = "default"
    } else {
        out["ship_or_combination_type"] = stype
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

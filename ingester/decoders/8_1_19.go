package decoders

import (
    "encoding/base64"
    "fmt"
    "strings"
)

// ----------------------------------------------------------------------------
// Register this decoder for MessageID=8, DAC=1, FI=19
// ----------------------------------------------------------------------------
func init() {
    RegisterDecoder(8, 1, 19, decode_8_1_19)
}

// https://www.e-navigation.nl/content/marine-traffic-signal

// ----------------------------------------------------------------------------
// decode_8_1_19 (always emit all fields, preserving original logic)
// ----------------------------------------------------------------------------
func decode_8_1_19(packet map[string]interface{}) (map[string]interface{}, error) {
    // 1) Base64 decode
    rawB64, ok := packet["BinaryData"].(string)
    if !ok {
        return nil, fmt.Errorf("decode_8_1_19: missing BinaryData")
    }
    raw, err := base64.StdEncoding.DecodeString(rawB64)
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_19: base64 decode: %v", err)
    }

    // 2) Bits slice (each byte is one bit: 0 or 1)
    bits := raw

    // 3) Field offsets after stripping 56-bit header
    O := map[string][2]int{
        "linkage_id":      {0, 10},
        "name":            {10, 120},  // 20 chars × 6 bits
        "longitude":       {130, 25},
        "latitude":        {155, 24},
        "status":          {179, 2},
        "signal":          {181, 5},
        "utc_hour":        {186, 5},
        "utc_min":         {191, 6},
        "expected_signal": {197, 5},
    }

    out := make(map[string]interface{})

    // — Message Linkage ID —
    off, length := O["linkage_id"][0], O["linkage_id"][1]
    mlid, err := SafeGetUint(bits, off, length)
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_19 linkage_id: %v", err)
    }
    // always emit
    out["message_linkage_id"] = mlid

    // — Name of Signal (20 chars × 6-bit ASCII) —
    var rawName [20]rune
    nameOff := O["name"][0]
    for i := 0; i < 20; i++ {
        code, err := SafeGetUint(bits, nameOff+i*6, 6)
        if err != nil {
            return nil, fmt.Errorf("decode_8_1_19 name[%d]: %v", i, err)
        }
        if code < 32 {
            rawName[i] = rune(code + 64)
        } else {
            rawName[i] = rune(code)
        }
    }
    name := strings.TrimRight(string(rawName[:]), "@")
    // always emit (empty string if truly blank)
    out["signal_name"] = name

    // — Position of Station — Longitude & Latitude —
    lonOff, lonLen := O["longitude"][0], O["longitude"][1]
    lonRaw, err := SafeGetSint(bits, lonOff, lonLen)
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_19 longitude: %v", err)
    }
    // always emit (even if sentinel 181*60000)
    out["station_longitude_deg"] = float64(lonRaw) / 60000.0

    latOff, latLen := O["latitude"][0], O["latitude"][1]
    latRaw, err := SafeGetSint(bits, latOff, latLen)
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_19 latitude: %v", err)
    }
    // always emit (even if sentinel 91*60000)
    out["station_latitude_deg"] = float64(latRaw) / 60000.0

    // — Status of Signal —
    stOff, stLen := O["status"][0], O["status"][1]
    st, err := SafeGetUint(bits, stOff, stLen)
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_19 status: %v", err)
    }
    statuses := []string{
        "not available",
        "in regular service",
        "irregular service",
        "reserved",
    }
    var statusLabel string
    if int(st) < len(statuses) {
        statusLabel = statuses[st]
    } else {
        statusLabel = "unknown"
    }
    out["signal_status"] = statusLabel

    // — Signal in Service —
    sigOff, sigLen := O["signal"][0], O["signal"][1]
    sv, err := SafeGetUint(bits, sigOff, sigLen)
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_19 signal: %v", err)
    }
    signals := []string{
        "not available",
        "IALA port traffic signal 1: Serious emergency – all vessels to stop or divert according to instructions.",
        "IALA port traffic signal 2: Vessels shall not proceed.",
        "IALA port traffic signal 3: Vessels may proceed. One way traffic.",
        "IALA port traffic signal 4: Vessels may proceed. Two way traffic.",
        "IALA port traffic signal 5: A vessel may proceed only when it has received specific orders to do so.",
        "IALA port traffic signal 2a: Vessels shall not proceed, except that vessels which navigate outside the main channel need not comply with the main message.",
        "IALA port traffic signal 5a: A vessel may proceed only when it has received specific orders to do so; except that vessels which navigate outside the main channel need not comply with the main message.",
        "Japan Traffic Signal I: “in-bound” only acceptable.",
        "Japan Traffic Signal O: “out-bound” only acceptable.",
        "Japan Traffic Signal F: both “in- and out-bound” acceptable.",
        "Japan Traffic Signal XI: Code will shift to “I” in due time.",
        "Japan Traffic Signal XO: Code will shift to “O” in due time.",
        "Japan Traffic Signal X: Vessels shall not proceed, except a vessel which receives the direction from the competent authority.",
    }
    var svcLabel string
    if int(sv) < len(signals) {
        svcLabel = signals[sv]
    } else {
        svcLabel = "reserved for future use"
    }
    out["signal_in_service"] = svcLabel

    // — Time of Next Signal Shift (UTC) —
    hr, err := SafeGetUint(bits, O["utc_hour"][0], O["utc_hour"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_19 utc_hour: %v", err)
    }
    mn, err := SafeGetUint(bits, O["utc_min"][0], O["utc_min"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_19 utc_min: %v", err)
    }
    // always emit even if hr>=24 or mn>=60
    out["next_shift_utc_time"] = fmt.Sprintf("%02d:%02d", hr, mn)

    // — Expected Next Signal —
    es, err := SafeGetUint(bits, O["expected_signal"][0], O["expected_signal"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_19 expected_signal: %v", err)
    }
    var expLabel string
    if int(es) < len(signals) {
        expLabel = signals[es]
    } else {
        expLabel = "reserved for future use"
    }
    out["expected_signal"] = expLabel

    return out, nil
}

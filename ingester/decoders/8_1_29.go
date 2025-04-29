package decoders

import (
    "encoding/base64"
    "fmt"
    "strings"
)

// ----------------------------------------------------------------------------
// Register this decoder for MessageID=8, DAC=1, FI=29
// ----------------------------------------------------------------------------

func init() {
    RegisterDecoder(8, 1, 29, decode_8_1_29)
}

// https://www.e-navigation.nl/content/text-description-0

// ----------------------------------------------------------------------------
// decode_8_1_29 (using SafeGetUint)
// ----------------------------------------------------------------------------

func decode_8_1_29(packet map[string]interface{}) (map[string]interface{}, error) {
    // 1) Base64 → bits (assumes the first 56-bit header has been stripped)
    rawB64, ok := packet["BinaryData"].(string)
    if !ok {
        return nil, fmt.Errorf("decode_8_1_29: missing BinaryData")
    }
    raw, err := base64.StdEncoding.DecodeString(rawB64)
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_29: base64 decode: %v", err)
    }
    bits := raw

    // 2) Field offsets (after stripping 18-bit header)
    //                                    offset, length
    O := map[string][2]int{
        "seq_num":    {0, 2},   // Sequence Number
        "dest_id":    {2, 30},  // Destination MMSI
        "retransmit": {32, 1},  // Retransmit Flag
        // spare bit at 33
        "linkage_id": {34, 10}, // Message Linkage ID
        // text starts at bit 44, runs to end
    }

    out := make(map[string]interface{})

    // — Sequence Number —
    sn, err := SafeGetUint(bits, O["seq_num"][0], O["seq_num"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_29: seq_num: %v", err)
    }
    if sn <= 3 {
        out["sequence_number"] = sn
    }

    // — Destination MMSI —
    dest, err := SafeGetUint(bits, O["dest_id"][0], O["dest_id"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_29: dest_id: %v", err)
    }
    if dest != 0 {
        out["destination_mmsi"] = dest
    }

    // — Retransmit Flag —
    rt, err := SafeGetUint(bits, O["retransmit"][0], O["retransmit"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_29: retransmit: %v", err)
    }
    if rt <= 1 {
        out["retransmit"] = (rt == 1)
    }

    // — Message Linkage ID —
    ml, err := SafeGetUint(bits, O["linkage_id"][0], O["linkage_id"][1])
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_29: linkage_id: %v", err)
    }
    if ml != 0 {
        out["message_linkage_id"] = ml
    }

    // — Text String (6-bit ASCII) —
    const textOffset = 44
    nBits := len(bits) - textOffset
    if nBits > 0 {
        nChars := nBits / 6
        var runes []rune
        for i := 0; i < nChars; i++ {
            bitPos := textOffset + i*6
            code, err := SafeGetUint(bits, bitPos, 6)
            if err != nil {
                return nil, fmt.Errorf("decode_8_1_29: text[%d]: %v", i, err)
            }
            var ch rune
            if code < 32 {
                ch = rune(code + 64)
            } else {
                ch = rune(code)
            }
            runes = append(runes, ch)
        }
        text := strings.TrimRight(string(runes), "@")
        if text != "" {
            out["text"] = text
        }
    }

    return out, nil
}

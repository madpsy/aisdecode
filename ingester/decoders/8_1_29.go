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

// ----------------------------------------------------------------------------
// decode_8_1_29
// ----------------------------------------------------------------------------

func decode_8_1_29(packet map[string]interface{}) (map[string]interface{}, error) {
    // 1) Base64 → bits (assumes the first 18-bit header has been stripped)
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
        "seq_num":       {0,  2},   // Sequence Number
        "dest_id":       {2, 30},   // Destination MMSI
        "retransmit":    {32, 1},   // Retransmit Flag
        // spare bit at 33
        "linkage_id":    {34, 10},  // Message Linkage ID
        // text starts at bit 44, runs to end
    }

    out := make(map[string]interface{})

    // — Sequence Number —
    if sn := getUint(bits, O["seq_num"][0], O["seq_num"][1]); sn <= 3 {
        out["sequence_number"] = sn
    }

    // — Destination MMSI —
    if dest := getUint(bits, O["dest_id"][0], O["dest_id"][1]); dest != 0 {
        out["destination_mmsi"] = dest
    }

    // — Retransmit Flag —
    if rt := getUint(bits, O["retransmit"][0], O["retransmit"][1]); rt <= 1 {
        out["retransmit"] = (rt == 1)
    }

    // — Message Linkage ID —
    if ml := getUint(bits, O["linkage_id"][0], O["linkage_id"][1]); ml != 0 {
        out["message_linkage_id"] = ml
    }

    // — Text String (6-bit ASCII) —
    // remaining bits after offset 44
    const textOffset = 44
    nBits := len(bits) - textOffset
    nChars := nBits / 6
    var runes []rune
    for i := 0; i < nChars; i++ {
        code := getUint(bits, textOffset+i*6, 6)
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

    return out, nil
}

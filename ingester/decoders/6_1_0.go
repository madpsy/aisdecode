package decoders

import (
    "encoding/base64"
    "fmt"
    "strings"
)

// ----------------------------------------------------------------------------
// Register this decoder for MessageID=6, DAC=1, FI=0
// ----------------------------------------------------------------------------

func init() {
    RegisterDecoder(6, 1, 0, decode_6_1_0)
}

// ----------------------------------------------------------------------------
// decode_6_1_0
// ----------------------------------------------------------------------------

func decode_6_1_0(packet map[string]interface{}) (map[string]interface{}, error) {
    // 1) Base64 → bits (assumes the first 18 bits header has been stripped)
    rawB64, ok := packet["BinaryData"].(string)
    if !ok {
        return nil, fmt.Errorf("decode_6_1_0: missing BinaryData")
    }
    raw, err := base64.StdEncoding.DecodeString(rawB64)
    if err != nil {
        return nil, fmt.Errorf("decode_6_1_0: base64 decode: %v", err)
    }
    bits := unpackBytesToBits(raw)

    // 2) Field offsets (after stripping 18-bit header)
    //                                              offset, length
    O := map[string][2]int{
        "seq_num":           {0,  2},   // Sequence number
        "dest_id":           {2, 30},   // Destination MMSI
        "retransmit":        {32, 1},   // Retransmit flag
        // spare bit at 33
        "ack_req":           {34, 1},   // Acknowledge required flag
        "text_seq_num":      {35, 11},  // Text sequence number
        // text begins at bit 46, variable length up to end- padding
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

    // — Acknowledge Required Flag —
    if ar := getUint(bits, O["ack_req"][0], O["ack_req"][1]); ar <= 1 {
        out["ack_required"] = (ar == 1)
    }

    // — Text Sequence Number —
    if ts := getUint(bits, O["text_seq_num"][0], O["text_seq_num"][1]); ts != 0 {
        out["text_sequence_number"] = ts
    }

    // — Text String (6-bit ASCII) —
    const textOffset = 46
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

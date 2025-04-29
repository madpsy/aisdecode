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

// https://www.e-navigation.nl/content/text-using-6-bit-ascii

// ----------------------------------------------------------------------------
// decode_6_1_0 (using SafeGetUint)
// ----------------------------------------------------------------------------
// Assumes the first 88 bits (Message ID, repeat indicator,
// source MMSI, sequence no., dest MMSI, retransmit flag,
// spare, DAC and FI) have already been stripped off.
func decode_6_1_0(packet map[string]interface{}) (map[string]interface{}, error) {
    // 1) Base64 decode
    rawB64, ok := packet["BinaryData"].(string)
    if !ok {
        return nil, fmt.Errorf("decode_6_1_0: missing BinaryData")
    }
    raw, err := base64.StdEncoding.DecodeString(rawB64)
    if err != nil {
        return nil, fmt.Errorf("decode_6_1_0: base64 decode: %v", err)
    }

    // 2) Bits slice (each byte is one bit: 0 or 1)
    bits := raw

    // 3) Field offsets after stripping 88-bit header
    O := map[string][2]int{
        "ack_req":      {0,  1},  // Acknowledge required flag
        "text_seq_num": {1, 11},  // Text sequence number
        // Text payload begins at bit offset 12
    }

    out := make(map[string]interface{})

    // — Acknowledge Required Flag —
    if ar, err := SafeGetUint(bits, O["ack_req"][0], O["ack_req"][1]); err != nil {
        return nil, fmt.Errorf("decode_6_1_0 ack_required: %v", err)
    } else {
        out["ack_required"] = (ar == 1)
    }

    // — Text Sequence Number —
    if ts, err := SafeGetUint(bits, O["text_seq_num"][0], O["text_seq_num"][1]); err != nil {
        return nil, fmt.Errorf("decode_6_1_0 text_sequence_number: %v", err)
    } else if ts != 0 {
        out["text_sequence_number"] = ts
    }

    // — Text String (6-bit ASCII) —
    const textOffset = 12
    nBits := len(bits) - textOffset
    nChars := nBits / 6

    var runes []rune
    for i := 0; i < nChars; i++ {
        bitPos := textOffset + i*6
        code, err := SafeGetUint(bits, bitPos, 6)
        if err != nil {
            return nil, fmt.Errorf("decode_6_1_0 text[%d]: %v", i, err)
        }
        var ch rune
        if code < 32 {
            // 0–31 map to '@' + code
            ch = rune(code + 64)
        } else {
            ch = rune(code)
        }
        runes = append(runes, ch)
    }

    // Trim any trailing '@' padding
    text := strings.TrimRight(string(runes), "@")
    if text != "" {
        out["text"] = text
    }

    return out, nil
}

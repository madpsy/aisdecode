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
// Assumes the first 56 bits (Message ID, repeat indicator, source MMSI,
// sequence no., destination MMSI, retransmit flag, spare) have already
// been stripped off.
func decode_8_1_29(packet map[string]interface{}) (map[string]interface{}, error) {
    // 1) Base64 → bits (each byte is one bit: 0 or 1)
    rawB64, ok := packet["BinaryData"].(string)
    if !ok {
        return nil, fmt.Errorf("decode_8_1_29: missing BinaryData")
    }
    raw, err := base64.StdEncoding.DecodeString(rawB64)
    if err != nil {
        return nil, fmt.Errorf("decode_8_1_29: base64 decode: %v", err)
    }
    bits := raw

    // 2) Field offsets within the *payload* (bit 0 is the first bit after those 56 header bits):
    O := map[string][2]int{
        "linkage_id": {0, 10}, // 10-bit Message Linkage ID
        // text payload begins at bit 10
    }

    out := make(map[string]interface{})

    // — Message Linkage ID (10 bits) —
    if ml, err := SafeGetUint(bits, O["linkage_id"][0], O["linkage_id"][1]); err != nil {
        return nil, fmt.Errorf("decode_8_1_29: linkage_id: %v", err)
    } else if ml != 0 {
        out["message_linkage_id"] = ml
    }

    // — Text String (6-bit ASCII) —
    const textOffset = 10
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
                // 0–31 → '@' + code
                ch = rune(code + 64)
            } else {
                ch = rune(code)
            }
            runes = append(runes, ch)
        }
        // Trim any trailing '@' padding
        if text := strings.TrimRight(string(runes), "@"); text != "" {
            out["text"] = text
        }
    }

    return out, nil
}

package decoders

import (
    "encoding/base64"
    "fmt"
)

// ----------------------------------------------------------------------------
// Register this decoder for MessageID=6, DAC=235, FI=10
// ----------------------------------------------------------------------------
func init() {
    RegisterDecoder(6, 235, 10, decode_6_235_10)
}

// https://www.e-navigation.nl/content/aids-navigation-monitoring-data

// ----------------------------------------------------------------------------
// decode_6_235_10 (using SafeGetUint for bit extraction)
// ----------------------------------------------------------------------------
// Upstream has already stripped the 88-bit AIS header, so `bits[0]` is
// the first payload bit (the MSB of the internal-analogue reading).
func decode_6_235_10(packet map[string]interface{}) (map[string]interface{}, error) {
    // 1) Base64 decode
    rawB64, ok := packet["BinaryData"].(string)
    if !ok {
        return nil, fmt.Errorf("decode_6_235_10: missing BinaryData")
    }
    raw, err := base64.StdEncoding.DecodeString(rawB64)
    if err != nil {
        return nil, fmt.Errorf("decode_6_235_10: base64 decode: %v", err)
    }

    // 2) Bits slice (each byte is one bit: 0 or 1)
    bits := raw

    // 3) Field offsets (all relative to bit 0 after the 88-bit header)
    O := map[string][2]int{
        // Analogue inputs (10 bits each; value ×0.05 V)
        "analogue_int":  {0,  10}, // internal analogue
        "analogue_ext1": {10, 10}, // external input 1
        "analogue_ext2": {20, 10}, // external input 2

        // Status bits
        "status_int":     {30, 5},  // internal (5 bits)
        "status_ext":     {35, 8},  // external (8 bits)
        "off_pos_status": {43, 1},  // off-position flag
        // spare bits at 44–47
    }

    out := make(map[string]interface{})

    // — Analogue readings (0.05 V steps) —
    if ai, err := SafeGetUint(bits, O["analogue_int"][0], O["analogue_int"][1]); err != nil {
        return nil, fmt.Errorf("decode_6_235_10 analogue_int: %v", err)
    } else if ai != 0 {
        out["analogue_internal_V"] = float64(ai) * 0.05
    }
    if ae1, err := SafeGetUint(bits, O["analogue_ext1"][0], O["analogue_ext1"][1]); err != nil {
        return nil, fmt.Errorf("decode_6_235_10 analogue_ext1: %v", err)
    } else if ae1 != 0 {
        out["analogue_external1_V"] = float64(ae1) * 0.05
    }
    if ae2, err := SafeGetUint(bits, O["analogue_ext2"][0], O["analogue_ext2"][1]); err != nil {
        return nil, fmt.Errorf("decode_6_235_10 analogue_ext2: %v", err)
    } else if ae2 != 0 {
        out["analogue_external2_V"] = float64(ae2) * 0.05
    }

    // — Internal status bits (5 bits) —
    if sb, err := SafeGetUint(bits, O["status_int"][0], O["status_int"][1]); err != nil {
        return nil, fmt.Errorf("decode_6_235_10 status_internal: %v", err)
    } else {
        out["status_internal"] = map[string]bool{
            "racon_installed":   (sb>>4)&1 == 0,
            "racon_monitored":   (sb>>4)&1 == 1,
            "racon_operational": (sb>>3)&1 == 1,
            "racon_error":       (sb>>3)&1 == 1 && (sb>>4)&1 == 1,
            "light_on":          (sb>>2)&1 == 1,
            "light_off":         (sb>>1)&1 == 1,
            "light_error":       (sb>>1)&1 == 1 && (sb>>2)&1 == 1,
            "good_health":       sb&1 == 0,
            "alarm":             sb&1 == 1,
        }
    }

    // — External status bits (8 bits) —
    if eb, err := SafeGetUint(bits, O["status_ext"][0], O["status_ext"][1]); err != nil {
        return nil, fmt.Errorf("decode_6_235_10 status_external: %v", err)
    } else {
        ext := make(map[string]bool, 8)
        for i := 0; i < 8; i++ {
            ext[fmt.Sprintf("digital_input_%d_on", 7-i)] = ((eb>>uint(i))&1 == 1)
        }
        out["status_external"] = ext
    }

    // — Off-position status (1 bit) —
    if ofs, err := SafeGetUint(bits, O["off_pos_status"][0], O["off_pos_status"][1]); err != nil {
        return nil, fmt.Errorf("decode_6_235_10 off_position_status: %v", err)
    } else {
        out["off_position_status"] = (ofs == 1)
    }

    return out, nil
}

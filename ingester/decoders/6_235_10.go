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

// ----------------------------------------------------------------------------
// decode_6_235_10
// ----------------------------------------------------------------------------

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

    // 2) Unpack bytes into a bit-slice (MSB first)
    bits := unpackBytesToBits(raw)

    // 3) Field offsets (after stripping 18-bit header)
    O := map[string][2]int{
        "seq_num":        {0,  2},  // Sequence Number
        "dest_id":        {2, 30},  // Destination MMSI
        "retransmit":     {32, 1},  // Retransmit Flag
        // spare bit at 33
        "analogue_int":   {34, 10}, // internal analogue (0.05V steps)
        "analogue_ext1":  {44, 10}, // external analogue input 1
        "analogue_ext2":  {54, 10}, // external analogue input 2
        "status_int":     {64, 5},  // internal status bits
        "status_ext":     {69, 8},  // external status bits
        "off_pos_status": {77, 1},  // Off-position status
        // spare 4 bits at [78..81]
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

    // — Analogue readings (0.05V steps) —
    if ai := getUint(bits, O["analogue_int"][0], O["analogue_int"][1]); ai != 0 {
        out["analogue_internal_V"] = float64(ai) * 0.05
    }
    if ae1 := getUint(bits, O["analogue_ext1"][0], O["analogue_ext1"][1]); ae1 != 0 {
        out["analogue_external1_V"] = float64(ae1) * 0.05
    }
    if ae2 := getUint(bits, O["analogue_ext2"][0], O["analogue_ext2"][1]); ae2 != 0 {
        out["analogue_external2_V"] = float64(ae2) * 0.05
    }

    // — Internal status bits (5 bits) —
    if sb := getUint(bits, O["status_int"][0], O["status_int"][1]); sb <= 31 {
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
    if eb := getUint(bits, O["status_ext"][0], O["status_ext"][1]); eb <= 0xFF {
        ext := make(map[string]bool)
        for i := 0; i < 8; i++ {
            ext[fmt.Sprintf("digital_input_%d_on", 7-i)] = ((eb>>uint(i))&1 == 1)
        }
        out["status_external"] = ext
    }

    // — Off-position status —
    if ofs := getUint(bits, O["off_pos_status"][0], O["off_pos_status"][1]); ofs <= 1 {
        out["off_position_status"] = (ofs == 1)
    }

    return out, nil
}

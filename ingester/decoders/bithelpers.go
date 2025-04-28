package decoders

// getUint reads `length` bits from `bits` starting at `off`, big-endian.
func getUint(bits []byte, off, length int) int {
    v := 0
    for i := 0; i < length; i++ {
        v = (v<<1 | int(bits[off+i]))
    }
    return v
}

// getSint reads a signed two’s-complement integer.
func getSint(bits []byte, off, length int) int {
    v := getUint(bits, off, length)
    if v&(1<<(length-1)) != 0 {
        v -= 1 << length
    }
    return v
}

// unpackBytesToBits expands each byte of raw into 8 bits (MSB first).
func unpackBytesToBits(raw []byte) []byte {
    bits := make([]byte, len(raw)*8)
    for i, b := range raw {
        for j := 0; j < 8; j++ {
            bits[i*8 + j] = (b >> (7 - j)) & 1
        }
    }
    return bits
}

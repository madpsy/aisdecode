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

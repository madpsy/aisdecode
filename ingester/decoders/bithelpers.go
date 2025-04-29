package decoders

import "fmt"

// ---------------------------------------------------------------------------
// Original helpers (unchanged for backward compatibility)
// ---------------------------------------------------------------------------

// getUint reads `length` bits from `bits` starting at `off`, big-endian.
// Panics if off/length are out of range.
func getUint(bits []byte, off, length int) int {
    v := 0
    for i := 0; i < length; i++ {
        v = (v<<1 | int(bits[off+i]))
    }
    return v
}

// getSint reads a signed two’s-complement integer.
// Panics if off/length are out of range.
func getSint(bits []byte, off, length int) int {
    v := getUint(bits, off, length)
    if v&(1<<(length-1)) != 0 {
        v -= 1 << length
    }
    return v
}

// ---------------------------------------------------------------------------
// New safe wrappers (use these in your decoders to avoid panics)
// ---------------------------------------------------------------------------

// SafeGetUint does the same as getUint but returns an error if the slice
// range is invalid instead of panicking.
func SafeGetUint(bits []byte, off, length int) (int, error) {
    if off < 0 || length < 0 || off+length > len(bits) {
        return 0, fmt.Errorf(
            "SafeGetUint: out of range (len(bits)=%d, off=%d, length=%d)",
            len(bits), off, length,
        )
    }
    // reuse existing logic
    return getUint(bits, off, length), nil
}

// SafeGetSint does the same as getSint but returns an error if the slice
// range is invalid instead of panicking.
func SafeGetSint(bits []byte, off, length int) (int, error) {
    u, err := SafeGetUint(bits, off, length)
    if err != nil {
        return 0, err
    }
    if u&(1<<(length-1)) != 0 {
        u -= 1 << length
    }
    return u, nil
}

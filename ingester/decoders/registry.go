package decoders

import "fmt"

// Key identifies an AIS addon by MessageID, DAC and FI.
type Key struct {
    MessageID, DAC, FI int
}

// DecoderFunc takes the raw packet map, and returns a “DecodedBinary” map (or an error).
type DecoderFunc func(packet map[string]interface{}) (map[string]interface{}, error)

var registry = make(map[Key]DecoderFunc)

// RegisterDecoder is called by each addon in its init().
func RegisterDecoder(messageID, dac, fi int, fn DecoderFunc) {
    key := Key{messageID, dac, fi}
    if _, exists := registry[key]; exists {
        panic(fmt.Sprintf("decoder for %v already registered", key))
    }
    registry[key] = fn
}

// Get returns the DecoderFunc (if any) for this triple.
func Get(messageID, dac, fi int) (DecoderFunc, bool) {
    fn, ok := registry[Key{messageID, dac, fi}]
    return fn, ok
}

package main

import (
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
)

// BitReader helps reading bits from a byte slice.
type BitReader struct {
	data []byte
	pos  int // current bit position
}

// ReadBits reads the next n bits and returns them as a uint64.
func (r *BitReader) ReadBits(n int) (uint64, error) {
	var value uint64
	for i := 0; i < n; i++ {
		byteIndex := r.pos / 8
		bitIndex := 7 - (r.pos % 8) // read MSB first
		if byteIndex >= len(r.data) {
			return 0, errors.New("not enough bits")
		}
		bit := (r.data[byteIndex] >> bitIndex) & 1
		value = (value << 1) | uint64(bit)
		r.pos++
	}
	return value, nil
}

// ais6BitTable maps 6-bit values (0-63) to AIS characters.
// For text messages the standard conversion (ITU-R M.1371-3, Annex 5) is used.
var ais6BitTable = []rune{
	'@', 'A', 'B', 'C', 'D', 'E', 'F', 'G',
	'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
	'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
	'X', 'Y', 'Z', '[', '\\', ']', '^', '_',
	// For values 32-63, AIS typically uses punctuation/digits.
	' ', '!', '"', '#', '$', '%', '&', '\'',
	'(', ')', '*', '+', ',', '-', '.', '/',
	'0', '1', '2', '3', '4', '5', '6', '7',
	'8', '9', ':', ';', '<', '=', '>', '?',
}

func main() {
	// Accept the Base64 message as a command-line flag.
	msgPtr := flag.String("message", "", "Base64 encoded AIS message (Text Description, DAC 1, FI 29)")
	flag.Parse()
	if *msgPtr == "" {
		fmt.Println("Usage: go run main.go -message <Base64Message>")
		os.Exit(1)
	}
	base64Data := *msgPtr

	// Decode the Base64 string.
	rawData, err := base64.StdEncoding.DecodeString(base64Data)
	if err != nil {
		log.Fatalf("Error decoding Base64: %v", err)
	}

	totalBits := len(rawData) * 8
	log.Printf("Total bits in input: %d", totalBits)

	// Optionally, print the raw header bytes (first 13 bytes should cover the 98 header bits)
	if len(rawData) >= 13 {
		fmt.Print("Raw header bytes: ")
		for i := 0; i < 13; i++ {
			fmt.Printf("%02X ", rawData[i])
		}
		fmt.Println()
	}

	// Create a BitReader.
	br := BitReader{data: rawData}

	// --- Fixed Header Fields (98 bits) ---

	// 1. Message ID (6 bits)
	messageID, _ := br.ReadBits(6)
	// 2. Repeat Indicator (2 bits)
	repeatIndicator, _ := br.ReadBits(2)
	// 3. Source ID (30 bits)
	sourceID, _ := br.ReadBits(30)
	// 4. Sequence Number (2 bits)
	sequenceNumber, _ := br.ReadBits(2)
	// 5. Destination ID (30 bits)
	destinationID, _ := br.ReadBits(30)
	// 6. Retransmit Flag (1 bit)
	retransmitFlag, _ := br.ReadBits(1)
	// 7. Spare (1 bit)
	spare, _ := br.ReadBits(1)
	// 8. IAI (16 bits)
	iai, _ := br.ReadBits(16)
	dac := iai >> 8
	fi := iai & 0xFF
	// 9. Message Linkage ID (10 bits)
	messageLinkageID, _ := br.ReadBits(10)

	// --- Text String Field ---
	remainingBits := totalBits - br.pos
	numChars := remainingBits / 6 // each character is 6 bits
	text := ""
	for i := 0; i < int(numChars); i++ {
		charVal, _ := br.ReadBits(6)
		var ch rune
		if charVal < uint64(len(ais6BitTable)) {
			ch = ais6BitTable[charVal]
		} else {
			ch = ' '
		}
		text += string(ch)
	}

	// --- Output the Decoded Message ---
	fmt.Println("=== Decoded AIS Text Description Message (DAC 1, FI 29) ===")
	fmt.Printf("Message ID: %d\n", messageID)
	fmt.Printf("Repeat Indicator: %d\n", repeatIndicator)
	fmt.Printf("Source ID (MMSI): %d\n", sourceID)
	fmt.Printf("Sequence Number: %d\n", sequenceNumber)
	fmt.Printf("Destination ID (MMSI): %d\n", destinationID)
	fmt.Printf("Retransmit Flag: %d\n", retransmitFlag)
	fmt.Printf("Spare: %d\n", spare)
	fmt.Printf("IAI: %d (DAC: %d, FI: %d)\n", iai, dac, fi)
	fmt.Printf("Message Linkage ID: %d\n", messageLinkageID)
	fmt.Printf("Text String (%d characters):\n%s\n", numChars, text)

	if br.pos != totalBits {
		fmt.Printf("Note: %d bits remaining unprocessed.\n", totalBits-br.pos)
	} else {
		fmt.Println("All bits have been processed.")
	}
}


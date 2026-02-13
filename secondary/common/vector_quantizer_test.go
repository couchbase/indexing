package common

import (
	"fmt"
	"testing"
)

func TestVectorQuantizerParser(t *testing.T) {

	dimension := 1024

	validInputs := []string{
		"IVF,PQ32x4",
		"IVF,    PQ32x4", // Spaces are supported
		"IVF,PQ32x5",     // number of bits can be odd
		"IVF1024,PQ32x4", // Specify the number of centroids
		"IVF,pq16x8",     // case-insensitive
		// "IVF, SQ_4bit",
		// "IVF, SQ_8bit",
		// "IVF, SQ_6bit",
		// "IVF, SQ_fp16",
		// "IVF, SQ_4bit_uniform",
		// "IVF, SQ_8bit_uniform",
		// "IVF, SQ_8bit_direct",
		// "IVF,PQ32x4fs", // commented outdated inputs which were leading to test failure
		"IVF43241343143214,PQ32x4", // large number of centroids
	}

	rabitqValidParseInputs := []struct {
		input         string
		expectedNlist int
		expectedNbits int
	}{
		{"IVF,RaBitQ", 0, 1},
		{"IVF, RaBitQ", 0, 1},       // Spaces are supported
		{"IVF1024,RaBitQ", 1024, 1}, // Default RaBitQ bits when suffix is omitted
		{"IVF,RaBitQ1", 0, 1},
		{"IVF,RaBitQ2", 0, 2},
		{"IVF,rabitq4", 0, 4}, // case-insensitive
	}

	invalidFormats := []string{
		"ABC,PQ32x4",   // No IVF leading
		"IVF,PQ",       // No sub quantizers and nbits
		"IVF,SQ",       // Not a valid model
		"IVF1024,ab",   // Invalid quantization scheme
		"IVF,RaBitQX",  // Invalid RaBitQ format
		"IVF,RaBitQ-1", // Invalid RaBitQ format
	}

	invalidInputs := []string{

		"IVF256,PQ31x4", // dimension % 31 != 0
		"IVF0,PQ32x4",   // nlist can not be specified as zero
		"IVF,PQ0x4",     // subquantizers can not be zero
		"IVF,PQ0x4fs",   // blocksize can not be zero
		"IVF,PQ32x0",    // nbits can not be zero
		"IVF,RaBitQ10",  // RaBitQNbits must be between 1 and 9
		"IVF,RaBitQ0",   // RaBitQNbits must be between 1 and 9

	}

	for _, inp := range validInputs {
		fmt.Printf("Parsing input: %v\n", inp)
		quantizer, err := ParseVectorDesciption(inp)
		if err != nil {
			t.Fatalf("Expected successful parsing but observed error: %v for inp: %v", err, inp)
		}

		if err = quantizer.IsValid(dimension, false); err != nil {
			t.Fatalf("Invalid quantizer seen with error: %v for inp: %v", err, inp)
		}
	}

	for _, tc := range rabitqValidParseInputs {
		fmt.Printf("Parsing RaBitQ input: %v\n", tc.input)
		quantizer, err := ParseVectorDesciption(tc.input)
		if err != nil {
			t.Fatalf("Expected successful RaBitQ parsing but observed error: %v for input: %v",
				err, tc.input)
		}
		if quantizer.Type != RaBitQ {
			t.Fatalf("Expected quantizer type RaBitQ but got %v for input: %v",
				quantizer.Type, tc.input)
		}
		if quantizer.Nlist != tc.expectedNlist {
			t.Fatalf("Expected nlist %v but got %v for input: %v",
				tc.expectedNlist, quantizer.Nlist, tc.input)
		}
		if quantizer.RaBitQNbits != tc.expectedNbits {
			t.Fatalf("Expected RaBitQ nbits %v but got %v for input: %v",
				tc.expectedNbits, quantizer.RaBitQNbits, tc.input)
		}
	}

	for _, inp := range invalidFormats {
		fmt.Printf("Parsing input: %v\n", inp)
		_, err := ParseVectorDesciption(inp)
		if err == nil {
			t.Fatalf("Expected parsing to fail parsing succeeded for inp: %v", inp)
		}
	}

	for _, inp := range invalidInputs {
		fmt.Printf("Parsing input: %v\n", inp)
		quantizer, _ := ParseVectorDesciption(inp)
		if quantizer.IsValid(dimension, false) == nil {
			t.Fatalf("Expected invalid quantizer but found a valid one for inp: %v", inp)
		}
	}

}

func TestSparseVectorQuantizerParser(t *testing.T) {

	validInputs := []struct {
		input         string
		expectedNlist int
	}{
		{"IVF1024", 1024},
		{"IVF256", 256},
		{"ivf512", 512},                       // case-insensitive
		{"  IVF1024  ", 1024},                 // spaces are trimmed
		{"IVF1", 1},                           // minimum valid nlist
		{"IVF43241343143214", 43241343143214}, // large number of centroids
	}

	invalidFormats := []string{
		"ABC",        // No IVF leading
		"PQ32x4",     // Not a sparse vector format
		"SQ8",        // Not a sparse vector format
		"IVF,SQ8",    // No quantization is supported for sparse
		"IVF,PQ32x4", // No quantization is supported for sparse
	}

	invalidInputs := []string{
		"IVF0", // nlist can not be zero
		"IVF",  // nlist must be specified
	}

	for _, tc := range validInputs {
		fmt.Printf("Parsing sparse vector input: %v\n", tc.input)
		quantizer, err := ParseSparseVectorDescription(tc.input)
		if err != nil {
			t.Fatalf("Expected successful parsing but observed error: %v for input: %v", err, tc.input)
		}

		fmt.Printf("  Result: Nlist=%v, Type=%v, SubQuantizers=%v, Nbits=%v, SQRange=%v\n",
			quantizer.Nlist, quantizer.Type, quantizer.SubQuantizers, quantizer.Nbits, quantizer.SQRange)

		if quantizer.Nlist != tc.expectedNlist {
			t.Fatalf("Expected nlist %v but got %v for input: %v", tc.expectedNlist, quantizer.Nlist, tc.input)
		}
	}

	for _, inp := range invalidFormats {
		fmt.Printf("Parsing sparse vector input: %v\n", inp)
		_, err := ParseSparseVectorDescription(inp)
		if err == nil {
			t.Fatalf("Expected parsing to fail but parsing succeeded for input: %v", inp)
		}
		fmt.Printf("  Error (expected): %v\n", err)
	}

	for _, inp := range invalidInputs {
		fmt.Printf("Parsing sparse vector input: %v\n", inp)
		_, err := ParseSparseVectorDescription(inp)
		if err == nil {
			t.Fatalf("Expected parsing to fail but parsing succeeded for input: %v", inp)
		}
		fmt.Printf("  Error (expected): %v\n", err)
	}
}

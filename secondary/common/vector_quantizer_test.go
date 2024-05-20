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
		"IVF, SQ_4bit",
		"IVF, SQ_8bit",
		"IVF, SQ_6bit",
		"IVF, SQ_fp16",
		"IVF, SQ_4bit_uniform",
		"IVF, SQ_8bit_uniform",
		"IVF, SQ_8bit_direct",
		"IVF,PQ32x4fs",
		"IVF43241343143214,PQ32x4", // large number of centroids
	}

	invalidFormats := []string{
		"ABC,PQ32x4", // No IVF leading
		"IVF,PQ",     // No sub quantizers and nbits
		"IVF,SQ",     // Not a valid model
		"IVF1024,ab", // Invalid quantization scheme
	}

	invalidInputs := []string{

		"IVF256,PQ31x4", // dimension % 31 != 0
		"IVF0,PQ32x4",   // nlist can not be specified as zero
		"IVF,PQ0x4",     // subquantizers can not be zero
		"IVF,PQ0x4fs",   // blocksize can not be zero
		"IVF,PQ32x0",    // nbits can not be zero

	}

	for _, inp := range validInputs {
		fmt.Printf("Parsing input: %v\n", inp)
		quantizer, err := ParseVectorDesciption(inp)
		if err != nil {
			t.Fatalf("Expected successful parsing but observed error: %v for inp: %v", err, inp)
		}

		if err = quantizer.IsValid(dimension); err != nil {
			t.Fatalf("Invalid quantizer seen with error: %v for inp: %v", err, inp)
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
		if quantizer.IsValid(dimension) == nil {
			t.Fatalf("Expected invalid quantizer but found a valid one for inp: %v", inp)
		}
	}

}

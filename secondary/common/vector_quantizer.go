package common

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type QuantizationType string

const (
	PQ QuantizationType = "PQ"
	SQ QuantizationType = "SQ"
)

type ScalarQuantizerRange string

const (
	NONE            ScalarQuantizerRange = ""
	SQ_8BIT                              = "SQ_8BIT"
	SQ_4BIT                              = "SQ_4BIT"
	SQ_8BIT_UNIFORM                      = "SQ_8BIT_UNIFORM"
	SQ_4BIT_UNIFORM                      = "SQ_4BIT_UNIFORM"
	SQ_FP16                              = "SQ_FP16"
	SQ_8BIT_DIRECT                       = "SQ_8BIT_DIRECT"
	SQ_6BIT                              = "SQ_6BIT"
)

type VectorQuantizer struct {
	Type QuantizationType `json:"quantization_type,omitempty"`

	// Number of centroids to be used
	// E.g., IVF1024,SQ_8BIT means that 1024 centroids are to be
	// used. This is an optional parameter. If not specified, indexer
	// will compute the number of centroids to be used based on items
	// in the keyspace at the time of index build
	Nlist int `json:"nlist,omitempty"`

	// Product quantizer related information
	SubQuantizers int `json:"subquantizers,omitempty"`
	Nbits         int `json:"nbits,omitempty"`

	FastScan  bool `json:"fastScan,omitempty"`
	BlockSize int  `json:"blockSize,omitempty"`

	// Scalar quantizer related information
	SQRange ScalarQuantizerRange `json:"sqrange,omitempty"`
}

func (vq *VectorQuantizer) Clone() *VectorQuantizer {
	if vq == nil {
		return nil
	}

	return &VectorQuantizer{
		Type:          vq.Type,
		Nlist:         vq.Nlist,
		SubQuantizers: vq.SubQuantizers,
		Nbits:         vq.Nbits,
		SQRange:       vq.SQRange,
	}
}

func (vq *VectorQuantizer) String() string {
	if vq == nil {
		return ""
	}

	switch vq.Type {
	case PQ:

		if vq.FastScan {
			if vq.Nlist > 0 {
				return fmt.Sprintf("IVF%v,PQ%vx%vfs", vq.Nlist, vq.BlockSize, vq.Nbits)
			} else {
				return fmt.Sprintf("IVF,PQ%vx%vfs", vq.BlockSize, vq.Nbits)
			}
		} else {
			if vq.Nlist > 0 {
				return fmt.Sprintf("IVF%v,PQ%vx%v", vq.Nlist, vq.SubQuantizers, vq.Nbits)
			} else {
				return fmt.Sprintf("IVF,PQ%vx%v", vq.SubQuantizers, vq.Nbits)
			}
		}
	case SQ:
		if vq.Nlist > 0 {
			return fmt.Sprintf("IVF%v,%v", vq.Nlist, vq.SQRange)
		} else {
			return fmt.Sprintf("IVF,%v", vq.SQRange)
		}
	}
	return ""
}

// When parsing an input string, indexer expects the following semantics:
//
// a. Product Quantization - IVF<num_centroids>,PQ<m>x<n>. 'm' and 'n' are variables in
// this string. 'm' represents the number of subquantizers and 'n' represents
// the number of bits per quantized index
//
// b. Scalar Quantization - IVF<num_centroids>,SQ_<val> where val is one of the strings supported
// by "ScalarQuantizerType". E.g., IVFSQ_8bit represents IVF index with scalar
// quantization on 8bit scale
//
// The num_centroids value is optional. If not specified, indexer will compute
// the number of centroids required at the time of build index.
// In all other cases, indexer should return error
func ParseVectorDesciption(inp string) (*VectorQuantizer, error) {
	inp = strings.TrimSpace(inp)
	inp = strings.ToUpper(inp)
	quantizer := &VectorQuantizer{}

	re := regexp.MustCompile(`(IVF)(\d*),( *)(PQ(\d+)X(\d+)(FS)?$|SQ_FP16$|SQ_4BIT$|SQ_6BIT$|SQ_8BIT$|SQ_4BIT_UNIFORM$|SQ_8BIT_UNIFORM$|SQ_8BIT_DIRECT$)`)
	matches := re.FindStringSubmatch(inp)

	// On a successful match, there will be 8 substrings in matches
	if len(matches) == 0 || len(matches) != 8 {
		return nil, fmt.Errorf("Invalid format for description")
	}

	// nlist parameter
	if len(matches[2]) > 0 {
		nlist, err := strconv.Atoi(matches[2])
		if err != nil {
			return nil, fmt.Errorf("Error observed while parsing number of centroids, err: %v", err)
		}

		if nlist == 0 {
			return nil, fmt.Errorf("number of centroids can not be equal to zero")
		}
		quantizer.Nlist = nlist
	}

	if strings.HasPrefix(matches[4], "PQ") {
		quantizer.Type = PQ

		if len(matches[7]) > 0 { // Fast scan specified
			quantizer.FastScan = true

			if blockSize, err := strconv.ParseInt(matches[5], 10, 32); err != nil {
				return nil, fmt.Errorf("Error observed while parsing number of subquantizers, err: %v", err)
			} else {
				quantizer.BlockSize = int(blockSize)
			}
		} else { // Normal product quantization
			if subQuantizers, err := strconv.ParseInt(matches[5], 10, 32); err != nil {
				return nil, fmt.Errorf("Error observed while parsing number of subquantizers, err: %v", err)
			} else {
				quantizer.SubQuantizers = int(subQuantizers)
			}
		}

		if nbits, err := strconv.ParseInt(matches[6], 10, 32); err != nil {
			return nil, fmt.Errorf("Error observed while parsing number of bits per quantizer, err: %v", err)
		} else {
			quantizer.Nbits = int(nbits)
		}

	} else if strings.HasPrefix(matches[4], "SQ") {
		quantizer.Type = SQ

		switch ScalarQuantizerRange(matches[4]) {
		case SQ_8BIT, SQ_8BIT_DIRECT, SQ_8BIT_UNIFORM, SQ_6BIT, SQ_4BIT, SQ_4BIT_UNIFORM, SQ_FP16:
			quantizer.SQRange = ScalarQuantizerRange(matches[4])
		default:
			return nil, fmt.Errorf("Invalid format for scalar quantization. Expected one of `SQ_4bit`,`SQ_6bit`,`SQ_8bit`,`SQ_4bit_uniform`,`SQ_8bit_uniform`,`SQ_8bit_direct`,`SQ_fp16`. Observed different format")
		}
	} else {
		return nil, fmt.Errorf("Invalid format for description")
	}
	return quantizer, nil
}

func (vq *VectorQuantizer) IsValid(dimension int) error {

	if vq == nil {
		return errors.New("Nil quantizer is not expected")
	}

	// As scalar quantizer works only with specific set of types
	// which are captured when parsing the description, return nil error
	if vq.Type == SQ {
		return nil
	}

	if vq.Type == PQ {
		if vq.Nbits == 0 { // Nbits should never be zero for PQ
			return errors.New("Number of bits per quantized value is zero. It should be greater than zero")
		}

		if vq.FastScan {
			if vq.BlockSize == 0 {
				return errors.New("BlockSize should be greater than zero for product quantization with fast scan")
			} else if dimension%vq.BlockSize != 0 {
				return errors.New("Dimension should be divisible by BlockSize for product quantization with fast scan")
			}
			return nil
		} else {
			if vq.SubQuantizers == 0 {
				return errors.New("SubQuantizers should be greater than zero for product quantization")
			} else if dimension%vq.SubQuantizers != 0 {
				return errors.New("Dimension should be divisible by SubQuantizers for product quantization")
			}
			return nil
		}
	}
	return errors.New("Invalid quantization scheme seen")
}

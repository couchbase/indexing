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
	// NO_QUANTIZATION_SPARSE is used only for sparse vectors
	NO_QUANTIZATION_SPARSE QuantizationType = "SPARSE"
	PQ                     QuantizationType = "PQ"
	SQ                     QuantizationType = "SQ"
	RaBitQ                 QuantizationType = "RaBitQ"
)

type ScalarQuantizerRange string

const (
	NONE            ScalarQuantizerRange = ""
	SQ_8BIT                              = "SQ8"
	SQ_4BIT                              = "SQ4"
	SQ_8BIT_UNIFORM                      = "SQ_8BIT_UNIFORM"
	SQ_4BIT_UNIFORM                      = "SQ_4BIT_UNIFORM"
	SQ_FP16                              = "SQFP16"
	SQ_8BIT_DIRECT                       = "SQ_8BIT_DIRECT"
	SQ_6BIT                              = "SQ6"
)

var DEFAULT_VECTOR_DESCRIPTION = "IVF,SQ8"
var DEFAULT_SPARSE_VECTOR_DESCRIPTION = "IVF"

var (
	ErrRaBitQNbitsOutOfRange = errors.New("RaBitQ nb_bits must be between 1 and 9")
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

	// RaBitQ (Binary Quantizer) related information
	RaBitQNbits int `json:"rabitqNbits,omitempty"`

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
		RaBitQNbits:   vq.RaBitQNbits,
		FastScan:      vq.FastScan,
		BlockSize:     vq.BlockSize,
		SQRange:       vq.SQRange,
	}
}

func (vq *VectorQuantizer) IsEquivalent(uq *VectorQuantizer) bool {

	if vq.Type != uq.Type {
		return false
	}

	if vq.Nlist != uq.Nlist {
		return false
	}

	if vq.SubQuantizers != uq.SubQuantizers {
		return false
	}

	if vq.Nbits != uq.Nbits {
		return false
	}

	if vq.RaBitQNbits != uq.RaBitQNbits {
		return false
	}

	if vq.FastScan != uq.FastScan {
		return false
	}

	if vq.BlockSize != uq.BlockSize {
		return false
	}

	if vq.SQRange != uq.SQRange {
		return false
	}

	return true
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
	case RaBitQ:
		// Build the string: IVF[<nlist>],RaBitQ[<nbits>]
		prefix := "IVF"
		if vq.Nlist > 0 {
			prefix = fmt.Sprintf("IVF%v", vq.Nlist)
		}
		suffix := "RaBitQ"
		if vq.RaBitQNbits > 1 {
			suffix = fmt.Sprintf("RaBitQ%v", vq.RaBitQNbits)
		}
		return fmt.Sprintf("%s,%s", prefix, suffix)
	case NO_QUANTIZATION_SPARSE:
		if vq.Nlist > 0 {
			return fmt.Sprintf("IVF%v", vq.Nlist)
		} else {
			return fmt.Sprintf("IVF")
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
// c. RaBitQ (Binary Quantization) - IVF<num_centroids>,RaBitQ<nb_bits> where
// nb_bits is the number of bits per dimension. E.g., IVF,RaBitQ4 represents
// IVF index with RaBitQ quantization with 4 bits per dimension
//
// The num_centroids value is optional. If not specified, indexer will compute
// the number of centroids required at the time of build index.
// In all other cases, indexer should return error
func ParseVectorDesciption(inp string) (*VectorQuantizer, error) {
	inp = strings.TrimSpace(inp)
	inp = strings.ToUpper(inp)

	quantizer := &VectorQuantizer{}

	re := regexp.MustCompile(`^(IVF)(\d*),( *)(PQ(\d+)X(\d+)(FS)?$|SQFP16$|SQ4$|SQ6$|SQ8$|` +
		`SQ_4BIT_UNIFORM$|SQ_8BIT_UNIFORM$|SQ_8BIT_DIRECT$|RABITQ(\d*)$)`)
	matches := re.FindStringSubmatch(inp)

	// On a successful match, there will be 9 substrings in matches
	if len(matches) == 0 || len(matches) != 9 {
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
			return nil, fmt.Errorf("Currently FastScan is not supported")

			// quantizer.FastScan = true
			// if blockSize, err := strconv.ParseInt(matches[5], 10, 32); err != nil {
			// 	return nil, fmt.Errorf("Error observed while parsing number of subquantizers, err: %v", err)
			// } else {
			// 	quantizer.BlockSize = int(blockSize)
			// }
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
		case SQ_8BIT, SQ_6BIT, SQ_4BIT:
			quantizer.SQRange = ScalarQuantizerRange(matches[4])
		case SQ_8BIT_DIRECT, SQ_8BIT_UNIFORM, SQ_4BIT_UNIFORM, SQ_FP16:
			return nil, fmt.Errorf("Currently only `SQ4`,`SQ6`,`SQ8` are supported for scalar quantization.")
		default:
			return nil, fmt.Errorf("Invalid format for scalar quantization. Expected one of `SQ4`,`SQ6`,`SQ8`,`SQfp16`,`SQ_8bit_DIRECT`,`SQ_8bit_UNIFORM`,`SQ_4bit_UNIFORM`. Observed different format")
		}
	} else if strings.HasPrefix(matches[4], "RABITQ") {
		quantizer.Type = RaBitQ
		if len(matches[8]) == 0 {
			quantizer.RaBitQNbits = 1
		} else if rabitqNbits, err := strconv.ParseInt(matches[8], 10, 32); err != nil {
			return nil, fmt.Errorf(
				"Error observed while parsing number of bits per RaBitQ quantizer: %w",
				err)
		} else {
			quantizer.RaBitQNbits = int(rabitqNbits)
		}
	} else {
		return nil, fmt.Errorf("Invalid format for description")
	}
	return quantizer, nil
}

// For sparse vectors
// * description - optional (default “IVF”)
func ParseSparseVectorDescription(inp string) (*VectorQuantizer, error) {
	inp = strings.TrimSpace(inp)
	inp = strings.ToUpper(inp)

	quantizer := &VectorQuantizer{}
	quantizer.Type = NO_QUANTIZATION_SPARSE

	re := regexp.MustCompile(`^(IVF)(\d*)$`)
	matches := re.FindStringSubmatch(inp)

	if len(matches) == 0 || len(matches) != 3 {
		return nil, fmt.Errorf("Invalid format for sparse vector description")
	}

	nlist, err := strconv.Atoi(matches[2])
	if err != nil {
		return nil, fmt.Errorf("Error observed while parsing number of centroids, err: %v", err)
	}

	if nlist == 0 {
		return nil, fmt.Errorf("number of centroids can not be equal to zero")
	}
	quantizer.Nlist = nlist

	return quantizer, nil
}

func (vq *VectorQuantizer) IsValid(dimension int, isSparseVector bool) error {

	if vq == nil {
		return errors.New("Nil quantizer is not expected")
	}

	// Add validation for sparse vector quantizer
	if isSparseVector {
		if vq.Type != NO_QUANTIZATION_SPARSE {
			return errors.New("quantizer is supported only for dense vectors. Observed it for sparse vector")
		}
		return nil
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
			} else if vq.Nbits != 4 {
				return errors.New("Currently only nbits=4 is supported with fast scan")
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

	if vq.Type == RaBitQ {
		if vq.RaBitQNbits < 1 || vq.RaBitQNbits > 9 {
			return fmt.Errorf("%w, got %d", ErrRaBitQNbitsOutOfRange, vq.RaBitQNbits)
		}
		return nil
	}

	return errors.New("Invalid quantization scheme seen")
}

package faiss

/*
#include <faiss/c_api/IndexIVFFlat_c.h>
#include <faiss/c_api/MetaIndexes_c.h>
#include <faiss/c_api/Index_c.h>
#include <faiss/c_api/IndexIVF_c.h>
#include <faiss/c_api/IndexIVF_c_ex.h>
*/
import "C"
import "fmt"

// pass nprobe to be set as index time option for IVF indexes only.
// varying nprobe impacts recall but with an increase in latency.
func (idx *IndexImpl) SetNProbe(nprobe int32) {
	ivfPtr := C.faiss_IndexIVF_cast(idx.cPtr())
	if ivfPtr == nil {
		return
	}
	C.faiss_IndexIVF_set_nprobe(ivfPtr, C.ulong(nprobe))
}

//Quantizer returns the pointer to the quantizer for
//IVF family indexes only.
func (idx *IndexImpl) Quantizer() (*IndexImpl, error) {
	ivfPtr := C.faiss_IndexIVF_cast(idx.cPtr())
	if ivfPtr == nil {
		return nil, fmt.Errorf("index is not of ivf type")
	}
	quantizer := C.faiss_IndexIVF_quantizer(ivfPtr)

	return &IndexImpl{&faissIndex{quantizer}}, nil
}

//returns the pointer to the quantizer for
//IVF family indexes only.
func (idx *IndexImpl) EncodeVectors(x []float32, nsub int, nbits int, nlist int) (
	codes []uint8, err error) {

	ivfPtr := C.faiss_IndexIVF_cast(idx.cPtr())
	if ivfPtr == nil {
		return nil, fmt.Errorf("index is not of ivf type")
	}

	//TODO check why this function always returns 0
	/*
		var coarse_size C.size_t
		if c := C.faiss_Index_sa_code_size(
			ivfPtr,
			&coarse_size,
		); c != 0 {
			err = getLastError()
		}

		if err != nil {
			return
		}
	*/

	//compute coarse code size based on nlist
	coarse_size := func(nlist int) int {
		nl := nlist - 1
		nbyte := 0
		for nl > 0 {
			nbyte++
			nl >>= 8
		}
		return nbyte
	}(nlist)

	n := len(x) / idx.D()
	//code size is dependent on nbits and nsub
	code_size := (nbits*nsub + 7) / 8
	//final encoded code size includes coarse size
	codes = make([]uint8, n*(code_size+coarse_size))
	if c := C.faiss_Index_sa_encode(
		ivfPtr,
		C.idx_t(n),
		(*C.float)(&x[0]),
		(*C.uint8_t)(&codes[0]),
	); c != 0 {
		err = getLastError()
	}

	//strip the coarse_size, only code needs to be returned
	codes = stripCoarseSize(codes, code_size, coarse_size)

	return
}

//stripCoarseSize is a helper function to strip out the coarse code from
//the quantized code. This function doesn't allocate a new slice.
func stripCoarseSize(codes []uint8, code_size, coarse_size int) []uint8 {

	total_size := code_size + coarse_size

	num_codes := len(codes) / total_size

	start_pos := 0
	copy_to := 0
	for i := 0; i < num_codes; i++ {
		copy_from := start_pos + coarse_size //actual code
		copy(codes[copy_to:copy_to+code_size], codes[copy_from:copy_from+code_size])
		start_pos += total_size //pos of next code before strip
		copy_to += code_size    //pos of next code after strip
	}
	codes = codes[:copy_to]
	return codes
}

all: clean comp run

export GOPATH=$(PWD)/../../../../../../../../goproj:$(PWD)/../../../../../../../../godeps
export GO111MODULE=auto
export CGO_CFLAGS=-I$(PWD)/../../../../../../../../sigar/include -I$(PWD)/../../../../../../../../build/tlm/deps/zstd-cpp.exploded/include -I$(PWD)/../../../../../../../../build/tlm/deps/jemalloc.exploded/include -I$(PWD)/../../../../../../../../forestdb/include/ -DJEMALLOC=1
export CGO_LDFLAGS=-L $(PWD)/../../../../../../../../install/lib -Wl,-rpath $(PWD)/../../../../../../../../install/lib

clean:
	rm -rf ./randdocs

comp:
	go build -o randdocs cmd/main.go

run:
	./randdocs -config ./cmd/config.json

nomodcomp:
	env GO111MODULE=off go build -o randdocs cmd/main.go

loadrandvecs:
	./randdocs -config ./cmd/config.json -genVectors

loadsiftsmall:
	./randdocs -config ./cmd/config.json -useSIFTSmall -NumDocs 40000 -skipNormalData

loadsparsesmall:
	./randdocs -config ./cmd/config.json -useSparseSmall -NumDocs 100000 -skipNormalData

# Generate random sparse vectors with variable dimensions (default)
loadrandsparse:
	./randdocs -config ./cmd/config.json -genSparseVectors -NumDocs 10000

# Generate random sparse vectors with fixed dimensions (e.g., 50 non-zero dimensions per vector)
# Usage: make loadrandsparse-fixed SPARSE_DIM=50 NUM_DOCS=10000
SPARSE_DIM ?= 50
NUM_DOCS ?= 10000
loadrandsparse-fixed:
	./randdocs -config ./cmd/config.json -genSparseVectors -sparseVecDim $(SPARSE_DIM) -NumDocs $(NUM_DOCS)

# Sparse vector dataset download from https://storage.googleapis.com/ann-challenge-sparse-vectors/
SPARSE_DATA_URL := https://storage.googleapis.com/ann-challenge-sparse-vectors/csr
SPARSE_DIR := ./sparsesmall

# Setup sparsesmall directory
setup-sparsesmall-dir:
	mkdir -p $(SPARSE_DIR)

# Download and extract small sparse dataset (~67MB compressed, 100K vectors)
download-sparsesmall: setup-sparsesmall-dir
	@echo "Downloading base_small.csr.gz..."
	curl -L -o $(SPARSE_DIR)/base_small.csr.gz $(SPARSE_DATA_URL)/base_small.csr.gz
	@echo "Extracting base_small.csr.gz..."
	gunzip -f $(SPARSE_DIR)/base_small.csr.gz
	@echo "Downloading queries.dev.csr.gz..."
	curl -L -o $(SPARSE_DIR)/queries.dev.csr.gz $(SPARSE_DATA_URL)/queries.dev.csr.gz
	@echo "Extracting queries.dev.csr.gz..."
	gunzip -f $(SPARSE_DIR)/queries.dev.csr.gz
	@echo "Downloading ground truth..."
	curl -L -o $(SPARSE_DIR)/base_small.dev.gt $(SPARSE_DATA_URL)/base_small.dev.gt
	@echo "Sparse small dataset ready in $(SPARSE_DIR)/"

# Download and extract 1M sparse dataset (~667MB compressed)
download-sparse1m: setup-sparsesmall-dir
	@echo "Downloading base_1M.csr.gz..."
	curl -L -o $(SPARSE_DIR)/base_1M.csr.gz $(SPARSE_DATA_URL)/base_1M.csr.gz
	@echo "Extracting base_1M.csr.gz..."
	gunzip -f $(SPARSE_DIR)/base_1M.csr.gz
	@echo "Downloading ground truth..."
	curl -L -o $(SPARSE_DIR)/base_1M.dev.gt $(SPARSE_DATA_URL)/base_1M.dev.gt
	@echo "Sparse 1M dataset ready in $(SPARSE_DIR)/"

# Download and extract full sparse dataset (~5.9GB compressed)
download-sparsefull: setup-sparsesmall-dir
	@echo "Downloading base_full.csr.gz (this may take a while)..."
	curl -L -o $(SPARSE_DIR)/base_full.csr.gz $(SPARSE_DATA_URL)/base_full.csr.gz
	@echo "Extracting base_full.csr.gz..."
	gunzip -f $(SPARSE_DIR)/base_full.csr.gz
	@echo "Downloading ground truth..."
	curl -L -o $(SPARSE_DIR)/base_full.dev.gt $(SPARSE_DATA_URL)/base_full.dev.gt
	@echo "Sparse full dataset ready in $(SPARSE_DIR)/"

# Clean sparse data directory
clean-sparse:
	rm -rf $(SPARSE_DIR)

# Setup and download sparse small dataset in one step
setup-sparsesmall: setup-sparsesmall-dir download-sparsesmall

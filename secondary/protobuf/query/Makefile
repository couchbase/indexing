.PHONY: clean build

all: build

fiximport: $(patsubst %.proto,%.pb.go,$(wildcard *.proto))

build:
	@echo "Compiling *.proto files ..."
	@protoc --go_out=. *.proto

clean:
	@rm *.pb.go

%.pb.go: FORCE
	@echo "Fixing import for $@ ..."
	@sed 's:import proto "github.com/golang/protobuf/proto":import proto "github.com/golang/protobuf/proto":' < $@  > tmpfile; mv tmpfile $@

FORCE:


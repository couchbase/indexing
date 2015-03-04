.PHONY: clean build fiximport

all: build fiximport

fiximport: $(patsubst %.proto,%.pb.go,$(wildcard *.proto))

build:
	@echo "Compiling *.proto files ..."
	@protoc --go_out=. *.proto

clean:
	@rm *.pb.go

%.pb.go: FORCE
	@echo "Fixing import for $@ ..."
	@sed 's:import proto "code.google.com/p/goprotobuf/proto":import proto "code.google.com/p/goprotobuf/proto":' < $@  > tmpfile; mv tmpfile $@

FORCE:


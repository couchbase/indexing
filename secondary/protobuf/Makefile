PROTOS = $(wildcard */*.proto)
TARGET = $(PROTOS:.proto=.pb.go)
GO_PACKAGE = $(subst ;,,$(lastword $(shell grep package $(1))))

.PHONY: all clean
all: $(TARGET)
clean:
	$(RM) $(TARGET)
%.pb.go: %.proto
	protoc --proto_path=$(dir $^) --go_out=$(dir $^) $(foreach protofile, $(filter $(dir $^)%, $(PROTOS)), --go_opt=M$(notdir $(protofile))="./;$(call GO_PACKAGE, $^)") $(notdir $^)

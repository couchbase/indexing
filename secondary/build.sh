#!/bin/bash

if [ "$C_INCLUDE_PATH" == "" ]; then
  top="`pwd`/../../../../../.."
  export GOPATH="$top/goproj:$top/godeps"
  export C_INCLUDE_PATH="$top/install/platform/include:$top/install/include:$top/forestdb/include:$top/install/build/tlm/deps/curl.exploded/include:$top/sigar/include"
  export CGO_LDFLAGS="-L $top/install/lib"
  export LD_LIBRARY_PATH="$top/install/lib"
fi

build_indexer(){

    echo "Building Indexer..."
    cd $top/build
    make indexer
    echo "Done"
    echo "Indexer binary under bin/"
}

clean_indexer(){

    cd $top/build
    make clean
}

build_projector(){

    echo "Building Projector..."
    cd $top/build
    make projector
    echo "Done"
    echo "Projector binary under bin/"
}

clean_projector(){

    cd $top/build
    go clean
    make clean
}

build_protobuf(){

    if which protoc; then
      echo "Building Protobuf..."
      cd protobuf
      make
      cd ..
      echo "Done"
    fi
}

if [ -z "$1" ]
    then
    build_protobuf
    build_indexer
    build_projector
elif [ $1 == "indexer" ]
    then
    build_indexer
elif [ $1 == "projector" ]
    then
    build_projector
elif [ $1 == "protobuf" ]
    then
    build_protobuf
elif [ $1 == "clean" ]
    then
    echo "Cleaning..."
    clean_indexer
    clean_projector
    echo "Done"
else
    echo "Unknown build option"
fi

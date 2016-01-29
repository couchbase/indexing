CMAKE_MINIMUM_REQUIRED (VERSION 2.8)
PROJECT (indexing)

# This project is intended to be built as part of a Couchbase Server build,
# but for nominal support building stand-alone, the following variables
# need to be specified:
#
# CMAKE_MODULE_PATH - should contain a directory containing
#      FindCouchbaseGo.cmake, eg. tlm/cmake/Modules
#
# CMAKE_INSTALL_PREFIX - should point to a directory where the bin/ output
#      directory will be placed
#
# GODEPSDIR - should point to a Go workspace directory containing all
#      transitive Go dependencies
#
# FORESTDB_INCLUDE_DIR - should point to where libforestdb/forestdb.h exists
#
# FORESTDB_LIBRARY_DIR - should point to where libforestdb.so resides
#
# Optionally:
#
# FORESTDB_TARGET - if set, assumed to the the CMake target name which
# builds libforestdb.so; will set dependencies to ensure it is build
# before attempting to compile indexer

INCLUDE (FindCouchbaseGo)
INCLUDE (FindCouchbaseJemalloc)

SET (_forestdb_dep)
IF (DEFINED FORESTDB_TARGET)
  SET (_forestdb_dep DEPENDS ${FORESTDB_TARGET})
ENDIF (DEFINED FORESTDB_TARGET)

get_filename_component(JEMALLOC_LIB_DIR ${JEMALLOC_LIBRARIES} DIRECTORY)

IF (APPLE)
  # On OS X 10.11 (El Capitan) upwards we can no longer use DYLD_LIBRARY_PATH to locate
  # runtime dependancies. Instead add the location of forestdb etc to the actual
  # binary itself.
  SET (_ldflags "-extldflags '-Wl,-rpath,@executable_path/../lib'")
ENDIF ()

SET(CGO_INCLUDE_DIRS "${FORESTDB_INCLUDE_DIR};${Platform_SOURCE_DIR}/include")
SET(CGO_LIBRARY_DIRS "${FORESTDB_LIBRARY_DIR};${Platform_BINARY_DIR}")

IF(JEMALLOC_FOUND)
    SET (ENV{CGO_CFLAGS} "$ENV{CGO_CFLAGS} -DJEMALLOC=1")
    SET (CGO_INCLUDE_DIRS "${CGO_INCLUDE_DIRS};${JEMALLOC_INCLUDE_DIR}")
    SET (CGO_LIBRARY_DIRS "${CGO_LIBRARY_DIRS};${JEMALLOC_LIB_DIR}")
ENDIF(JEMALLOC_FOUND)

GoInstall (TARGET indexer PACKAGE github.com/couchbase/indexing/secondary/cmd/indexer
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  ${_forestdb_dep}
  CGO_INCLUDE_DIRS "${CGO_INCLUDE_DIRS}"
  CGO_LIBRARY_DIRS "${CGO_LIBRARY_DIRS}"
  LDFLAGS "${_ldflags}"
  INSTALL_PATH bin OUTPUT indexer)

GoInstall (TARGET projector PACKAGE github.com/couchbase/indexing/secondary/cmd/projector
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
 INSTALL_PATH bin OUTPUT projector)

GoInstall (TARGET cbindex PACKAGE github.com/couchbase/indexing/secondary/cmd/cbindex
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
 INSTALL_PATH bin OUTPUT cbindex)

GoInstall (TARGET cbindexperf PACKAGE github.com/couchbase/indexing/secondary/cmd/cbindexperf
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
 INSTALL_PATH bin OUTPUT cbindexperf)

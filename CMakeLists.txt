CMAKE_MINIMUM_REQUIRED (VERSION 2.8)
PROJECT (indexing)

INCLUDE (FindCouchbaseGo)
INCLUDE (FindCouchbaseJemalloc)
INCLUDE (FindCouchbaseCurl)

IF (BUILD_ENTERPRISE)
  MESSAGE(STATUS "Building EE edition of GSI")
  SET (TAGS "jemalloc")
ELSE (BUILD_ENTERPRISE)
  MESSAGE(STATUS "Building CE edition of GSI")
  SET (TAGS "jemalloc community")
ENDIF (BUILD_ENTERPRISE)

SET (LDFLAGS)
IF (APPLE)
  # On macOS, we must use -rpath to locate libraries
  # TODO: Remove debug info stripping (-s) added to allow 1.7.6 to work on macOS
  SET (LDFLAGS "-s -extldflags '-Wl,-rpath,@executable_path/../lib'")
ENDIF ()

IF (WIN32)
   if("${MSVC_VERSION}" LESS 1910)
      SET (LDFLAGS "${LDFLAGS} -linkmode internal")
   endif()
ENDIF ()

GET_FILENAME_COMPONENT (CURL_LIBRARY_DIR "${CURL_LIBRARIES}" DIRECTORY)
GET_FILENAME_COMPONENT (JEMALLOC_LIB_DIR ${JEMALLOC_LIBRARIES} DIRECTORY)

SET(CGO_INCLUDE_DIRS "${FORESTDB_INCLUDE_DIR};${sigar_SOURCE_DIR}/include;${Platform_SOURCE_DIR}/include;${CURL_INCLUDE_DIR}")
SET(CGO_LIBRARY_DIRS "${FORESTDB_LIBRARY_DIR};${sigar_BINARY_DIR}/src;${Platform_BINARY_DIR};${CURL_LIBRARY_DIR}")

IF(NOT JEMALLOC_FOUND)
  MESSAGE (FATAL_ERROR "GSI requires jemalloc normally, but it was not found")
ENDIF(NOT JEMALLOC_FOUND)

SET (ENV{CGO_CFLAGS} "$ENV{CGO_CFLAGS} -DJEMALLOC=1")
SET (CGO_INCLUDE_DIRS "${CGO_INCLUDE_DIRS};${JEMALLOC_INCLUDE_DIR}")
SET (CGO_LIBRARY_DIRS "${CGO_LIBRARY_DIRS};${JEMALLOC_LIB_DIR}")

GoInstall (TARGET indexer PACKAGE github.com/couchbase/indexing/secondary/cmd/indexer
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  DEPENDS forestdb
  CGO_INCLUDE_DIRS "${CGO_INCLUDE_DIRS}"
  CGO_LIBRARY_DIRS "${CGO_LIBRARY_DIRS}"
  GOTAGS "${TAGS}"
  LDFLAGS "${LDFLAGS}"
  INSTALL_PATH bin OUTPUT indexer
  GOVERSION 1.11.6)

ADD_DEPENDENCIES(indexer sigar forestdb platform)

GoInstall (TARGET projector PACKAGE github.com/couchbase/indexing/secondary/cmd/projector
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  CGO_INCLUDE_DIRS "${CGO_INCLUDE_DIRS}"
  CGO_LIBRARY_DIRS "${CGO_LIBRARY_DIRS}"
  GOTAGS "${TAGS}"
  LDFLAGS "${LDFLAGS}"
  INSTALL_PATH bin
  GOVERSION 1.11.6)

GoInstall (TARGET cbindex PACKAGE github.com/couchbase/indexing/secondary/cmd/cbindex
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  GOTAGS "${TAGS}"
  CGO_INCLUDE_DIRS "${CGO_INCLUDE_DIRS}"
  CGO_LIBRARY_DIRS "${CGO_LIBRARY_DIRS}"
  LDFLAGS "${LDFLAGS}"
  INSTALL_PATH bin
  GOVERSION 1.11.4)

GoInstall (TARGET cbindexperf PACKAGE github.com/couchbase/indexing/secondary/cmd/cbindexperf
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  CGO_INCLUDE_DIRS "${CGO_INCLUDE_DIRS}"
  CGO_LIBRARY_DIRS "${CGO_LIBRARY_DIRS}"
  GOTAGS "${TAGS}"
  LDFLAGS "${LDFLAGS}"
  INSTALL_PATH bin
  GOVERSION 1.11.4)

GoInstall (TARGET cbindexplan PACKAGE github.com/couchbase/indexing/secondary/cmd/cbindexplan
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  DEPENDS forestdb
  CGO_INCLUDE_DIRS "${CGO_INCLUDE_DIRS}"
  CGO_LIBRARY_DIRS "${CGO_LIBRARY_DIRS}"
  GOTAGS "${TAGS}"
  LDFLAGS "${LDFLAGS}"
  INSTALL_PATH bin OUTPUT cbindexplan
  GOVERSION 1.11.4)



file(
  GLOB_RECURSE
  TPCH_SRCS
  CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/dbgen/*.cpp 
)

add_library(tpch
  STATIC
    tpch_dsdgen.cpp
    tpch_wrapper.cpp
    ${TPCH_SRCS}
)

target_include_directories(tpch
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/dbgen
    ${PostgreSQL_SERVER_INCLUDE_DIRS}
)

target_compile_options(tpch
  PUBLIC
    "-fPIC"
    "-Wno-write-strings"
    "-Wno-format-truncation"
)


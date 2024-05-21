add_subdirectory(tpch-dbgen)

add_library(tpch STATIC
tpch_dsdgen.cpp tpch_wrapper.cpp)

target_link_libraries(tpch PUBLIC tpch_dbgen)

target_include_directories(tpch PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/tpch-dbgen/tpch-dbgen
  ${PostgreSQL_SERVER_INCLUDE_DIRS}
)

target_compile_options(tpch PUBLIC
  "-fPIC")


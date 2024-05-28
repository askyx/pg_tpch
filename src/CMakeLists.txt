
add_subdirectory(tpch)

add_postgresql_extension(
  pg_tpch
  VERSION 1.0
  SOURCES pg_tpch.cpp
  SCRIPTS pg_tpch--1.0.sql)

target_link_libraries(pg_tpch tpch)

cmake_host_system_information(RESULT core_num QUERY NUMBER_OF_LOGICAL_CORES)

file(APPEND
  ${CMAKE_CURRENT_SOURCE_DIR}/pre_prepare.sql
  "update tpch.tpch_host_info set host_core = ${core_num};\n")

set(TPCH_CURR_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tpch)
set(TPCH_INSTALL_DIR ${PostgreSQL_EXTENSION_DIR}/tpch)

install(
  DIRECTORY
    ${TPCH_CURR_DIR}/schema
  DESTINATION
    ${TPCH_INSTALL_DIR}
)

install(
  DIRECTORY
    ${TPCH_CURR_DIR}/queries
  DESTINATION
    ${TPCH_INSTALL_DIR}
)

install(
  FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/pre_prepare.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/post_prepare.sql
  DESTINATION
    ${TPCH_INSTALL_DIR}
)

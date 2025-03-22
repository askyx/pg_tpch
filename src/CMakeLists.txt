
add_subdirectory(tpch)

add_postgresql_extension(
  pg_tpch
  VERSION 1.0
  SOURCES pg_tpch.cpp
  SCRIPTS pg_tpch--1.0.sql
  REGRESS tpch0.1 tpch0.5)

target_link_libraries(pg_tpch tpch)

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

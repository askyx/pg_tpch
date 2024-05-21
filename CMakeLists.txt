
cmake_minimum_required(VERSION 3.20.0)
project(pg_tpcds VERSION
  1.0.0
  LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 23)

set(CMAKE_EXPORT_COMPILE_COMMANDS on)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

find_package(PostgreSQL REQUIRED)

add_subdirectory(src)

# clang-format
add_custom_target(format
  COMMAND git diff --name-only --diff-filter=d --cached '*.cpp' '*.h' | xargs clang-format -i
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
)
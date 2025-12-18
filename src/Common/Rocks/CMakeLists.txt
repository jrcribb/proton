include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(rocks .)

add_library(rocks ${rocks_headers} ${rocks_sources})

target_include_directories(rocks SYSTEM PUBLIC
        "${ROCKSDB_INCLUDE_DIR}")

target_link_libraries(rocks PUBLIC ch_contrib::rocksdb clickhouse_common_io)

if (ENABLE_TESTS)
    add_subdirectory(tests)
endif ()

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

proton_add_executable(rocksdb_ttl_compaction rocksdb_ttl_compaction.cpp)
target_link_libraries(rocksdb_ttl_compaction PRIVATE ch_contrib::rocksdb clickhouse_common_io)

target_include_directories(rocksdb_ttl_compaction
        SYSTEM PRIVATE
        "${ROCKSDB_INCLUDE_DIR}")

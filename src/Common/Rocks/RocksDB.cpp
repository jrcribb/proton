#include <base/scope_guard.h>
#include <Common/Rocks/RocksDB.h>
/// #include <Common/Rocks/RocksLogger.h>
#include <Common/logger_useful.h>

#include <rocksdb/utilities/db_ttl.h>

#include <filesystem>

namespace DB
{

RocksDB::RocksDB(rocksdb::DB * db_, const std::vector<rocksdb::ColumnFamilyHandle *> & cf_handles_, bool cleanup_, LoggerPtr logger_)
    : db(db_), cleanup(cleanup_), logger(logger_)
{
    chassert(db && logger);
    for (auto * cf_handle : cf_handles_)
        cf_handles.emplace(cf_handle->GetName(), cf_handle);
}

RocksDB::~RocksDB()
{
    try
    {
        shutdown(cleanup);
    }
    catch (...)
    {
        tryLogCurrentException(logger);
    }
}

RocksDBPtr
RocksDB::createOrLoadIfExists(const rocksdb::Options & options, const std::string & path, Int32 ttl, bool cleanup_, LoggerPtr logger)
{
    if (!logger)
        logger = getLogger("Rocks");

    {
        LOG_INFO(
            logger,
            "Init rocks with ttl={} path={} cleanup={} max_background_jobs={} max_write_buffer_number={} enable_blob_files={}",
            ttl,
            path,
            cleanup_,
            options.max_background_jobs,
            options.max_write_buffer_number,
            options.enable_blob_files);
        /// RocksLogger rlogger{rocksdb::InfoLogLevel::INFO_LEVEL, logger};
        /// options.Dump(&rlogger);
    }

    if (std::filesystem::exists(path))
    {
        /// Open existing rocksdb with column families
        std::vector<std::string> column_families;
        auto status = rocksdb::DB::ListColumnFamilies(options, path, &column_families);
        if (!status.ok())
            throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to list column families: {}", status.ToString());

        std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
        cf_descriptors.reserve(column_families.size());
        for (auto & cf : column_families)
            cf_descriptors.emplace_back(std::move(cf), rocksdb::ColumnFamilyOptions(options));

        std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
        cf_handles.reserve(cf_descriptors.size());
        rocksdb::DB * db = nullptr;

        if (ttl > 0)
        {
            rocksdb::DBWithTTL * ttl_db = nullptr;

            std::vector<Int32> ttls(cf_descriptors.size(), ttl);

            status = rocksdb::DBWithTTL::Open(options, path, cf_descriptors, &cf_handles, &ttl_db, ttls);
            db = ttl_db;
        }
        else
        {
            status = rocksdb::DB::Open(options, path, cf_descriptors, &cf_handles, &db);
        }

        if (!status.ok())
            throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to open rocksdb: {}", status.ToString());

        return std::make_shared<RocksDB>(db, cf_handles, cleanup_, logger);
    }
    else
    {
        rocksdb::DB * db = nullptr;
        rocksdb::Status status;

        if (ttl > 0)
        {
            rocksdb::DBWithTTL * ttl_db = nullptr;
            status = rocksdb::DBWithTTL::Open(options, path, &ttl_db, ttl);
            db = ttl_db;
        }
        else
        {
            status = rocksdb::DB::Open(options, path, &db);
        }

        if (!status.ok())
            throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to open rocksdb: {}", status.ToString());

        return std::make_shared<RocksDB>(db, std::vector<rocksdb::ColumnFamilyHandle *>{}, cleanup_, logger);
    }
}

void RocksDB::shutdown(bool cleanup_)
{
    if (shutdown_flag.test_and_set())
        return;

    for (auto & [_, cf_handle] : cf_handles)
    {
        auto status = db->DestroyColumnFamilyHandle(cf_handle);
        if (!status.ok())
            LOG_ERROR(logger, "Failed to destroy column family handle: {}", status.ToString());
    }
    cf_handles.clear();

    auto status = db->Close();
    if (!status.ok())
        throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to close rocksdb: {}", status.ToString());

    if (cleanup_)
    {
        const auto & path = db->GetName();
        std::filesystem::remove_all(path);
        LOG_INFO(logger, "Cleanup on disk data '{}'", path);
    }

    db.reset();
}

RocksDBColumnFamilyHandlerPtr
RocksDB::getOrCreateColumnFamilyHandler(const std::string & cf_handle_id, std::optional<rocksdb::ColumnFamilyOptions> cf_options)
{
    if (isShutdown())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "RocksDB is already shutdown");

    if (cf_handle_id.empty())
        return std::make_shared<RocksDBColumnFamilyHandler>(shared_from_this());

    std::lock_guard lock(handles_mutex);
    auto it = cf_handles.find(cf_handle_id);
    if (it != cf_handles.end())
        return std::make_shared<RocksDBColumnFamilyHandler>(shared_from_this(), it->second);

    rocksdb::ColumnFamilyHandle * cf_handle = nullptr;
    auto status
        = db->CreateColumnFamily(cf_options ? *cf_options : rocksdb::ColumnFamilyOptions(db->GetOptions()), cf_handle_id, &cf_handle);
    if (!status.ok())
        throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to get create column family: {}", status.ToString());

    cf_handles.emplace(cf_handle_id, cf_handle);
    return std::make_shared<RocksDBColumnFamilyHandler>(shared_from_this(), cf_handle);
}

void RocksDB::destroy(const std::string & handle_id)
{
    if (isShutdown()) [[unlikely]]
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "RocksDB is already shutdown");

    if (handle_id.empty())
        return;

    rocksdb::ColumnFamilyHandle * cf_handle = nullptr;
    {
        std::lock_guard lock(handles_mutex);
        auto it = cf_handles.find(handle_id);
        if (it == cf_handles.end())
            return;

        cf_handle = it->second;
        cf_handles.erase(it);
    }

    auto status = db->DropColumnFamily(cf_handle);
    if (!status.ok())
        throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to drop rocks column family: {}", status.ToString());

    LOG_INFO(logger, "Dropped rocks column family '{}'", cf_handle->GetName());

    status = db->DestroyColumnFamilyHandle(cf_handle);
    if (!status.ok())
        throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to destroy column family handle: {}", status.ToString());
}

RocksDBColumnFamilyHandler::RocksDBColumnFamilyHandler(RocksDBPtr rocks_, rocksdb::ColumnFamilyHandle * cf_handle_)
    : db(rocks_->db.get()), cf_handle(cf_handle_), rocks(rocks_)
{
    chassert(db);
    if (!cf_handle)
        cf_handle = db->DefaultColumnFamily();

    write_options.disableWAL = rocks_->cleanup;
}

void RocksDBColumnFamilyHandler::destroy()
{
    SCOPE_EXIT({
        db = nullptr;
        cf_handle = nullptr;
        rocks.reset();
    });

    /// Does not destroy default column family
    if (cf_handle == db->DefaultColumnFamily())
        return;

    auto rocks_locked = getRocksDB();
    if (rocks_locked)
        rocks_locked->destroy(cf_handle->GetName());
}

}

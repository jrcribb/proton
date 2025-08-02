#pragma once

#include <unordered_map>

namespace cluster
{
struct LogTimestampAndCount
{
    int64_t ts = 0; /// Last logged timestamp
    uint64_t count = 0;
};

using LogTrackContainer = std::unordered_map<uint64_t, LogTimestampAndCount>;

/// \return {true, appearance_count} if the log shall be logged, otherwise {false, appearance_count}
std::pair<bool, uint64_t> shouldLog(LogTrackContainer & tracked_logs, uint64_t log_key, int64_t throttling_sec);

}

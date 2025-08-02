#include <Cluster/Common/LogTrack.h>
#include <base/ClockUtils.h>

namespace cluster
{
std::pair<bool, uint64_t> shouldLog(LogTrackContainer & tracked_logs, uint64_t log_key, int64_t throttling_sec)
{
    if (auto iter = tracked_logs.find(log_key); iter != tracked_logs.end())
    {
        auto now_sec = DB::MonotonicSeconds::now();
        if (now_sec > iter->second.ts + throttling_sec)
        {
            /// Clear the count and reset the last logged timestamp
            auto count = iter->second.count;
            iter->second.ts = now_sec;
            iter->second.count = 1;

            return {true, count};
        }
        else
        {
            ++iter->second.count;
            return {false, iter->second.count};
        }
    }
    else
    {
        tracked_logs.emplace(log_key, LogTimestampAndCount{.ts = DB::MonotonicSeconds::now(), .count = 1});
        return {true, 1};
    }
}

}

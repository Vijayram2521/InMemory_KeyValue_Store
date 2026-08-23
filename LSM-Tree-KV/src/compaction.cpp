#include "engine/compaction.h"

namespace kv_engine {

std::map<std::string, std::string> merge_records(const std::vector<Record>& older,
                                                   const std::vector<Record>& newer) {
    std::map<std::string, std::string> result;
    size_t i = 0, j = 0;

    while (i < older.size() || j < newer.size()) {
        bool older_has = i < older.size();
        bool newer_has = j < newer.size();
        bool same_key = older_has && newer_has && older[i].key == newer[j].key;
        bool pick_newer = same_key || (newer_has && (!older_has || newer[j].key < older[i].key));

        const Record& winner = pick_newer ? newer[j] : older[i];
        if (!winner.is_tombstone) {
            result.emplace(winner.key, winner.value);
        }

        if (same_key) { ++i; ++j; }
        else if (pick_newer) { ++j; }
        else { ++i; }
    }
    return result;
}

} // namespace kv_engine

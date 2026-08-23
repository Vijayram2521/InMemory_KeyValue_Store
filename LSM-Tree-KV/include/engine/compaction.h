#pragma once
#include <map>
#include <string>
#include <vector>
#include "engine/sstable.h"

namespace kv_engine {

// Merges two already-sorted (by key) record sequences from adjacent SSTable
// generations -- `older` strictly precedes `newer` in flush/generation
// order -- into the map of PUT entries that should survive into a single
// merged generation.
//
// Rule (uniform, no special-casing): for each key present in either input,
// take the version from `newer` if it has one, else the version from
// `older`. If the winning record is a tombstone, drop it entirely from the
// output (write nothing for that key); otherwise keep the surviving PUT.
//
// This is only safe to call on the two GLOBALLY OLDEST currently-live
// generations -- dropping a tombstone is only correct when nothing even
// older survives for it to still be shadowing. Callers (StorageEngine's
// compactor) must enforce that invariant; this function has no way to
// verify it itself, since it only sees the two record sets being merged.
std::map<std::string, std::string> merge_records(const std::vector<Record>& older,
                                                   const std::vector<Record>& newer);

} // namespace kv_engine

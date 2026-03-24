#include "../include/engine/manifest.h"
#include <filesystem>
#include <fstream>

namespace kv_engine {

uint64_t Manifest::get_last_seq(const std::string& dir) {
    std::filesystem::path p = std::filesystem::path(dir) / "MANIFEST";
    if (!std::filesystem::exists(p)) {
        return 1; // Start at 1 if brand new
    }

    std::ifstream ifs(p);
    uint64_t seq;
    if (ifs >> seq) {
        return seq;
    }
    return 1;
}

void Manifest::update_seq(const std::string& dir, uint64_t seq) {
    std::filesystem::path p = std::filesystem::path(dir) / "MANIFEST";
    // We write to a temp file then rename for atomicity
    std::filesystem::path temp_p = p;
    temp_p.replace_extension(".tmp");

    std::ofstream ofs(temp_p);
    ofs << seq;
    ofs.close();

    std::filesystem::rename(temp_p, p);
}

}
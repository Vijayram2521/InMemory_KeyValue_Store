#include "../include/engine/manifest.h"
#include <filesystem>
#include <fstream>

namespace kv_engine {

void Manifest::add_entry(const std::string& dir, const std::string& filename, uint64_t seq) {
    std::string path = (std::filesystem::path(dir) / "MANIFEST").string();
    // Open in append mode
    std::ofstream f(path, std::ios::app);
    if (f.is_open()) {
        f << seq << "," << filename << "\n";
    }
}

std::vector<ManifestEntry> Manifest::load_history(const std::string& dir) {
    std::vector<ManifestEntry> history;
    std::string path = (std::filesystem::path(dir) / "MANIFEST").string();
    std::ifstream f(path);
    
    std::string line;
    while (std::getline(f, line)) {
        size_t comma = line.find(',');
        if (comma != std::string::npos) {
            uint64_t seq = std::stoull(line.substr(0, comma));
            std::string filename = line.substr(comma + 1);
            history.push_back({filename, seq});
        }
    }
    return history;
}

uint64_t Manifest::get_last_seq(const std::string& dir) {
    auto history = load_history(dir);
    if (history.empty()) return 0;
    return history.back().sequence; // The last line is the latest seq
}

}
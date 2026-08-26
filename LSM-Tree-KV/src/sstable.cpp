#include "../include/engine/sstable.h"
#include <algorithm>
#include <cstring>
#include <fstream>
#include <cstdint>
#if defined(__unix__) || defined(__APPLE__)
#define KV_HAVE_MMAP 1
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace kv_engine {

// MappedFile is a no-op (always invalid, mmap_reads silently falls back to
// the ifstream path via Get()'s defensive check) on non-POSIX platforms --
// this is an experimental, Unix-only optimization, and gating it this way
// keeps kv_engine/kv_tests/kv_benchmark buildable on Windows for local
// iteration, same as every other target except the POSIX-socket-only
// cluster layer.
#ifdef KV_HAVE_MMAP
MappedFile::MappedFile(const std::string& filename) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) return;
    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size <= 0) {
        close(fd);
        return;
    }
    void* p = mmap(nullptr, static_cast<size_t>(st.st_size), PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd); // the mapping itself keeps the file's contents accessible; the fd isn't needed after mmap() returns
    if (p == MAP_FAILED) return;
    data_ = p;
    size_ = static_cast<size_t>(st.st_size);
}

void MappedFile::reset() {
    if (data_) {
        munmap(data_, size_);
        data_ = nullptr;
        size_ = 0;
    }
}
#else
MappedFile::MappedFile(const std::string&) {}
void MappedFile::reset() {}
#endif

MappedFile::~MappedFile() { reset(); }

MappedFile::MappedFile(MappedFile&& other) noexcept : data_(other.data_), size_(other.size_) {
    other.data_ = nullptr;
    other.size_ = 0;
}

MappedFile& MappedFile::operator=(MappedFile&& other) noexcept {
    if (this != &other) {
        reset();
        data_ = other.data_;
        size_ = other.size_;
        other.data_ = nullptr;
        other.size_ = 0;
    }
    return *this;
}

namespace {
    // Trailer written at the very end of every SSTable file, at a fixed
    // offset from EOF, so a reader can seek(file_size - kFooterSize) without
    // needing to know anything else about the file up front.
    //   [0..8)   index block offset (from start of file)
    //   [8..16)  number of index entries
    //   [16..20) magic number, so a lookup fails fast on a non-SSTable file
    //            instead of interpreting garbage as an offset/count.
    constexpr uint64_t kSSTableMagic = 0x53535442; // "SSTB" as a little-endian u32
    constexpr size_t kFooterSize = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t);

    void write_put_record(std::ofstream& ofs, const std::string& key, const std::string& value) {
        char type = 1;
        uint32_t kLen = static_cast<uint32_t>(key.size());
        uint32_t vLen = static_cast<uint32_t>(value.size());
        ofs.write(&type, 1);
        ofs.write(reinterpret_cast<const char*>(&kLen), sizeof(kLen));
        ofs.write(key.data(), kLen);
        ofs.write(reinterpret_cast<const char*>(&vLen), sizeof(vLen));
        ofs.write(value.data(), vLen);
    }

    uint64_t put_record_size(const std::string& key, const std::string& value) {
        return 1 + sizeof(uint32_t) + key.size() + sizeof(uint32_t) + value.size();
    }

    void write_delete_record(std::ofstream& ofs, const std::string& key) {
        char type = 2;
        uint32_t kLen = static_cast<uint32_t>(key.size());
        ofs.write(&type, 1);
        ofs.write(reinterpret_cast<const char*>(&kLen), sizeof(kLen));
        ofs.write(key.data(), kLen);
    }

    uint64_t delete_record_size(const std::string& key) {
        return 1 + sizeof(uint32_t) + key.size();
    }

    // Forward-only cursor over one SSTable's data block, decoding and
    // holding exactly one record at a time -- used by merge_files() so
    // compaction's peak memory is O(1) per input file instead of O(file
    // size). Materializing whole files (the original approach) is fine on
    // an unconstrained dev machine but genuinely OOM-kills a
    // memory-capped deployment (observed directly: 1.2GB-limited compute
    // node containers killed by the kernel cgroup OOM killer while holding
    // two full files' records plus a merged copy simultaneously).
    class RecordCursor {
    public:
        explicit RecordCursor(const std::string& filename) : ifs_(filename, std::ios::binary) {
            if (!ifs_.is_open()) { valid_ = false; return; }
            ifs_.seekg(0, std::ios::end);
            std::streamoff file_size = ifs_.tellg();
            if (file_size < static_cast<std::streamoff>(kFooterSize)) { valid_ = false; return; }
            ifs_.seekg(file_size - static_cast<std::streamoff>(kFooterSize));
            uint64_t index_offset = 0, index_count = 0;
            uint32_t magic = 0;
            if (!ifs_.read(reinterpret_cast<char*>(&index_offset), sizeof(index_offset))) { valid_ = false; return; }
            if (!ifs_.read(reinterpret_cast<char*>(&index_count), sizeof(index_count))) { valid_ = false; return; }
            if (!ifs_.read(reinterpret_cast<char*>(&magic), sizeof(magic))) { valid_ = false; return; }
            if (magic != static_cast<uint32_t>(kSSTableMagic)) { valid_ = false; return; }
            data_end_ = index_offset;
            ifs_.seekg(0, std::ios::beg);
            advance(); // load the first record into current_, if any
        }

        bool has_current() const { return has_current_; }
        const Record& current() const { return current_; }

        void advance() {
            has_current_ = false;
            if (!valid_ || static_cast<uint64_t>(ifs_.tellg()) >= data_end_) return;
            char type_raw;
            if (!ifs_.read(&type_raw, 1)) return;
            uint8_t type = static_cast<uint8_t>(type_raw);
            uint32_t kLen = 0;
            if (!ifs_.read(reinterpret_cast<char*>(&kLen), sizeof(kLen))) return;
            std::string key(kLen, '\0');
            if (kLen > 0 && !ifs_.read(&key[0], kLen)) return;
            if (type == 2) { // DELETE / tombstone -- no value bytes follow
                current_ = {std::move(key), "", true};
                has_current_ = true;
                return;
            }
            uint32_t vLen = 0;
            if (!ifs_.read(reinterpret_cast<char*>(&vLen), sizeof(vLen))) return;
            std::string value(vLen, '\0');
            if (vLen > 0 && !ifs_.read(&value[0], vLen)) return;
            current_ = {std::move(key), std::move(value), false};
            has_current_ = true;
        }

    private:
        std::ifstream ifs_;
        bool valid_ = true;
        uint64_t data_end_ = 0;
        bool has_current_ = false;
        Record current_;
    };
} // namespace

bool SSTable::write_file(const std::string& filename,
                          const std::map<std::string, std::string>& data,
                          const std::set<std::string>& tombstones,
                          std::vector<IndexEntry>& out_index) {
    std::ofstream ofs(filename, std::ios::binary);
    if (!ofs.is_open()) return false;

    out_index.clear();
    out_index.reserve(data.size() + tombstones.size());

    // Merge the two already-sorted containers into one ascending-key
    // sequence. A key can never appear in both: Put() clears any tombstone
    // for the key it just wrote, and Delete() clears any memtable entry for
    // the key it just marked deleted, so the two sets are always disjoint
    // for a single flush generation.
    auto it_data = data.begin();
    auto it_tomb = tombstones.begin();
    uint64_t offset = 0;

    while (it_data != data.end() || it_tomb != tombstones.end()) {
        bool take_data;
        if (it_tomb == tombstones.end()) take_data = true;
        else if (it_data == data.end()) take_data = false;
        else take_data = it_data->first < *it_tomb;

        if (take_data) {
            out_index.push_back({it_data->first, offset});
            write_put_record(ofs, it_data->first, it_data->second);
            offset += put_record_size(it_data->first, it_data->second);
            ++it_data;
        } else {
            out_index.push_back({*it_tomb, offset});
            write_delete_record(ofs, *it_tomb);
            offset += delete_record_size(*it_tomb);
            ++it_tomb;
        }
    }

    // Index block: one (keyLen, key, offset) triple per data record, same
    // ascending-key order as the data block above.
    uint64_t index_offset = offset;
    for (const auto& entry : out_index) {
        uint32_t kLen = static_cast<uint32_t>(entry.key.size());
        ofs.write(reinterpret_cast<const char*>(&kLen), sizeof(kLen));
        ofs.write(entry.key.data(), kLen);
        ofs.write(reinterpret_cast<const char*>(&entry.offset), sizeof(entry.offset));
    }

    // Footer.
    uint64_t index_count = out_index.size();
    uint32_t magic = static_cast<uint32_t>(kSSTableMagic);
    ofs.write(reinterpret_cast<const char*>(&index_offset), sizeof(index_offset));
    ofs.write(reinterpret_cast<const char*>(&index_count), sizeof(index_count));
    ofs.write(reinterpret_cast<const char*>(&magic), sizeof(magic));

    ofs.close();
    return true;
}

std::vector<IndexEntry> SSTable::load_index(const std::string& filename) {
    std::vector<IndexEntry> index;
    std::ifstream ifs(filename, std::ios::binary);
    if (!ifs.is_open()) return index;

    ifs.seekg(0, std::ios::end);
    std::streamoff file_size = ifs.tellg();
    if (file_size < static_cast<std::streamoff>(kFooterSize)) return index;

    ifs.seekg(file_size - static_cast<std::streamoff>(kFooterSize));
    uint64_t index_offset = 0, index_count = 0;
    uint32_t magic = 0;
    if (!ifs.read(reinterpret_cast<char*>(&index_offset), sizeof(index_offset))) return index;
    if (!ifs.read(reinterpret_cast<char*>(&index_count), sizeof(index_count))) return index;
    if (!ifs.read(reinterpret_cast<char*>(&magic), sizeof(magic))) return index;
    if (magic != static_cast<uint32_t>(kSSTableMagic)) return index;

    ifs.seekg(static_cast<std::streamoff>(index_offset));
    index.reserve(index_count);
    for (uint64_t i = 0; i < index_count; ++i) {
        uint32_t kLen = 0;
        if (!ifs.read(reinterpret_cast<char*>(&kLen), sizeof(kLen))) break;
        std::string key(kLen, '\0');
        if (kLen > 0 && !ifs.read(&key[0], kLen)) break;
        uint64_t rec_offset = 0;
        if (!ifs.read(reinterpret_cast<char*>(&rec_offset), sizeof(rec_offset))) break;
        index.push_back({std::move(key), rec_offset});
    }
    return index;
}

bool SSTable::merge_files(const std::string& older_path, const std::string& newer_path,
                           const std::string& output_path, std::vector<IndexEntry>& out_index) {
    RecordCursor older(older_path);
    RecordCursor newer(newer_path);

    std::ofstream ofs(output_path, std::ios::binary);
    if (!ofs.is_open()) return false;

    out_index.clear();
    uint64_t offset = 0;

    // Same merge rule as a two-way sorted merge, applied to two STREAMS
    // instead of two in-memory containers: on equal keys, `newer` wins;
    // whichever is smaller advances alone otherwise. A winning tombstone is
    // dropped entirely (never written) -- correct only when nothing older
    // than `older_path` survives, which callers (StorageEngine's
    // compactor) must guarantee by always merging the two globally-oldest
    // generations.
    while (older.has_current() || newer.has_current()) {
        bool same_key = older.has_current() && newer.has_current() &&
                         older.current().key == newer.current().key;
        bool pick_newer = same_key ||
            (newer.has_current() && (!older.has_current() || newer.current().key < older.current().key));

        const Record& winner = pick_newer ? newer.current() : older.current();
        if (!winner.is_tombstone) {
            out_index.push_back({winner.key, offset});
            write_put_record(ofs, winner.key, winner.value);
            offset += put_record_size(winner.key, winner.value);
        }

        if (same_key) { older.advance(); newer.advance(); }
        else if (pick_newer) { newer.advance(); }
        else { older.advance(); }
    }

    uint64_t index_offset = offset;
    for (const auto& entry : out_index) {
        uint32_t kLen = static_cast<uint32_t>(entry.key.size());
        ofs.write(reinterpret_cast<const char*>(&kLen), sizeof(kLen));
        ofs.write(entry.key.data(), kLen);
        ofs.write(reinterpret_cast<const char*>(&entry.offset), sizeof(entry.offset));
    }

    uint64_t index_count = out_index.size();
    uint32_t magic = static_cast<uint32_t>(kSSTableMagic);
    ofs.write(reinterpret_cast<const char*>(&index_offset), sizeof(index_offset));
    ofs.write(reinterpret_cast<const char*>(&index_count), sizeof(index_count));
    ofs.write(reinterpret_cast<const char*>(&magic), sizeof(magic));

    ofs.close();
    return true;
}

SearchResult SSTable::search_with_index(const std::string& filename,
                                         const std::vector<IndexEntry>& index,
                                         const std::string& key) {
    auto it = std::lower_bound(index.begin(), index.end(), key,
        [](const IndexEntry& entry, const std::string& k) { return entry.key < k; });
    if (it == index.end() || it->key != key) {
        return {false, "", false};
    }

    std::ifstream ifs(filename, std::ios::binary);
    if (!ifs.is_open()) return {false, "", false};
    ifs.seekg(static_cast<std::streamoff>(it->offset));

    char type_raw;
    if (!ifs.read(&type_raw, 1)) return {false, "", false};
    uint8_t type = static_cast<uint8_t>(type_raw);

    uint32_t kLen = 0;
    if (!ifs.read(reinterpret_cast<char*>(&kLen), sizeof(kLen))) return {false, "", false};
    std::string found_key(kLen, '\0');
    if (kLen > 0 && !ifs.read(&found_key[0], kLen)) return {false, "", false};
    if (found_key != key) {
        // The index pointed somewhere that doesn't actually hold `key` --
        // treat as not-found rather than risk returning the wrong value.
        return {false, "", false};
    }

    if (type == 2) { // DELETE / tombstone
        return {true, "", true};
    }

    uint32_t vLen = 0;
    if (!ifs.read(reinterpret_cast<char*>(&vLen), sizeof(vLen))) return {false, "", false};
    std::string value(vLen, '\0');
    if (vLen > 0 && !ifs.read(&value[0], vLen)) return {false, "", false};
    return {true, value, false};
}

SearchResult SSTable::search_with_index_mmap(const MappedFile& mapped,
                                              const std::vector<IndexEntry>& index,
                                              const std::string& key) {
    if (!mapped.valid()) return {false, "", false};

    auto it = std::lower_bound(index.begin(), index.end(), key,
        [](const IndexEntry& entry, const std::string& k) { return entry.key < k; });
    if (it == index.end() || it->key != key) {
        return {false, "", false};
    }

    const char* base = mapped.data();
    const size_t sz = mapped.size();
    uint64_t off = it->offset;

    if (off + 1 > sz) return {false, "", false};
    uint8_t type = static_cast<uint8_t>(base[off]);
    off += 1;

    if (off + sizeof(uint32_t) > sz) return {false, "", false};
    uint32_t kLen;
    std::memcpy(&kLen, base + off, sizeof(kLen));
    off += sizeof(kLen);

    if (off + kLen > sz) return {false, "", false};
    if (std::string(base + off, kLen) != key) {
        // The index pointed somewhere that doesn't actually hold `key` --
        // treat as not-found rather than risk returning the wrong value.
        return {false, "", false};
    }
    off += kLen;

    if (type == 2) { // DELETE / tombstone
        return {true, "", true};
    }

    if (off + sizeof(uint32_t) > sz) return {false, "", false};
    uint32_t vLen;
    std::memcpy(&vLen, base + off, sizeof(vLen));
    off += sizeof(vLen);

    if (off + vLen > sz) return {false, "", false};
    return {true, std::string(base + off, vLen), false};
}

} // namespace kv_engine

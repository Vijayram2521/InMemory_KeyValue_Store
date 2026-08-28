#include "../include/engine/sstable.h"
#include <algorithm>
#include <cstring>
#include <fstream>
#include <cstdint>
#include <lz4.h>
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

    // Attempts LZ4 compression of `value` when `enabled`. Only reports a
    // compressed result if it's strictly smaller than the original -- a
    // record on disk never expands relative to storing the value raw, so
    // pathologically incompressible values (e.g. this project's own
    // uniformly-random benchmark values) just fall back to raw storage at
    // no extra cost beyond the one failed compression attempt.
    struct CompressedValue {
        bool compressed = false;
        std::string bytes; // what actually gets written to disk
    };

    CompressedValue compress_value(const std::string& value, bool enabled) {
        if (!enabled || value.empty()) return {false, value};
        int bound = LZ4_compressBound(static_cast<int>(value.size()));
        if (bound <= 0) return {false, value};
        std::string dst(static_cast<size_t>(bound), '\0');
        int written = LZ4_compress_default(value.data(), &dst[0],
                                            static_cast<int>(value.size()), bound);
        if (written <= 0 || static_cast<size_t>(written) >= value.size()) {
            return {false, value};
        }
        dst.resize(static_cast<size_t>(written));
        return {true, std::move(dst)};
    }

    // Reverses compress_value. `compressed=false` just returns the stored
    // bytes as-is (they already are the value). A decompression failure
    // (shouldn't happen against this engine's own writes) returns an empty
    // string rather than garbage -- fails safe, not silently wrong.
    std::string decompress_value(const char* data, uint32_t stored_len, bool compressed, uint32_t original_len) {
        if (!compressed) return std::string(data, stored_len);
        std::string out(original_len, '\0');
        if (original_len > 0) {
            int written = LZ4_decompress_safe(data, &out[0],
                                               static_cast<int>(stored_len), static_cast<int>(original_len));
            if (written < 0 || static_cast<uint32_t>(written) != original_len) return std::string();
        }
        return out;
    }

    // Every on-disk data record is a live PUT at write time -- type is kept
    // (rather than dropped) purely so the framing style matches the rest of
    // this codebase's length-prefixed records and leaves room for a future
    // record kind without another format change; it is always 1 today,
    // since deletes no longer produce their own on-disk record (see the
    // del-bitmap in sstable.h). Returns the number of bytes actually
    // written, so callers track data-block offsets from the real,
    // possibly-compressed size instead of a separately (and redundantly)
    // computed one.
    uint64_t write_put_record(std::ofstream& ofs, uint64_t serial, const std::string& key,
                               const std::string& value, bool compress) {
        char type = 1;
        uint32_t kLen = static_cast<uint32_t>(key.size());
        CompressedValue cv = compress_value(value, compress);
        char compFlag = cv.compressed ? 1 : 0;
        uint32_t storedLen = static_cast<uint32_t>(cv.bytes.size());
        uint32_t originalLen = static_cast<uint32_t>(value.size());

        ofs.write(&type, 1);
        ofs.write(reinterpret_cast<const char*>(&serial), sizeof(serial));
        ofs.write(reinterpret_cast<const char*>(&kLen), sizeof(kLen));
        ofs.write(key.data(), kLen);
        ofs.write(&compFlag, 1);
        ofs.write(reinterpret_cast<const char*>(&storedLen), sizeof(storedLen));
        ofs.write(reinterpret_cast<const char*>(&originalLen), sizeof(originalLen));
        ofs.write(cv.bytes.data(), storedLen);

        return 1 + sizeof(serial) + sizeof(kLen) + kLen + 1 + sizeof(storedLen) + sizeof(originalLen) + storedLen;
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
            if (!ifs_.read(&type_raw, 1)) return; // always 1 (PUT) -- see write_put_record's comment
            uint64_t serial = 0;
            if (!ifs_.read(reinterpret_cast<char*>(&serial), sizeof(serial))) return;
            uint32_t kLen = 0;
            if (!ifs_.read(reinterpret_cast<char*>(&kLen), sizeof(kLen))) return;
            std::string key(kLen, '\0');
            if (kLen > 0 && !ifs_.read(&key[0], kLen)) return;
            char compFlag = 0;
            if (!ifs_.read(&compFlag, 1)) return;
            uint32_t storedLen = 0;
            if (!ifs_.read(reinterpret_cast<char*>(&storedLen), sizeof(storedLen))) return;
            uint32_t originalLen = 0;
            if (!ifs_.read(reinterpret_cast<char*>(&originalLen), sizeof(originalLen))) return;
            std::string stored(storedLen, '\0');
            if (storedLen > 0 && !ifs_.read(&stored[0], storedLen)) return;
            std::string value = decompress_value(stored.data(), storedLen, compFlag != 0, originalLen);
            current_ = {std::move(key), std::move(value), serial};
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
                          std::vector<IndexEntry>& out_index,
                          bool compress) {
    std::ofstream ofs(filename, std::ios::binary);
    if (!ofs.is_open()) return false;

    out_index.clear();
    out_index.reserve(data.size());

    uint64_t offset = 0;
    uint64_t serial = 0;
    for (const auto& [key, value] : data) {
        out_index.push_back({key, offset, serial});
        offset += write_put_record(ofs, serial, key, value, compress);
        ++serial;
    }

    // Index block: one (keyLen, key, offset, serial) quad per data record,
    // same ascending-key order as the data block above.
    uint64_t index_offset = offset;
    for (const auto& entry : out_index) {
        uint32_t kLen = static_cast<uint32_t>(entry.key.size());
        ofs.write(reinterpret_cast<const char*>(&kLen), sizeof(kLen));
        ofs.write(entry.key.data(), kLen);
        ofs.write(reinterpret_cast<const char*>(&entry.offset), sizeof(entry.offset));
        ofs.write(reinterpret_cast<const char*>(&entry.serial), sizeof(entry.serial));
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
        uint64_t serial = 0;
        if (!ifs.read(reinterpret_cast<char*>(&serial), sizeof(serial))) break;
        index.push_back({std::move(key), rec_offset, serial});
    }
    return index;
}

bool SSTable::merge_files(const std::string& older_path, const std::vector<uint8_t>& older_del,
                           const std::string& newer_path, const std::vector<uint8_t>& newer_del,
                           const std::string& output_path,
                           std::vector<IndexEntry>& out_index, std::vector<uint8_t>& out_del,
                           bool compress) {
    RecordCursor older(older_path);
    RecordCursor newer(newer_path);

    // Skip past any record already marked dead in its own file -- it was
    // already proven superseded/deleted by the eager dead-marking
    // invariant (StorageEngine's Put/Delete flip this bit the moment a
    // record stops being live), so nothing behind it can still need it.
    auto skip_dead = [](RecordCursor& cur, const std::vector<uint8_t>& del) {
        while (cur.has_current() && is_dead(del, cur.current().serial)) cur.advance();
    };
    skip_dead(older, older_del);
    skip_dead(newer, newer_del);

    std::ofstream ofs(output_path, std::ios::binary);
    if (!ofs.is_open()) return false;

    out_index.clear();
    uint64_t offset = 0;
    uint64_t next_serial = 0;

    // A standard two-way merge of two already-sorted, already-live-only
    // streams. Keys should no longer collide between the two files in
    // practice (eager marking means an overwritten key's old copy is
    // already dead and was just skipped above), but the same-key branch is
    // kept as a defensive tie-breaker (newer wins) rather than assuming
    // that invariant always holds perfectly.
    while (older.has_current() || newer.has_current()) {
        bool same_key = older.has_current() && newer.has_current() &&
                         older.current().key == newer.current().key;
        bool pick_newer = same_key ||
            (newer.has_current() && (!older.has_current() || newer.current().key < older.current().key));

        const Record& winner = pick_newer ? newer.current() : older.current();
        out_index.push_back({winner.key, offset, next_serial});
        offset += write_put_record(ofs, next_serial, winner.key, winner.value, compress);
        ++next_serial;

        if (same_key) { older.advance(); newer.advance(); }
        else if (pick_newer) { newer.advance(); }
        else { older.advance(); }

        skip_dead(older, older_del);
        skip_dead(newer, newer_del);
    }

    uint64_t index_offset = offset;
    for (const auto& entry : out_index) {
        uint32_t kLen = static_cast<uint32_t>(entry.key.size());
        ofs.write(reinterpret_cast<const char*>(&kLen), sizeof(kLen));
        ofs.write(entry.key.data(), kLen);
        ofs.write(reinterpret_cast<const char*>(&entry.offset), sizeof(entry.offset));
        ofs.write(reinterpret_cast<const char*>(&entry.serial), sizeof(entry.serial));
    }

    uint64_t index_count = out_index.size();
    uint32_t magic = static_cast<uint32_t>(kSSTableMagic);
    ofs.write(reinterpret_cast<const char*>(&index_offset), sizeof(index_offset));
    ofs.write(reinterpret_cast<const char*>(&index_count), sizeof(index_count));
    ofs.write(reinterpret_cast<const char*>(&magic), sizeof(magic));

    ofs.close();
    out_del = make_del_bitmap(out_index.size());
    return true;
}

SearchResult SSTable::search_with_index(const std::string& filename,
                                         const std::vector<IndexEntry>& index,
                                         const std::string& key) {
    auto it = std::lower_bound(index.begin(), index.end(), key,
        [](const IndexEntry& entry, const std::string& k) { return entry.key < k; });
    if (it == index.end() || it->key != key) {
        return {false, "", 0};
    }

    std::ifstream ifs(filename, std::ios::binary);
    if (!ifs.is_open()) return {false, "", 0};
    ifs.seekg(static_cast<std::streamoff>(it->offset));

    char type_raw;
    if (!ifs.read(&type_raw, 1)) return {false, "", 0};

    uint64_t serial = 0;
    if (!ifs.read(reinterpret_cast<char*>(&serial), sizeof(serial))) return {false, "", 0};

    uint32_t kLen = 0;
    if (!ifs.read(reinterpret_cast<char*>(&kLen), sizeof(kLen))) return {false, "", 0};
    std::string found_key(kLen, '\0');
    if (kLen > 0 && !ifs.read(&found_key[0], kLen)) return {false, "", 0};
    if (found_key != key) {
        // The index pointed somewhere that doesn't actually hold `key` --
        // treat as not-found rather than risk returning the wrong value.
        return {false, "", 0};
    }

    char compFlag = 0;
    if (!ifs.read(&compFlag, 1)) return {false, "", 0};
    uint32_t storedLen = 0;
    if (!ifs.read(reinterpret_cast<char*>(&storedLen), sizeof(storedLen))) return {false, "", 0};
    uint32_t originalLen = 0;
    if (!ifs.read(reinterpret_cast<char*>(&originalLen), sizeof(originalLen))) return {false, "", 0};
    std::string stored(storedLen, '\0');
    if (storedLen > 0 && !ifs.read(&stored[0], storedLen)) return {false, "", 0};
    return {true, decompress_value(stored.data(), storedLen, compFlag != 0, originalLen), serial};
}

SearchResult SSTable::search_with_index_mmap(const MappedFile& mapped,
                                              const std::vector<IndexEntry>& index,
                                              const std::string& key) {
    if (!mapped.valid()) return {false, "", 0};

    auto it = std::lower_bound(index.begin(), index.end(), key,
        [](const IndexEntry& entry, const std::string& k) { return entry.key < k; });
    if (it == index.end() || it->key != key) {
        return {false, "", 0};
    }

    const char* base = mapped.data();
    const size_t sz = mapped.size();
    uint64_t off = it->offset;

    if (off + 1 > sz) return {false, "", 0};
    off += 1; // type byte, always 1 (PUT)

    if (off + sizeof(uint64_t) > sz) return {false, "", 0};
    uint64_t serial;
    std::memcpy(&serial, base + off, sizeof(serial));
    off += sizeof(serial);

    if (off + sizeof(uint32_t) > sz) return {false, "", 0};
    uint32_t kLen;
    std::memcpy(&kLen, base + off, sizeof(kLen));
    off += sizeof(kLen);

    if (off + kLen > sz) return {false, "", 0};
    if (std::string(base + off, kLen) != key) {
        // The index pointed somewhere that doesn't actually hold `key` --
        // treat as not-found rather than risk returning the wrong value.
        return {false, "", 0};
    }
    off += kLen;

    if (off + 1 > sz) return {false, "", 0};
    char compFlag = base[off];
    off += 1;

    if (off + sizeof(uint32_t) > sz) return {false, "", 0};
    uint32_t storedLen;
    std::memcpy(&storedLen, base + off, sizeof(storedLen));
    off += sizeof(storedLen);

    if (off + sizeof(uint32_t) > sz) return {false, "", 0};
    uint32_t originalLen;
    std::memcpy(&originalLen, base + off, sizeof(originalLen));
    off += sizeof(originalLen);

    if (off + storedLen > sz) return {false, "", 0};
    return {true, decompress_value(base + off, storedLen, compFlag != 0, originalLen), serial};
}

// --- Del-bitmap helpers ---

std::vector<uint8_t> SSTable::make_del_bitmap(size_t num_bits) {
    return std::vector<uint8_t>((num_bits + 7) / 8, 0);
}

std::vector<uint8_t> SSTable::load_del_bitmap(const std::string& path) {
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs.is_open()) return {};
    return std::vector<uint8_t>(std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>());
}

bool SSTable::save_del_bitmap(const std::string& path, const std::vector<uint8_t>& bits) {
    std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
    if (!ofs.is_open()) return false;
    if (!bits.empty()) ofs.write(reinterpret_cast<const char*>(bits.data()), static_cast<std::streamsize>(bits.size()));
    return true;
}

bool SSTable::is_dead(const std::vector<uint8_t>& bits, uint64_t serial) {
    size_t byte_idx = serial / 8;
    if (byte_idx >= bits.size()) return false; // out of range -- treat as live rather than guess
    uint8_t mask = static_cast<uint8_t>(1u << (serial % 8));
    return (bits[byte_idx] & mask) != 0;
}

void SSTable::mark_dead_in_memory(std::vector<uint8_t>& bits, uint64_t serial) {
    size_t byte_idx = serial / 8;
    if (byte_idx >= bits.size()) bits.resize(byte_idx + 1, 0);
    bits[byte_idx] |= static_cast<uint8_t>(1u << (serial % 8));
}

bool SSTable::flip_dead_on_disk(const std::string& path, uint64_t serial) {
    size_t byte_idx = serial / 8;
    std::fstream fs(path, std::ios::binary | std::ios::in | std::ios::out);
    if (!fs.is_open()) return false;

    fs.seekg(0, std::ios::end);
    std::streamoff file_size = fs.tellg();
    if (static_cast<std::streamoff>(byte_idx) >= file_size) {
        // Bitmap file is shorter than expected (shouldn't normally happen,
        // since it's always created sized to the flush threshold up front)
        // -- extend it with zero bytes up to and including byte_idx rather
        // than silently failing to record the mark.
        fs.seekp(0, std::ios::end);
        std::vector<char> pad(static_cast<size_t>(byte_idx + 1 - file_size), 0);
        fs.write(pad.data(), static_cast<std::streamsize>(pad.size()));
    }

    fs.seekg(static_cast<std::streamoff>(byte_idx));
    char byte = 0;
    fs.read(&byte, 1);
    uint8_t mask = static_cast<uint8_t>(1u << (serial % 8));
    byte = static_cast<char>(static_cast<uint8_t>(byte) | mask);

    fs.seekp(static_cast<std::streamoff>(byte_idx));
    fs.write(&byte, 1);
    fs.flush();
    return true;
}

std::string SSTable::del_path_for(const std::string& sst_path) {
    if (sst_path.size() >= 4 && sst_path.compare(sst_path.size() - 4, 4, ".sst") == 0) {
        return sst_path.substr(0, sst_path.size() - 4) + ".del";
    }
    return sst_path + ".del";
}

} // namespace kv_engine

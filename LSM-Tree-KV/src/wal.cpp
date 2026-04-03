#include "../include/engine/wal.h"
#include <iostream>
#include <map>
#include <set>

WAL::WAL(const std::string& filepath) : path(filepath) {
    // Open in append and binary mode
    log_file.open(path, std::ios::app | std::ios::binary);
    if (!log_file.is_open()) {
        throw std::runtime_error("Failed to open WAL file: " + path);
    }
}

WAL::~WAL() {
    if (log_file.is_open()) {
        log_file.close();
    }
}

bool WAL::append(LogOp op, uint64_t sequence,const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(write_mutex);

    char type = static_cast<char>(op);
    uint32_t kLen = static_cast<uint32_t>(key.size());
    uint32_t vLen = static_cast<uint32_t>(value.size());

    try {
        log_file.write(&type, sizeof(type));
        log_file.write(reinterpret_cast<const char*>(&sequence), sizeof(sequence));
        log_file.write(reinterpret_cast<const char*>(&kLen), sizeof(kLen));
        log_file.write(key.data(), kLen);
        
        if (op == LogOp::PUT) {
            log_file.write(reinterpret_cast<const char*>(&vLen), sizeof(vLen));
            log_file.write(value.data(), vLen);
        }

        log_file.flush(); // Ensure it hits the OS buffer immediately
        return true;
    } catch (...) {
        return false;
    }
}

// Update this in wal.h and wal.cpp
void WAL::recover(std::map<std::string, std::string>& memtable, std::set<std::string>& tombstones) {
    std::ifstream reader(path, std::ios::binary | std::ios::in);
    if (!reader || !reader.is_open()) return;

    while (reader.peek() != EOF) {
        char type_raw;
        uint64_t sequence;
        uint32_t kLen;

        if (!reader.read(&type_raw, sizeof(type_raw))) break;
        LogOp op = static_cast<LogOp>(type_raw);
        
        // Read the sequence we added earlier
        if (!reader.read(reinterpret_cast<char*>(&sequence), sizeof(sequence))) break;
        
        // Read Key
        if (!reader.read(reinterpret_cast<char*>(&kLen), sizeof(kLen))) break;
        std::string key(kLen, '\0');
        reader.read(&key[0], kLen);

        if (op == LogOp::PUT) {
            uint32_t vLen;
            if (!reader.read(reinterpret_cast<char*>(&vLen), sizeof(vLen))) break;
            std::string value(vLen, '\0');
            reader.read(&value[0], vLen);
            
            memtable[key] = value; 
            tombstones.erase(key); // If it was previously deleted, a new PUT revives it
        } else if (op == LogOp::DELETE) {
            memtable.erase(key);    // Remove from RAM
            tombstones.insert(key); // Add to the "Death Row" for the next flush
        }
    }
}
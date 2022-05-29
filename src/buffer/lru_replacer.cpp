//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
    lru_list_max_size_ = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

// 淘汰
bool LRUReplacer::Victim(frame_id_t *frame_id) {
    std::scoped_lock mtxLock{mtx_};
    if (lru_list_.size() == 0) {
        return false;
    }

    *frame_id = lru_list_.back();
    lru_hash_map_.erase(*frame_id);
    lru_list_.pop_back();

    return true;
}

// 删除
void LRUReplacer::Pin(frame_id_t frame_id) {
    std::scoped_lock mtxLock{mtx_};
    if (lru_hash_map_.count(frame_id) == 0) {
        return;
    }
    auto frame_id_itr = lru_hash_map_[frame_id];
    lru_list_.erase(frame_id_itr);
    lru_hash_map_.erase(frame_id);
}

// 增加
void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::scoped_lock mtxLock{mtx_};
    if (lru_hash_map_.count(frame_id) != 0) {
        return;
    }
    if (lru_list_.size() == lru_list_max_size_) {
        return;
    }
    lru_list_.push_front(frame_id);
    lru_hash_map_.emplace(frame_id, lru_list_.begin());
}

size_t LRUReplacer::Size() { return lru_list_.size(); }

}  // namespace bustub

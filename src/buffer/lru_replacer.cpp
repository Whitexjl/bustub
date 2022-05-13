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
    lruListMaxSize = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

// 淘汰
bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    std::scoped_lock mtxLock{mtx};
    if(lruList.size() == 0)
        return false; 

    *frame_id = lruList.back();
    lruHashMap.erase(*frame_id);
    lruList.pop_back();

    return true;
 }

// 删除
void LRUReplacer::Pin(frame_id_t frame_id) {
    std::scoped_lock mtxLock{mtx};
    if(lruHashMap.count(frame_id) == 0)
        return ; 

    auto frame_id_itr = lruHashMap[frame_id];
    lruList.erase(frame_id_itr);
    lruHashMap.erase(frame_id);
}

// 增加
void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::scoped_lock mtxLock{mtx};
    if (lruHashMap.count(frame_id) != 0) 
        return;

    if(lruList.size() == lruListMaxSize)
        return ; 

    lruList.push_front(frame_id);
    lruHashMap.emplace(frame_id, lruList.begin());
}

size_t LRUReplacer::Size() { return lruList.size(); }

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_; // page类的数组释放
  delete replacer_; // replacer类释放
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::scoped_lock Lock(latch_);

  if(page_table_.count(page_id) == 0 || page_id == INVALID_PAGE_ID) {
    return false;
  } else {
    frame_id_t frame_id_tmp = page_table_[page_id];
    Page *page_tmp = &pages_[frame_id_tmp];

    if(page_tmp->IsDirty()) {
      disk_manager_->WritePage(page_id, page_tmp->data_);
      page_tmp->is_dirty_ = false;
    }

    return true;
  }

}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::scoped_lock Lock(latch_);
  
  for(auto &p : page_table_) {
    FlushPgImp(p.first);
  }

}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock Lock(latch_);
  
  frame_id_t frame_id_tmp = -1;
  // 缓冲区未满
  if(!free_list_.empty()) {
    frame_id_tmp = free_list_.front();
    free_list_.pop_front();
  } else { // 缓冲区满了，lru淘汰
    if(replacer_->Victim(&frame_id_tmp)) { //能找到

    } else { // 不能找到
      return nullptr;
    }
  }

  // 新申请的页
  page_id_t new_page_id = AllocatePage();
  
  Page *page_tmp = &pages_[frame_id_tmp];
  
  // 脏页回写磁盘
  if(page_tmp->IsDirty()) {
    disk_manager_->WritePage(page_tmp->page_id_, page_tmp->data_);
    page_tmp->is_dirty_ = false;
  }

  // 更新页表
  page_table_.erase(page_tmp->page_id_);
  if(new_page_id != INVALID_PAGE_ID) {
    page_table_.emplace(new_page_id, frame_id_tmp);
  }

  // 页数据更新
  page_tmp->ResetMemory();
  page_tmp->pin_count_ = 1;
  page_tmp->page_id_ = new_page_id;
  disk_manager_->ReadPage(new_page_id, page_tmp->data_);
  replacer_->Pin(frame_id_tmp);
  *page_id = new_page_id;

  return page_tmp;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::scoped_lock Lock{latch_};

  // page在页表中存在，说明page在缓冲区
  if(page_table_.count(page_id) != 0) {
    frame_id_t frame_id_tmp = page_table_[page_id];
    
    Page *page_tmp = &pages_[frame_id_tmp];
    page_tmp->pin_count_++;

    replacer_->Pin(frame_id_tmp);
    return page_tmp;

  } else {
    frame_id_t frame_id_tmp = -1;
    // 先看free_list中是否非空
    if(!free_list_.empty()) { // 非空说明缓冲区未满,直接free_list中取出
      frame_id_tmp = free_list_.front();
      free_list_.pop_front();
    } else { // 如果为空，则说明缓冲区已满，需要调用lru换出
      // 得到被淘汰的那个frame_id
      if(replacer_->Victim(&frame_id_tmp)) { 
        
      } else { // 没有找到替换页？？？缓冲区满，lru空？这是啥情况
        return nullptr;
      }
    }

    if(frame_id_tmp == -1) {
      return nullptr;
    }

    assert(frame_id_tmp >= 0 && frame_id_tmp < static_cast<int>(pool_size_));
    Page *page_tmp = &pages_[frame_id_tmp];
    // 是脏页，需要先写回磁盘
    if(page_tmp->IsDirty()) { 
      disk_manager_->WritePage(page_tmp->page_id_, page_tmp->data_);
      page_tmp->is_dirty_ = false;
    }  

    // 更新页表
    page_table_.erase(page_tmp->page_id_);
    if(page_id != INVALID_PAGE_ID)
      page_table_.emplace(page_id, frame_id_tmp);

    // 更新缓冲区
    page_tmp->ResetMemory(); // 擦除换出页
    page_tmp->page_id_ = page_id; // 页号更新
    page_tmp->pin_count_ = 1; // 
    disk_manager_->ReadPage(page_tmp->page_id_, page_tmp->data_); // 把磁盘page_id的内容读取到被换出的缓冲区frame

    replacer_->Pin(frame_id_tmp);
    
    return page_tmp;
  }
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock Lock{latch_};
  // 页表中是否存在，也即是否在缓冲区里面
  if(page_table_.count(page_id) == 0) {
    return true;
  } else { // 说明不再缓冲区里面，要去看lru里面
    frame_id_t frame_id_tmp = page_table_[page_id];
    Page *page_tmp = &pages_[frame_id_tmp];
    // 当前已经没有线程在占用此缓冲区页
    if(page_tmp->pin_count_ <= 0) {
      if(page_tmp->IsDirty()) {
        disk_manager_->WritePage(page_tmp->page_id_, page_tmp->data_);
        page_tmp->is_dirty_ = false;
      } 

      DeallocatePage(page_id);
      page_tmp->pin_count_ = 0;
      page_tmp->page_id_ = INVALID_PAGE_ID;
      replacer_->Pin(page_id);
      page_table_.erase(page_id);
      free_list_.push_back(frame_id_tmp);

      return true;
    } else {
      return false;
    }
  }

}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) { 
  std::scoped_lock Lock{latch_};
  
  // 缓冲区不存在此页
  if(page_table_.count(page_id) == 0) {
    return false;
  } else { // 缓冲区存在
    frame_id_t frame_id_tmp = page_table_[page_id];
    Page *page_tmp = &pages_[frame_id_tmp];
    if(is_dirty) {
      page_tmp->is_dirty_ = true;
    }

    if(page_tmp->pin_count_ == 0) {
      return false;
    }

    page_tmp->pin_count_--;
    if(page_tmp->pin_count_ == 0) {
      replacer_->Unpin(page_id);
    }

    return true;
  }

}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub

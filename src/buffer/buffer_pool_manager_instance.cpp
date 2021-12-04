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

// #pragma once

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
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  latch_.lock();
  if (page_table_.count(page_id) == 0U) {
    latch_.unlock();
    return false;
  }
  frame_id_t frame = page_table_[page_id];
  // though not dirty,also flash into disk
  // if (!pages_[frame].IsDirty()) {
  //   latch_.unlock();
  //   return true;
  // }
  disk_manager_->WritePage(page_id, pages_[frame].data_);
  pages_[frame].is_dirty_ = false;
  latch_.unlock();
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  latch_.lock();
  for (auto &x : page_table_) {
    auto page_id = x.first;
    frame_id_t frame = x.second;
    // 不是脏文件是否可以不需要读入磁盘
    if (!pages_[frame].IsDirty()) {
      continue;
    }
    disk_manager_->WritePage(page_id, pages_[frame].data_);
    pages_[frame].is_dirty_ = false;
  }
  latch_.unlock();
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  latch_.lock();
  Page *res = nullptr;
  frame_id_t frame;
  bool get = false;
  if (!free_list_.empty()) {
    frame = free_list_.back();
    free_list_.pop_back();
    get = true;
  } else if (replacer_->Victim(&frame)) {
    get = true;
    if (pages_[frame].IsDirty()) {
      disk_manager_->WritePage(pages_[frame].page_id_, pages_[frame].data_);
    }
    page_table_.erase(pages_[frame].page_id_);
    pages_[frame].ResetMemory();
    pages_[frame].is_dirty_ = false;
  }
  if (get) {
    *page_id = AllocatePage();
    pages_[frame].page_id_ = *page_id;
    pages_[frame].pin_count_ = 1;
    page_table_[*page_id] = frame;
    res = &pages_[frame];
  }
  latch_.unlock();
  return res;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  Page *res = nullptr;
  latch_.lock();
  bool fetched = false;
  frame_id_t frame;
  if (page_table_.count(page_id) != 0U) {
    frame = page_table_[page_id];
    replacer_->Pin(frame);
    pages_[frame].pin_count_++;
    fetched = true;
  } else {
    if (!free_list_.empty()) {
      frame = free_list_.back();
      free_list_.pop_back();
      page_table_[page_id] = frame;
      fetched = true;
    } else if (replacer_->Victim(&frame)) {
      if (pages_[frame].IsDirty()) {
        disk_manager_->WritePage(pages_[frame].page_id_, pages_[frame].data_);
      }
      page_table_.erase(pages_[frame].page_id_);
      fetched = true;
    }
    if (fetched) {
      disk_manager_->ReadPage(page_id, pages_[frame].data_);
      pages_[frame].page_id_ = page_id;
      pages_[frame].is_dirty_ = false;
      pages_[frame].pin_count_ = 1;
      page_table_[page_id] = frame;
    }
  }
  if (fetched) {
    res = &pages_[frame];
  }
  latch_.unlock();
  return res;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  latch_.lock();
  bool deleted = false;
  DeallocatePage(page_id);
  if (page_table_.count(page_id) != 0U) {
    frame_id_t frame = page_table_[page_id];
    if (pages_[frame].GetPinCount() == 0) {
      // dirty不需要考虑?
      // if (pages_[frame].is_dirty_) {
      //   disk_manager_->WritePage(page_id, pages_[frame].data_);
      // }
      page_table_.erase(page_id);
      pages_[frame].page_id_ = INVALID_PAGE_ID;
      pages_[frame].ResetMemory();
      pages_[frame].is_dirty_ = false;
      // replacer和free_list之间只能选一个呆着,pincount为0且在pagetable中,说明replacer中已经有frame
      replacer_->Pin(frame);
      free_list_.push_front(frame);
      deleted = true;
    }
  } else {
    deleted = true;
  }
  latch_.unlock();
  return deleted;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  bool usedpin = false;
  if (page_table_.count(page_id) != 0U) {
    frame_id_t frame = page_table_[page_id];
    // 这要if嘛
    if (is_dirty) {
      pages_[frame].is_dirty_ = is_dirty;
    }
    if (pages_[frame].GetPinCount() > 0) {
      usedpin = true;
      pages_[frame].pin_count_--;
      if (pages_[frame].pin_count_ == 0) {
        replacer_->Unpin(frame);
      }
    }
  }
  latch_.unlock();
  return usedpin;
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

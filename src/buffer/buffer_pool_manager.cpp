//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  Page *res = nullptr;
  latch_.lock();
  frame_id_t frame = static_cast<frame_id_t>(pool_size_);
  if (page_table_.count(page_id) != 0U) {
    frame = page_table_[page_id];
    replacer_->Pin(frame);
    pages_[frame].pin_count_++;
  } else {
    if (!free_list_.empty()) {
      frame = free_list_.back();
      free_list_.pop_back();
      page_table_[page_id] = frame;
    } else if (replacer_->Victim(&frame)) {
      if (pages_[frame].IsDirty()) {
        disk_manager_->WritePage(pages_[frame].page_id_, pages_[frame].data_);
      }
      page_table_.erase(pages_[frame].page_id_);
    }
    if (frame != static_cast<frame_id_t>(pool_size_)) {
      disk_manager_->ReadPage(page_id, pages_[frame].data_);
      pages_[frame].page_id_ = page_id;
      pages_[frame].is_dirty_ = false;
      pages_[frame].pin_count_ = 1;
      page_table_[page_id] = frame;
    }
  }
  if (frame != static_cast<frame_id_t>(pool_size_)) {
    res = &pages_[frame];
  }
  latch_.unlock();
  return res;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
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

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  latch_.lock();
  if (page_table_.count(page_id) == 0U) {
    latch_.unlock();
    return false;
  }
  frame_id_t frame = page_table_[page_id];
  //不是脏文件是否可以不需要读入磁盘
  if (!pages_[frame].IsDirty()) {
    latch_.unlock();
    return true;
  }
  disk_manager_->WritePage(page_id, pages_[frame].data_);
  pages_[frame].is_dirty_ = false;
  latch_.unlock();
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  latch_.lock();
  Page *res = nullptr;
  *page_id = disk_manager_->AllocatePage();
  frame_id_t frame = static_cast<frame_id_t>(pool_size_);
  if (!free_list_.empty()) {
    frame = free_list_.back();
    free_list_.pop_back();
  } else if (replacer_->Victim(&frame)) {
    if (pages_[frame].IsDirty()) {
      disk_manager_->WritePage(pages_[frame].page_id_, pages_[frame].data_);
    }
    page_table_.erase(pages_[frame].page_id_);
  }
  if (frame != static_cast<frame_id_t>(pool_size_)) {
    pages_[frame].page_id_ = *page_id;
    pages_[frame].ResetMemory();
    pages_[frame].is_dirty_ = false;
    pages_[frame].pin_count_ = 1;
    page_table_[*page_id] = frame;
    res = &pages_[frame];
  }
  latch_.unlock();
  return res;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  latch_.lock();
  bool deleted = false;
  disk_manager_->DeallocatePage(page_id);
  if (page_table_.count(page_id) != 0U) {
    frame_id_t frame = page_table_[page_id];
    if (pages_[frame].pin_count_ == 0) {
      // dirty不需要考虑?
      if (pages_[frame].is_dirty_) {
        disk_manager_->WritePage(page_id, pages_[frame].data_);
      }
      pages_[frame].page_id_ = INVALID_PAGE_ID;
      pages_[frame].ResetMemory();
      pages_[frame].is_dirty_ = false;
      free_list_.push_front(frame);
      deleted = true;
    }
  } else {
    deleted = true;
  }
  latch_.unlock();
  return deleted;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (auto &x : page_table_) {
    FlushPageImpl(x.first);
  }
}

}  // namespace bustub
